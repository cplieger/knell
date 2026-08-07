package watch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cplieger/slogx/capture"
)

// fakeClock is a mutable test clock, safe for concurrent reads.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// call records one notifier invocation. It stays comparable so a test can
// assert a whole expected sequence with ==; a history call records how many
// ended outages it collapsed, and its payload is kept in histories.
type call struct {
	kind    string
	id      string
	elapsed time.Duration
	outages int
}

// fakeNotifier records calls and fails on demand. onMissing, when set, runs
// inside BeatMissing to interleave work with an in-flight send (set it from
// the same goroutine that calls sweep; no concurrent mutation).
type fakeNotifier struct {
	mu        sync.Mutex
	calls     []call
	histories [][]Outage
	fail      error
	onMissing func()
	onHistory func()
}

func (n *fakeNotifier) BeatMissing(_ context.Context, id string, live Transition) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fail != nil {
		return n.fail
	}
	if n.onMissing != nil {
		n.onMissing()
	}
	n.calls = append(n.calls, call{kind: "missing", id: id, elapsed: live.DownFor()})
	return nil
}

func (n *fakeNotifier) BeatRecovered(_ context.Context, id string, live Transition) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fail != nil {
		return n.fail
	}
	n.calls = append(n.calls, call{kind: "recovered", id: id, elapsed: live.DownFor()})
	return nil
}

func (n *fakeNotifier) BeatOutageHistory(_ context.Context, id string, outages []Outage) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fail != nil {
		return n.fail
	}
	if n.onHistory != nil {
		n.onHistory()
	}
	n.histories = append(n.histories, slices.Clone(outages))
	n.calls = append(n.calls, call{kind: "history", id: id, outages: len(outages)})
	return nil
}

func (n *fakeNotifier) setFail(err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.fail = err
}

func (n *fakeNotifier) snapshot() []call {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.calls)
}

// onlyHistory returns the single history payload the notifier received,
// failing when it saw any other number of history notices.
func onlyHistory(t *testing.T, n *fakeNotifier) []Outage {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.histories) != 1 {
		t.Fatalf("history notices = %d, want exactly 1: %v", len(n.histories), n.histories)
	}
	return slices.Clone(n.histories[0])
}

// checkSpans asserts the reported outages' full spans in order, plus the
// per-record shape a past-tense notice depends on: every reported outage is
// closed (a recovery point after its start) and they are chronological.
func checkSpans(t *testing.T, outages []Outage, want ...time.Duration) {
	t.Helper()
	if len(outages) != len(want) {
		t.Fatalf("reported outages = %d (%+v), want %d", len(outages), outages, len(want))
	}
	for i, wantSpan := range want {
		if got := outages[i].DownFor(); got != wantSpan {
			t.Errorf("outage %d DownFor = %s, want %s", i, got, wantSpan)
		}
		if !outages[i].Recovered.After(outages[i].Started) {
			t.Errorf("outage %d recovered at %s, not after its start %s (a history notice must only report ended outages)",
				i, outages[i].Recovered, outages[i].Started)
		}
		if i > 0 && outages[i].Started.Before(outages[i-1].Recovered) {
			t.Errorf("outage %d started at %s, before outage %d recovered at %s (history must be chronological)",
				i, outages[i].Started, i-1, outages[i-1].Recovered)
		}
	}
}

// reasonName names a late reason for a failure message; the numeric value of
// an enum tells a reader nothing about which clause the operator would read.
func reasonName(r LateReason) string {
	switch r {
	case LateUndelivered:
		return "LateUndelivered"
	case LateEndedBeforeDetection:
		return "LateEndedBeforeDetection"
	}
	return fmt.Sprintf("LateReason(%d)", uint8(r))
}

// checkLateReasons asserts the reported outages' late reasons in order. The
// reason is what the notice tells an operator to DO about a notice that
// arrived after the fact -- inspect the webhook, or nothing at all -- and a
// swapped reason passes every span, order and count assertion in this file
// while sending that operator to a webhook that was working (or vouching for
// one that was not).
func checkLateReasons(t *testing.T, outages []Outage, want ...LateReason) {
	t.Helper()
	if len(outages) != len(want) {
		t.Fatalf("reported outages = %d (%+v), want %d", len(outages), outages, len(want))
	}
	for i, wantReason := range want {
		if got := outages[i].LateReason; got != wantReason {
			t.Errorf("outage %d late reason = %s, want %s", i, reasonName(got), reasonName(wantReason))
		}
	}
}

// historyPayloads returns every history payload the notifier received, in
// delivery order.
func historyPayloads(n *fakeNotifier) [][]Outage {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([][]Outage, 0, len(n.histories))
	for _, h := range n.histories {
		out = append(out, slices.Clone(h))
	}
	return out
}

func newTestWatcher(beats ...Beat) (*Watcher, *fakeClock, *fakeNotifier) {
	clock := newFakeClock()
	notifier := &fakeNotifier{}
	return New(beats, notifier, clock.Now, clock.Now()), clock, notifier
}

// recordedBeat pings id and reports only whether it was RECORDED, for the
// tests that care about the configured-id verdict rather than about shutdown
// admission (which TestBeatAfterAdmissionClosedRecordsNothing covers).
func recordedBeat(w *Watcher, id string) bool {
	return w.Beat(id) == BeatRecorded
}

// drainRecoveries synchronously delivers queued recovered transitions, in
// place of the Run loop.
func drainRecoveries(w *Watcher) {
	for {
		select {
		case ev := <-w.recoveries:
			w.sendRecovered(context.Background(), ev)
		default:
			return
		}
	}
}

func TestBeatUnknownID(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	if recordedBeat(w, "ghost") {
		t.Error("Beat(ghost) = true, want false")
	}
	if got := n.snapshot(); len(got) != 0 {
		t.Fatalf("unknown id caused notifications: %v", got)
	}

	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0].kind != "missing" || got[0].id != "api" {
		t.Errorf("calls after unknown beat = %v, want one missing notification for api", got)
	}
}

// TestBeatAfterAdmissionClosedRecordsNothing pins the atomic shutdown
// boundary. Once admission is closed (StopAccepting, which the composition root
// calls from the pre-drain hook and Run calls before tallying undelivered
// work), a ping already in flight must change NOTHING — not lastSeen, not the
// alerted state, not the received counter, not the freshness gauge, and not the
// recovery queue — and must report accepting=false so webapi answers 503 rather
// than 200. This is the endpoint's ONLY shutdown refusal: webapi keeps no
// lifecycle check of its own, precisely because one made before this call could
// pass and then be descheduled, and the ping would record behind a tally Run had
// already reported.
func TestBeatAfterAdmissionClosedRecordsNothing(t *testing.T) {
	t.Parallel()

	// Unique beat id: the metric registry is package-global, so a label value
	// no other test uses keeps this test's series isolated under t.Parallel.
	const id = "admission-closed-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})

	// Drive the beat into a DELIVERED missing state first: a ping there is the
	// damaging one, because it would also queue a recovered notification onto
	// a channel whose only reader (Run) has already returned.
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if got := n.snapshot(); len(got) != 1 || got[0].kind != "missing" {
		t.Fatalf("setup sweep = %v, want exactly one missing notification", got)
	}

	lastSeenBefore := w.beats[id].lastSeen
	receivedBefore := beatCounterValue(t, "knell_beats_received_total", id)

	w.StopAccepting()
	clock.Advance(time.Minute)

	// The zero BeatOutcome must be the refusal, not the success: a Beater
	// implementation that returns an unset outcome then fails closed, the way
	// the pre-migration zero pair (false, false) did.
	var unset BeatOutcome
	if unset != BeatClosed {
		t.Errorf("zero BeatOutcome = %v, want BeatClosed: an omitted outcome must not read as a recorded ping", unset)
	}

	if got := w.Beat(id); got != BeatClosed {
		t.Fatalf("Beat after StopAccepting = %v, want BeatClosed", got)
	}
	if got := w.beats[id].lastSeen; !got.Equal(lastSeenBefore) {
		t.Errorf("lastSeen = %v, want %v: a refused ping must not re-arm the switch", got, lastSeenBefore)
	}
	if !w.beats[id].alerted {
		t.Error("alerted cleared by a refused ping: the reported outage must stay reported")
	}
	if got := beatCounterValue(t, "knell_beats_received_total", id); got != receivedBefore {
		t.Errorf("beats_received_total = %v, want %v: a refused ping must not be counted", got, receivedBefore)
	}
	if got := labeledValue(t, "knell_beat_fresh", "beat", id); got != "0" {
		t.Errorf("beat_fresh = %s, want 0: a refused ping must not republish freshness", got)
	}
	if got := len(w.recoveries); got != 0 {
		t.Errorf("queued recoveries = %d, want 0: a refused ping must not queue a notice no reader will take", got)
	}
}

// TestAdmissionClosedBeforeRunExitsStillTalliesUndeliveredWork pins the
// double-close production now performs: the composition root closes admission in
// webhttp's pre-drain hook, and Run closes it AGAIN when its ctx.Done arm runs.
// Closing is an assignment, not a transition, so the second call must be an
// inert no-op — and above all it must not cost the shutdown tally, which is the
// operator's only trace of the notices this process will never send. A
// StopAccepting that ever grew a one-shot guard, an early return, or any work
// beyond the flag would break exactly here, silently: the log line would go
// missing or arrive with the wrong counts while every other test still passed.
func TestAdmissionClosedBeforeRunExitsStillTalliesUndeliveredWork(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default.
	const id = "double-close-probe"
	w, clock, _ := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})

	// A queue with something to lose: one ENDED record (a permanent loss) and
	// one ongoing outage behind it, so a dropped or truncated tally is visible
	// in the counts rather than only in the line's presence.
	clock.Advance(11 * time.Minute)
	if !recordedBeat(w, id) {
		t.Fatalf("Beat(%s) = false during setup", id)
	}
	clock.Advance(11 * time.Minute)
	w.collectDue()

	// The pre-drain close, ahead of Run's own.
	w.StopAccepting()
	if got := w.Beat(id); got != BeatClosed {
		t.Fatalf("Beat after the pre-drain close = %v, want BeatClosed", got)
	}

	rec := capture.Default(t)

	// An already-cancelled context: Run takes its ctx.Done arm immediately,
	// which closes admission a SECOND time and then tallies.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w.Run(ctx, time.Minute)

	if !rec.HasAttr("watch loop stopped", "undelivered_records", "2") {
		t.Errorf("shutdown summary does not count both queued records after a double close: %v", rec.Records())
	}
	if !rec.HasAttr("watch loop stopped", "permanent_loss", "1") {
		t.Errorf("shutdown summary does not classify the ended record as permanently lost after a double close: %v", rec.Records())
	}
	if got := w.Beat(id); got != BeatClosed {
		t.Errorf("Beat after the second close = %v, want BeatClosed: closing twice must leave admission closed", got)
	}
}

func TestFreshBeatNeverNotifies(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	for range 10 {
		clock.Advance(5 * time.Minute)
		if !recordedBeat(w, "api") {
			t.Fatal("Beat(api) = false")
		}
		w.sweep(context.Background())
	}
	if got := n.snapshot(); len(got) != 0 {
		t.Errorf("fresh beat produced notifications: %v", got)
	}
}

func TestMissingFiresOncePerOutage(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	w.Beat("api")

	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	w.sweep(context.Background())
	clock.Advance(time.Hour)
	w.sweep(context.Background())

	got := n.snapshot()
	if len(got) != 1 || got[0].kind != "missing" || got[0].id != "api" {
		t.Fatalf("calls = %v, want exactly one missing for api", got)
	}
	if got[0].elapsed < 11*time.Minute {
		t.Errorf("silence = %s, want >= 11m", got[0].elapsed)
	}
}

func TestFirstDeadlineCountsFromProcessStartNotConstruction(t *testing.T) {
	t.Parallel()

	// main captures the process-start instant at entry and hands it to New as
	// start, so a beat that never pings alerts one deadline after PROCESS START,
	// not one deadline after the watcher was wired. Startup work (config parse,
	// secret file reads, listener bind) sits between the two, and every other
	// test in this package constructs with start == now, so a regression that
	// baselined off now() instead of start would extend the very first deadline
	// by the startup duration -- silently delaying the boot-armed alert that is
	// knell's whole reason for existing -- without failing anything.
	const id = "process-start-baseline-probe"
	clock := newFakeClock()
	n := &fakeNotifier{}
	start := clock.Now().Add(-9 * time.Minute)
	w := New([]Beat{{ID: id, Deadline: 10 * time.Minute}}, n, clock.Now, start)

	clock.Advance(2 * time.Minute)
	w.sweep(context.Background())

	got := n.snapshot()
	if len(got) != 1 || got[0].kind != "missing" || got[0].id != id {
		t.Fatalf("calls = %v, want one missing notice: 11m of process life passed with no ping", got)
	}
	if got[0].elapsed != 11*time.Minute {
		t.Errorf("silence = %s, want 11m measured from the process-start baseline (construction time would report 2m)", got[0].elapsed)
	}
}

func TestBootGraceFiresWithoutAnyBeat(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})

	clock.Advance(9 * time.Minute)
	w.sweep(context.Background())
	if got := n.snapshot(); len(got) != 0 {
		t.Fatalf("notified before boot deadline: %v", got)
	}

	clock.Advance(2 * time.Minute)
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0].kind != "missing" {
		t.Fatalf("calls = %v, want one missing after boot deadline", got)
	}
}

func TestRecoveryAfterMissing(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	w.Beat("api")

	clock.Advance(30 * time.Minute)
	w.sweep(context.Background())

	w.Beat("api")
	drainRecoveries(w)

	got := n.snapshot()
	if len(got) != 2 {
		t.Fatalf("calls = %v, want missing then recovered", got)
	}
	if got[0].kind != "missing" || got[1].kind != "recovered" {
		t.Fatalf("calls = %v, want [missing recovered]", got)
	}
	if got[1].elapsed < 30*time.Minute {
		t.Errorf("downFor = %s, want >= 30m", got[1].elapsed)
	}

	// A second fresh beat must not enqueue another recovery.
	w.Beat("api")
	drainRecoveries(w)
	if got := n.snapshot(); len(got) != 2 {
		t.Errorf("extra beat added notifications: %v", got)
	}
}

func TestFailedMissingRetriesNextSweep(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	w.Beat("api")
	clock.Advance(11 * time.Minute)

	n.setFail(errors.New("discord down"))
	w.sweep(context.Background())
	w.sweep(context.Background())
	if got := n.snapshot(); len(got) != 0 {
		t.Fatalf("failed sends recorded calls: %v", got)
	}

	n.setFail(nil)
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0].kind != "missing" {
		t.Fatalf("calls = %v, want one missing after recovery of the notifier", got)
	}

	// Delivered once: further sweeps stay silent.
	w.sweep(context.Background())
	if got := n.snapshot(); len(got) != 1 {
		t.Errorf("post-delivery sweep re-sent: %v", got)
	}
}

func TestFailedMissingStillDeliversAfterBeatRecovers(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	w.Beat("api")
	clock.Advance(11 * time.Minute)
	n.setFail(errors.New("discord down"))
	w.sweep(context.Background())
	w.Beat("api")
	n.setFail(nil)
	w.sweep(context.Background())
	got := n.snapshot()
	// The outage the failed send held back is over by the time delivery
	// succeeds, so it is reported once, in the past tense, instead of a
	// present-tense missing notice for an incident that already ended.
	if len(got) != 1 || got[0].kind != "history" || got[0].outages != 1 {
		t.Fatalf("calls = %v, want the pending outage delivered as one history notice", got)
	}
	outages := onlyHistory(t, n)
	checkSpans(t, outages, 11*time.Minute)
	// A sweep raised this outage and the send failed, so its notice really is
	// late because delivery could not complete.
	checkLateReasons(t, outages, LateUndelivered)
}

func TestSecondOutageNotifiesAgain(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	w.Beat("api")

	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	w.Beat("api")
	drainRecoveries(w)

	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())

	got := n.snapshot()
	if len(got) != 3 {
		t.Fatalf("calls = %v, want missing/recovered/missing", got)
	}
	if got[2].kind != "missing" {
		t.Errorf("third call = %+v, want missing", got[2])
	}
}

func TestPingRacingDeliveredMissingEmitsRecoveryAndRearms(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	w.Beat("api")

	// The ping lands while the missing notification is in flight: Beat sees
	// alerted=false and queues no recovery, so the sweep must emit it.
	clock.Advance(11 * time.Minute)
	n.onMissing = func() { w.Beat("api") }
	w.sweep(context.Background())
	n.onMissing = nil

	got := n.snapshot()
	if len(got) != 2 || got[0].kind != "missing" || got[1].kind != "recovered" {
		t.Fatalf("calls = %v, want [missing recovered]", got)
	}
	if got[1].id != "api" {
		t.Errorf("recovered id = %s, want api", got[1].id)
	}
	if got[1].elapsed < 11*time.Minute {
		t.Errorf("downFor = %s, want >= 11m", got[1].elapsed)
	}

	// The recovery came from the sweep itself; nothing extra may be queued.
	drainRecoveries(w)
	if got := n.snapshot(); len(got) != 2 {
		t.Fatalf("recovery was double-queued: %v", got)
	}

	// The beat is re-armed: a second silence must produce a second missing
	// (before the fix, alerted stayed true and this outage was swallowed).
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	got = n.snapshot()
	if len(got) != 3 || got[2].kind != "missing" {
		t.Fatalf("calls = %v, want a second missing after re-silence", got)
	}
}

func TestBeatsAreIndependent(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(
		Beat{ID: "fast", Deadline: time.Minute},
		Beat{ID: "slow", Deadline: time.Hour},
	)
	w.Beat("fast")
	w.Beat("slow")

	clock.Advance(2 * time.Minute)
	w.sweep(context.Background())

	got := n.snapshot()
	if len(got) != 1 || got[0].id != "fast" {
		t.Fatalf("calls = %v, want only fast missing", got)
	}
}

func TestRunLoopDeliversSweepAndRecovery(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: time.Minute})
		w.Beat("api")
		clock.Advance(2 * time.Minute)

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.Run(ctx, 5*time.Millisecond)
		}()

		synctest.Wait()
		time.Sleep(5 * time.Millisecond)
		synctest.Wait()
		got := n.snapshot()
		if len(got) != 1 || got[0].kind != "missing" {
			t.Fatalf("calls after sweep tick = %v, want one missing", got)
		}

		w.Beat("api")
		synctest.Wait()
		got = n.snapshot()
		if len(got) != 2 || got[1].kind != "recovered" {
			t.Fatalf("calls after recovered beat = %v, want missing then recovered", got)
		}

		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("Run did not stop on ctx cancel")
		}
	})
}

func TestFailedRecoveredIsBestEffortOnce(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	w.Beat("api")

	// First outage: missing delivered, then the beat pings again while the
	// notifier is down, so the queued recovered send fails.
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	n.setFail(errors.New("discord down"))
	w.Beat("api")
	drainRecoveries(w)

	// Best-effort means the failed recovery is consumed, never retried:
	// after the notifier heals, nothing is re-queued or re-sent.
	n.setFail(nil)
	drainRecoveries(w)
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0].kind != "missing" {
		t.Fatalf("calls = %v, want only the original missing (failed recovery never retried)", got)
	}

	// The switch stays armed: the next silence still fires a missing notice.
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	got = n.snapshot()
	if len(got) != 2 || got[1].kind != "missing" {
		t.Fatalf("calls = %v, want a second missing after the next outage", got)
	}
}

func TestPendingRecoveryBlocksNextMissingUntilDelivered(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	w.Beat("api")

	// First outage: missing delivered, then a ping queues the recovery,
	// which stays undrained (the Run loop is busy elsewhere).
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	w.Beat("api")

	// The beat goes silent past its deadline again while the recovery is
	// still queued. The sweep must not start the next missing transition
	// ahead of the pending recovery, or Discord would observe
	// missing/missing/recovered out of chronological order.
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0].kind != "missing" {
		t.Fatalf("calls = %v, want only the first missing while a recovery is pending", got)
	}

	// Once the recovery is delivered, the next sweep sends the second
	// missing: chronologically ordered [missing recovered missing].
	drainRecoveries(w)
	w.sweep(context.Background())
	got = n.snapshot()
	want := []string{"missing", "recovered", "missing"}
	if len(got) != len(want) {
		t.Fatalf("calls = %v, want missing/recovered/missing", got)
	}
	for i, kind := range want {
		if got[i].kind != kind {
			t.Errorf("calls[%d].kind = %s, want %s", i, got[i].kind, kind)
		}
	}
}

// failFirstNotifier fails the FIRST attempted missing send whichever beat it
// is for, and counts every attempt. Which beat comes first is deliberately
// irrelevant: Watcher stores beats in a map and therefore promises no
// iteration order, so a notifier keyed on a named beat would let an
// early-return regression pass whenever map order put the failing beat last.
type failFirstNotifier struct {
	attempts int
}

func (n *failFirstNotifier) BeatMissing(context.Context, string, Transition) error {
	n.attempts++
	if n.attempts == 1 {
		return errors.New("discord down")
	}
	return nil
}

func (*failFirstNotifier) BeatRecovered(context.Context, string, Transition) error {
	return nil
}

func (*failFirstNotifier) BeatOutageHistory(context.Context, string, []Outage) error {
	return nil
}

func TestOneBeatsFailedSendDoesNotStarveTheOthers(t *testing.T) {
	t.Parallel()

	// A failed send is per-beat: it is retried on the next sweep for THAT
	// beat, and the sweep must still deliver every other beat's notice in the
	// same pass. Abandoning the sweep on a plain failure would let one
	// permanently failing beat starve the rest for as long as it keeps
	// failing -- the observers would go quiet about beats that are down.
	clock := newFakeClock()
	n := &failFirstNotifier{}
	w := New([]Beat{
		{ID: "starve-probe-a", Deadline: 10 * time.Minute},
		{ID: "starve-probe-b", Deadline: 10 * time.Minute},
		{ID: "starve-probe-c", Deadline: 10 * time.Minute},
	}, n, clock.Now, clock.Now())

	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())

	if n.attempts != 3 {
		t.Errorf("missing delivery attempts = %d, want 3 (one failed beat must not starve the others)", n.attempts)
	}
}

// failFirstHistoryNotifier fails the FIRST attempted history send whichever
// beat it is for, records that beat, and counts every attempt. Which beat comes
// first is deliberately irrelevant: Watcher stores beats in a map and therefore
// promises no iteration order, so a notifier keyed on a named beat would let a
// give-up-on-failure regression pass whenever map order put the failing beat
// last.
type failFirstHistoryNotifier struct {
	failed   string
	attempts int
}

func (n *failFirstHistoryNotifier) BeatOutageHistory(_ context.Context, id string, _ []Outage) error {
	n.attempts++
	if n.attempts == 1 {
		n.failed = id
		return errors.New("discord down")
	}
	return nil
}

func (*failFirstHistoryNotifier) BeatMissing(context.Context, string, Transition) error { return nil }

func (*failFirstHistoryNotifier) BeatRecovered(context.Context, string, Transition) error {
	return nil
}

func TestOneBeatsFailedHistorySendDoesNotStarveTheOthers(t *testing.T) {
	t.Parallel()

	// A failed history send is per-beat: its records stay queued and the next
	// sweep retries THAT beat, and the sweep must still deliver every other
	// beat's past-tense notice in the same pass. Abandoning the sweep on a
	// plain failure would let one permanently failing beat starve the rest for
	// as long as it keeps failing -- the observers would go quiet about outages
	// that are over and about the live ones queued behind them.
	// TestOneBeatsFailedSendDoesNotStarveTheOthers pins the live half of that
	// contract; every other history test uses a single beat, where giving up
	// after a failure and carrying on are indistinguishable.
	const deadline = 10 * time.Minute
	beats := budgetProbeBeats("history-starve-probe", 3, deadline)
	clock := newFakeClock()
	n := &failFirstHistoryNotifier{}
	w := New(beats, n, clock.Now, clock.Now())

	// Give every beat one outage that is already over when the sweep runs: a
	// ping, a full deadline of silence, then a late ping. Every beat's queue
	// head is a closed record, so this sweep owes each of them one notice.
	for _, b := range beats {
		if !recordedBeat(w, b.ID) {
			t.Fatalf("Beat(%s) = false", b.ID)
		}
	}
	clock.Advance(deadline + time.Minute)
	for _, b := range beats {
		if !recordedBeat(w, b.ID) {
			t.Fatalf("late Beat(%s) = false", b.ID)
		}
	}

	w.sweep(context.Background())

	if n.attempts != len(beats) {
		t.Fatalf("history delivery attempts = %d, want %d: one beat's failed history send must not starve the others",
			n.attempts, len(beats))
	}
	for _, b := range beats {
		want := 0
		if b.ID == n.failed {
			want = 1
		}
		if got := len(w.beats[b.ID].pendingMissing); got != want {
			t.Errorf("beat %s holds %d queued record(s) after the sweep, want %d (a delivered notice drops its run, a failed one keeps it)",
				b.ID, got, want)
		}
	}
}

// blockingNotifier blocks every BeatMissing until released, simulating a
// send stuck on a slow or unreachable webhook.
type blockingNotifier struct {
	entered chan struct{}
	release chan struct{}
}

func (n *blockingNotifier) BeatMissing(ctx context.Context, _ string, _ Transition) error {
	n.entered <- struct{}{}
	select {
	case <-n.release:
	case <-ctx.Done():
	}
	return ctx.Err()
}

func (n *blockingNotifier) BeatRecovered(context.Context, string, Transition) error {
	return nil
}

func (n *blockingNotifier) BeatOutageHistory(context.Context, string, []Outage) error {
	return nil
}

func TestFreshnessGaugeUpdatesWhileSenderBlocked(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// Unique beat ids: the metric registry is package-global.
		clock := newFakeClock()
		n := &blockingNotifier{entered: make(chan struct{}, 8), release: make(chan struct{})}
		w := New([]Beat{
			{ID: "blocked-sender-a", Deadline: 10 * time.Minute},
			{ID: "blocked-sender-b", Deadline: 30 * time.Minute},
		}, n, clock.Now, clock.Now())

		// Beat a goes overdue before the loop starts; its missing send
		// will block the sender loop indefinitely.
		clock.Advance(11 * time.Minute)

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.Run(ctx, 5*time.Millisecond)
		}()

		time.Sleep(5 * time.Millisecond)
		<-n.entered // sender loop is now stuck inside BeatMissing

		// Beat b passes its own deadline while the sender is blocked.
		// The sweep cannot run, so only the independent gauge ticker can
		// flip b's freshness -- the documented ground-truth path.
		clock.Advance(25 * time.Minute)
		time.Sleep(5 * time.Millisecond)
		synctest.Wait()

		if got := labeledValue(t, "knell_beat_fresh", "beat", "blocked-sender-b"); got != "0" {
			t.Fatalf("beat_fresh for b while sender blocked = %s, want 0 (gauge ticker must not depend on the sender loop)", got)
		}

		// Cancel before releasing: the blocked send then returns
		// context.Canceled, the sweep stops, and Run exits without the
		// sender ever starting beat b's (also overdue) transition.
		cancel()
		close(n.release)
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("Run did not stop on ctx cancel")
		}
	})
}

func TestLatePingBeforeSweepPreservesOutage(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	w.Beat("api")
	start := clock.Now()
	clock.Advance(11 * time.Minute)

	if !recordedBeat(w, "api") {
		t.Fatal("late Beat(api) = false")
	}
	if calls := n.snapshot(); len(calls) != 0 {
		t.Fatalf("late ping notified synchronously: %v", calls)
	}

	// The crossing survives the ping that moved lastSeen, and because the
	// outage was already over when its notice came due, it is reported once
	// as history: a single past-tense notice, no live missing notice and no
	// separate recovered notice for an incident that ended before anyone
	// heard about it.
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0] != (call{kind: "history", id: "api", outages: 1}) {
		t.Fatalf("calls = %v, want one history notice covering one ended outage", got)
	}
	outages := onlyHistory(t, n)
	checkSpans(t, outages, 11*time.Minute)
	if !outages[0].Started.Equal(start) {
		t.Errorf("outage start = %s, want the last ping before it (%s)", outages[0].Started, start)
	}
	if want := start.Add(11 * time.Minute); !outages[0].Recovered.Equal(want) {
		t.Errorf("recovery point = %s, want the late ping's instant %s", outages[0].Recovered, want)
	}
	// No sweep ever saw this outage, so its notice is late for the cadence,
	// not for the webhook: the notice must not blame delivery.
	checkLateReasons(t, outages, LateEndedBeforeDetection)
}

func TestLatePingDuringPendingRecoveryPreservesSecondOutage(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	w.Beat("api")

	// First outage: missing delivered, then a ping queues the recovery,
	// which stays undrained (the Run loop is busy on a slow send).
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	w.Beat("api")

	// A second full deadline passes and a late ping arrives while the
	// first recovery is still queued. Beat must retain the crossed
	// deadline even in the recovering state, and the sweep must hold it
	// behind the pending recovery so Discord observes the transitions in
	// chronological order.
	clock.Advance(11 * time.Minute)
	if !recordedBeat(w, "api") {
		t.Fatal("late Beat(api) = false")
	}
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0].kind != "missing" {
		t.Fatalf("calls = %v, want only the first missing while the recovery is queued", got)
	}

	// Draining the first recovery unblocks the held second outage. The late
	// ping already ended it, so it arrives as history rather than as a
	// second live missing/recovered pair.
	drainRecoveries(w)
	w.sweep(context.Background())
	got = n.snapshot()
	want := []call{
		{kind: "missing", id: "api", elapsed: 11 * time.Minute},
		{kind: "recovered", id: "api", elapsed: 11 * time.Minute},
		{kind: "history", id: "api", outages: 1},
	}
	if len(got) != len(want) {
		t.Fatalf("calls = %v, want missing/recovered/history %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("calls[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
	checkSpans(t, onlyHistory(t, n), 11*time.Minute)
}

func TestSecondOutageDuringUndeliveredMissingIsNotErased(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	w.Beat("api")

	// Outage A: detected at t+11m, but the webhook is down, so its missing
	// notice stays undelivered. A ping at the same instant ends A.
	clock.Advance(11 * time.Minute)
	n.setFail(errors.New("discord down"))
	w.sweep(context.Background())
	if !recordedBeat(w, "api") {
		t.Fatal("Beat(api) = false")
	}

	// Outage B begins AND ends entirely while A's missing notice is still
	// undelivered: another full deadline of silence, then a late ping.
	// Detection must not be gated on A's delivery, or B leaves no trace at
	// all -- no notification, no counter movement, the outage erased.
	clock.Advance(11 * time.Minute)
	if !recordedBeat(w, "api") {
		t.Fatal("late Beat(api) = false")
	}
	if calls := n.snapshot(); len(calls) != 0 {
		t.Fatalf("pings notified while the notifier was down: %v", calls)
	}

	// The webhook heals: both outages are over, so ONE sweep delivers one
	// history notice carrying both of their spans. Nothing is replayed as a
	// live failure, and no further sweep has anything left to send.
	n.setFail(nil)
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0] != (call{kind: "history", id: "api", outages: 2}) {
		t.Fatalf("calls after the drain sweep = %v, want one history notice covering both outages", got)
	}
	w.sweep(context.Background())
	if got := n.snapshot(); len(got) != 1 {
		t.Fatalf("calls = %v, want nothing left to send after the collapsed history notice", got)
	}
	outages := onlyHistory(t, n)
	checkSpans(t, outages, 11*time.Minute, 11*time.Minute)
	// One batch, two reasons: A's live notice was raised by a sweep and held
	// back by the dead webhook, while B was over before any sweep saw it. The
	// summary reports both rather than blaming the webhook for both.
	checkLateReasons(t, outages, LateUndelivered, LateEndedBeforeDetection)
}

func TestThreeOutagesQueueWhileNoticesAreUndelivered(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	w.Beat("api")
	n.setFail(errors.New("discord down"))

	// Outage A (11m): sweep-detected, then ended by a ping.
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if !recordedBeat(w, "api") {
		t.Fatal("Beat(api) = false ending outage A")
	}

	// Outage B (13m): detected by a sweep while A is still queued, so the
	// sweep itself must record a crossing behind an undelivered notice.
	clock.Advance(13 * time.Minute)
	w.sweep(context.Background())
	if !recordedBeat(w, "api") {
		t.Fatal("Beat(api) = false ending outage B")
	}

	// Outage C (17m): no sweep ever sees it -- a late ping records the
	// whole closed outage while A and B are both still queued.
	clock.Advance(17 * time.Minute)
	if !recordedBeat(w, "api") {
		t.Fatal("late Beat(api) = false ending outage C")
	}
	if calls := n.snapshot(); len(calls) != 0 {
		t.Fatalf("calls while the notifier was down = %v, want none delivered", calls)
	}

	// All three outages survive with their own measurements: a two-slot
	// patch drops one of them, and losing a record loses its span for good.
	// They are all over, so one sweep reports all three in one notice
	// instead of eight sweeps replaying stale live failures.
	n.setFail(nil)
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0] != (call{kind: "history", id: "api", outages: 3}) {
		t.Fatalf("calls = %v, want one history notice covering all three outages", got)
	}
	outages := onlyHistory(t, n)
	// checkSpans pins each outage's own span at its own index, which is the
	// per-record independence this scenario exists to prove: a shared or
	// overwritten measurement shows up here as the wrong span on some index.
	checkSpans(t, outages, 11*time.Minute, 13*time.Minute, 17*time.Minute)
	// A and B were detected by a sweep whose send failed; C began and ended
	// with no sweep in between. The batch mixes the two reasons, so the
	// summary must state both instead of picking one.
	checkLateReasons(t, outages, LateUndelivered, LateUndelivered, LateEndedBeforeDetection)
}

func TestLiveOutageIsNotDelayedBehindHistory(t *testing.T) {
	t.Parallel()

	const id = "live-behind-history-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	n.setFail(errors.New("discord down"))

	// Fill the queue one slot short with ended outages, then start an
	// outage that never ends: it queues as the open tail, behind the whole
	// backlog of history.
	const ended = missingQueueSize - 1
	for range ended {
		clock.Advance(11 * time.Minute)
		if !recordedBeat(w, id) {
			t.Fatalf("Beat(%s) = false", id)
		}
	}
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if got := len(w.beats[id].pendingMissing); got != missingQueueSize {
		t.Fatalf("queued records = %d, want %d ended outages plus the open one", got, missingQueueSize)
	}

	// The webhook heals. The live outage must not wait one sweep per stale
	// record: the first sweep collapses the whole backlog into one notice
	// and the very next one raises the live alarm.
	n.setFail(nil)
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0] != (call{kind: "history", id: id, outages: ended}) {
		t.Fatalf("calls after the first drain sweep = %v, want one history notice covering %d ended outages", got, ended)
	}
	w.sweep(context.Background())
	got = n.snapshot()
	if len(got) != 2 || got[1].kind != "missing" {
		t.Fatalf("calls = %v, want the live outage's missing notice on the sweep right after the history notice", got)
	}
	if got[1].elapsed != 11*time.Minute {
		t.Errorf("live silence = %s, want 11m (the ongoing outage, not a replayed record)", got[1].elapsed)
	}
}

func TestFailedHistoryDeliveryKeepsRecordsAndRetries(t *testing.T) {
	t.Parallel()

	const id = "history-retry-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	n.setFail(errors.New("discord down"))

	// Two ended outages queue up while the webhook is down.
	for range 2 {
		clock.Advance(11 * time.Minute)
		if !recordedBeat(w, id) {
			t.Fatalf("Beat(%s) = false", id)
		}
	}

	// A failed history send must not consume the records it tried to
	// report: losing them would erase both outages with no trace, so they
	// stay queued and the whole run is retried on the next sweep.
	for range 2 {
		w.sweep(context.Background())
		if calls := n.snapshot(); len(calls) != 0 {
			t.Fatalf("failed history send recorded calls: %v", calls)
		}
		if got := len(w.beats[id].pendingMissing); got != 2 {
			t.Fatalf("queued records = %d after a failed history send, want both retained", got)
		}
	}

	n.setFail(nil)
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0] != (call{kind: "history", id: id, outages: 2}) {
		t.Fatalf("calls = %v, want the retried history notice covering both outages", got)
	}
	checkSpans(t, onlyHistory(t, n), 11*time.Minute, 11*time.Minute)
	if got := len(w.beats[id].pendingMissing); got != 0 {
		t.Errorf("queued records = %d after a delivered history notice, want 0", got)
	}
}

// fillMissingQueue drives missingQueueSize outages that each begin AND end
// between two sweeps, leaving the beat's pending-missing queue at its bound
// with already-ended records none of which was delivered. It is the shared
// precondition of every overflow test: one late ping past the deadline closes
// one outage and queues one record, so that relationship and the resulting
// count live here instead of in a copy per test.
func fillMissingQueue(t *testing.T, w *Watcher, clock *fakeClock, id string) {
	t.Helper()
	for range missingQueueSize {
		clock.Advance(11 * time.Minute)
		if !recordedBeat(w, id) {
			t.Fatalf("Beat(%s) = false", id)
		}
	}
	if got := len(w.beats[id].pendingMissing); got != missingQueueSize {
		t.Fatalf("queued records = %d, want the full bound %d", got, missingQueueSize)
	}
}

// queueOutageNoSweepEverSaw drives one full deadline of silence ended by a late
// ping, the only producer of a LateEndedBeforeDetection record, and asserts the
// beat is left holding exactly that. It is the precondition of the two tests
// below: both are about what happens to that reason afterwards, so a change
// that stopped producing it would otherwise make them pass vacuously.
func queueOutageNoSweepEverSaw(t *testing.T, w *Watcher, clock *fakeClock, id string) {
	t.Helper()
	clock.Advance(11 * time.Minute)
	if !recordedBeat(w, id) {
		t.Fatalf("late Beat(%s) = false", id)
	}
	queued := w.beats[id].pendingMissing
	if len(queued) != 1 {
		t.Fatalf("queued records = %d, want the one closed record the late ping recorded", len(queued))
	}
	if got := queued[0].late; got != LateEndedBeforeDetection {
		t.Fatalf("queued late reason = %s, want %s: nothing here can vouch for delivery any more",
			reasonName(got), reasonName(LateEndedBeforeDetection))
	}
}

func TestFailedHistorySendMakesTheRetriedNoticeBlameDelivery(t *testing.T) {
	t.Parallel()

	// A record that no sweep ever saw starts out able to say "nothing was
	// wrong with delivery" (LateEndedBeforeDetection). Then its own history
	// notice fails to send and stays queued for the next sweep, which makes
	// that statement false in the opposite direction: the notice an operator
	// finally reads was refused once by the very webhook it vouches for.
	// notify renders the reason verbatim
	// (TestBeatOutageHistoryStatesTheTrueReasonForALateNotice), so the state
	// machine is the only place that can keep the claim honest.
	const id = "history-late-reason-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	queueOutageNoSweepEverSaw(t, w, clock, id)

	n.setFail(errors.New("discord down"))
	w.sweep(context.Background())
	if calls := n.snapshot(); len(calls) != 0 {
		t.Fatalf("failed history send recorded calls: %v", calls)
	}
	if got := len(w.beats[id].pendingMissing); got != 1 {
		t.Fatalf("queued records = %d after the failed history send, want the record retained for the retry", got)
	}

	n.setFail(nil)
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0] != (call{kind: "history", id: id, outages: 1}) {
		t.Fatalf("calls = %v, want the retried history notice covering the one ended outage", got)
	}
	outages := onlyHistory(t, n)
	// The outage itself is unchanged by the retry: only the REASON moves.
	checkSpans(t, outages, 11*time.Minute)
	if got := outages[0].LateReason; got != LateUndelivered {
		t.Errorf(`late reason on the retried notice = %s, want %s: %s renders as "nothing was wrong with delivery", which is false in a notice this webhook already refused once - the operator is told to look nowhere while delivery is what failed`,
			reasonName(got), reasonName(LateUndelivered), reasonName(got))
	}
}

func TestFailedHistorySendBlamesDeliveryForEveryRecordItCovered(t *testing.T) {
	t.Parallel()

	// The multi-record half of the blame rule. A sustained webhook outage is
	// exactly what queues several ended outages behind ONE history notice, and
	// when that notice fails, every record it covered is now late because
	// delivery failed - not just the head. A record left claiming
	// LateEndedBeforeDetection renders as "nothing was wrong with delivery" in
	// the very notice this webhook already refused, so the operator is told to
	// look nowhere for the rest of the batch.
	const (
		id       = "history-blame-run-probe"
		deadline = 10 * time.Minute
		outages  = 3
	)
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: deadline})
	if !recordedBeat(w, id) {
		t.Fatalf("Beat(%s) = false", id)
	}
	// Three outages that each began AND ended between two sweeps, so every
	// record starts out blaming nothing (LateEndedBeforeDetection).
	for range outages {
		clock.Advance(deadline + time.Minute)
		if !recordedBeat(w, id) {
			t.Fatalf("late Beat(%s) = false", id)
		}
	}
	queued := w.beats[id].pendingMissing
	if len(queued) != outages {
		t.Fatalf("queued records = %d, want %d closed records for the batch", len(queued), outages)
	}
	for i, rec := range queued {
		if rec.late != LateEndedBeforeDetection {
			t.Fatalf("queued record %d late reason = %s, want %s before any send was attempted",
				i, reasonName(rec.late), reasonName(LateEndedBeforeDetection))
		}
	}

	// One history notice covers all three records, and it fails.
	n.setFail(errors.New("discord down"))
	w.sweep(context.Background())
	if calls := n.snapshot(); len(calls) != 0 {
		t.Fatalf("failed history send recorded calls: %v", calls)
	}

	n.setFail(nil)
	w.sweep(context.Background())
	got := onlyHistory(t, n)
	checkSpans(t, got, deadline+time.Minute, deadline+time.Minute, deadline+time.Minute)
	checkLateReasons(t, got, LateUndelivered, LateUndelivered, LateUndelivered)
}

func TestCanceledHistorySendKeepsTheLateReasonItWasRecordedWith(t *testing.T) {
	t.Parallel()

	// The twin of the test above, and the reason the upgrade sits behind the
	// context.Canceled arm rather than in front of it: a shutdown abandons the
	// send, it does not fail it. Nothing was refused, so the queued record must
	// keep the reason it was recorded with; rewriting it would turn a shutdown
	// into a webhook incident that never happened.
	const id = "history-cancel-reason-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	queueOutageNoSweepEverSaw(t, w, clock, id)

	n.setFail(context.Canceled)
	w.sweep(context.Background())
	if calls := n.snapshot(); len(calls) != 0 {
		t.Fatalf("abandoned history send recorded calls: %v", calls)
	}

	n.setFail(nil)
	w.sweep(context.Background())
	outages := onlyHistory(t, n)
	checkSpans(t, outages, 11*time.Minute)
	if got := outages[0].LateReason; got != LateEndedBeforeDetection {
		t.Errorf("late reason after a shutdown-abandoned send = %s, want %s: a cancelled send is not a delivery failure and must not rewrite what the record says",
			reasonName(got), reasonName(LateEndedBeforeDetection))
	}
}

func TestPendingMissingQueueOverflowIsAccountedNotSilent(t *testing.T) {
	// Serial (no t.Parallel): it asserts deltas on the package-global
	// notification counters, which the parallel tests also move.
	const id = "missing-overflow-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)

	// Fill the queue to its bound: every late ping past the deadline
	// records one whole closed outage, and no sweep runs to drain them.
	fillMissingQueue(t, w, clock, id)

	// One more outage, of a distinctive length, overflows the queue. That
	// outage already ended, so its record was its last trace: no notice for
	// it will ever arrive. A permanent loss of a RECORD is what the per-record
	// dropped counter reports (failed means a delivery attempt that will retry,
	// which this is not; the per-MESSAGE dropped counter would claim one lost
	// message for a record a history notice would have collapsed with others),
	// and the outage must still count as detected.
	const droppedSilence = 47 * time.Minute
	failedBefore := counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore := counterValue(t, "knell_notifications_dropped_total", "missing")
	recordsDroppedBefore := beatCounterValue(t, "knell_outage_records_dropped_total", id)
	outagesBefore := beatCounterValue(t, "knell_beat_outages_total", id)
	clock.Advance(droppedSilence)
	if !recordedBeat(w, id) {
		t.Fatalf("overflow Beat(%s) = false", id)
	}
	if got, want := beatCounterValue(t, "knell_outage_records_dropped_total", id), recordsDroppedBefore+1; got != want {
		t.Errorf("outage_records_dropped_total = %v after the queue-full drop, want %v (a lost outage record must be accounted)", got, want)
	}
	if got := counterValue(t, "knell_notifications_dropped_total", "missing"); got != droppedBefore {
		t.Errorf("dropped{missing} = %v after the queue-full drop, want unchanged %v (that counter counts MESSAGES, and no missing message was ever built for this record)", got, droppedBefore)
	}
	if got := counterValue(t, "knell_notifications_failed_total", "missing"); got != failedBefore {
		t.Errorf("failed{missing} = %v after the queue-full drop, want unchanged %v (nothing was attempted and nothing will retry)", got, failedBefore)
	}
	if got, want := beatCounterValue(t, "knell_beat_outages_total", id), outagesBefore+1; got != want {
		t.Errorf("beat_outages_total = %v after the dropped outage, want %v (a detected outage counts even when its notice is dropped)", got, want)
	}
	if got := len(w.beats[id].pendingMissing); got != missingQueueSize {
		t.Errorf("queued outages = %d after overflow, want the bound %d to hold", got, missingQueueSize)
	}

	// The queued outages all ended, so ONE sweep reports them as a single
	// history notice, and the dropped one contributes nothing.
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0] != (call{kind: "history", id: id, outages: missingQueueSize}) {
		t.Fatalf("calls = %v, want one history notice covering the %d queued outages", got, missingQueueSize)
	}
	outages := onlyHistory(t, n)
	for i, o := range outages {
		if o.DownFor() == droppedSilence {
			t.Errorf("outage %d = %+v reports the dropped outage's interval", i, o)
		}
	}

	// The beat stays armed after an overflow: the next crossing alerts, and
	// as a live outage (present tense), not as more history. The dropped
	// outage stays gone: no later sweep resurrects its notice.
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	got = n.snapshot()
	if len(got) != 2 || got[1].kind != "missing" {
		t.Fatalf("calls = %v, want one live missing notice once the queue drained", got)
	}
	for _, c := range got {
		if c.elapsed == droppedSilence {
			t.Errorf("call %+v delivered the dropped outage; a dropped record must never reach Discord", c)
		}
	}
}

func TestRecoveryPointIsTheFirstPingAfterTheOutage(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "downfor-first-ping", Deadline: 10 * time.Minute})
	w.Beat("downfor-first-ping")
	start := clock.Now()

	// Outage detected at t+11m but the missing send fails, so the
	// transition stays pending while pings resume.
	clock.Advance(11 * time.Minute)
	n.setFail(errors.New("discord down"))
	w.sweep(context.Background())

	// First ping after the outage at t+12m ends it; a later ping at
	// t+17m must NOT move the recovery point (first ping wins).
	clock.Advance(time.Minute)
	if !recordedBeat(w, "downfor-first-ping") {
		t.Fatal("Beat = false")
	}
	clock.Advance(5 * time.Minute)
	if !recordedBeat(w, "downfor-first-ping") {
		t.Fatal("second Beat = false")
	}

	n.setFail(nil)
	w.sweep(context.Background())

	got := n.snapshot()
	if len(got) != 1 || got[0].kind != "history" || got[0].outages != 1 {
		t.Fatalf("calls = %v, want one history notice for the ended outage", got)
	}
	outage := onlyHistory(t, n)[0]
	if want := start.Add(12 * time.Minute); !outage.Recovered.Equal(want) {
		t.Errorf("recovery point = %s, want %s (the FIRST ping after the outage, not a later one)", outage.Recovered, want)
	}
	if got := outage.DownFor(); got != 12*time.Minute {
		t.Errorf("outage span = %s, want exactly 12m (last ping before to first ping after)", got)
	}
}

func TestRetriedMissingReportsCurrentSilence(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "silence-refresh", Deadline: 10 * time.Minute})
	w.Beat("silence-refresh")

	// Outage detected at t+11m; the missing send fails and stays pending.
	clock.Advance(11 * time.Minute)
	n.setFail(errors.New("discord down"))
	w.sweep(context.Background())

	// The beat stays silent for another 49m before the notifier heals.
	// The retried missing notice must report the CURRENT silence (1h),
	// not the stale 11m captured when the outage was first detected.
	clock.Advance(49 * time.Minute)
	n.setFail(nil)
	w.sweep(context.Background())

	got := n.snapshot()
	if len(got) != 1 || got[0].kind != "missing" {
		t.Fatalf("calls = %v, want one missing", got)
	}
	if got[0].elapsed != time.Hour {
		t.Errorf("missing silence = %s, want exactly 1h (silence refreshed on each retry sweep while the beat stays silent)", got[0].elapsed)
	}
}

func TestSweepDetectedCrossingSurvivesAQueueFullOverflow(t *testing.T) {
	// Serial (no t.Parallel): it asserts deltas on the package-global
	// notification counters, which the parallel tests also move.
	const id = "sweep-overflow-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	fillMissingQueue(t, w, clock, id)

	// The beat now goes silent with no ping to end it, so the sweep -- not
	// Beat -- is the path that detects the crossing while the queue is at
	// its bound. The overflow must not mark the beat alerted: a dead-man
	// switch that swallows an ongoing outage because its queue was briefly
	// full is the worst failure it can have.
	failedBefore := counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore := counterValue(t, "knell_notifications_dropped_total", "missing")
	recordsDroppedBefore := beatCounterValue(t, "knell_outage_records_dropped_total", id)
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())

	// That sweep collapsed the whole ended backlog into one history notice,
	// which frees every slot; the next sweep records the ongoing outage and
	// raises it as a live alarm. An overflow that marked the beat alerted
	// instead would swallow it: the history notice and nothing more.
	w.sweep(context.Background())
	got := n.snapshot()
	want := []call{
		{kind: "history", id: id, outages: missingQueueSize},
		{kind: "missing", id: id, elapsed: 11 * time.Minute},
	}
	if len(got) != len(want) {
		t.Fatalf("calls = %v, want the collapsed history notice plus the ongoing outage's missing notice %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("calls[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}

	// Nothing failed and nothing was lost on this path, and the notice above
	// proves it: the sweep-path queue-full event only deferred the outage by
	// one tick, so neither delivery counter may move for it. Counting it as
	// a failure would page KnellNotifyFailing for an outage that WAS
	// delivered, every 15s while the queue stayed full.
	if got := counterValue(t, "knell_notifications_failed_total", "missing"); got != failedBefore {
		t.Errorf("failed{missing} = %v after a sweep-path queue-full event, want unchanged %v (nothing failed: the outage was delivered a tick later)", got, failedBefore)
	}
	if got := counterValue(t, "knell_notifications_dropped_total", "missing"); got != droppedBefore {
		t.Errorf("dropped{missing} = %v after a sweep-path queue-full event, want unchanged %v (nothing was dropped: the record was queued once a slot freed)", got, droppedBefore)
	}
	if got := beatCounterValue(t, "knell_outage_records_dropped_total", id); got != recordsDroppedBefore {
		t.Errorf("outage_records_dropped_total = %v after a sweep-path queue-full event, want unchanged %v (no record was discarded for good: the outage stayed detected and was queued once a slot freed)", got, recordsDroppedBefore)
	}
}

func TestOutageDetectedWhileTheQueueWasFullBlamesDeliveryNotTheSweep(t *testing.T) {
	t.Parallel()

	// The one path where a CLOSED record recorded by a PING is nevertheless an
	// outage a sweep already saw: the sweep detected it while the queue was at
	// its bound, so nothing could be queued for it (overflowAccounted), and the
	// same sweep then drained the ended backlog and freed the slots. A ping now
	// records the whole closed outage. Reporting that as "ended before a sweep
	// detected it" would vouch for a delivery path that was eight records
	// behind, so lateReasonForUnqueuedOutage reads overflowAccounted and blames
	// delivery instead.
	const id = "overflow-reason-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)

	// Fill the queue with outages no sweep ever sees.
	fillMissingQueue(t, w, clock, id)

	// A new outage begins with the queue full: this sweep detects it and
	// cannot queue it, then delivers the whole ended run as one notice.
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if !w.beats[id].overflowAccounted {
		t.Fatal("the sweep did not mark the ongoing outage as detected-but-unqueued; this test no longer covers the overflow path")
	}
	if got := len(w.beats[id].pendingMissing); got != 0 {
		t.Fatalf("queued records = %d after the history notice drained the run, want 0", got)
	}

	// The ping that ends it records the whole closed outage, and the next
	// sweep reports it.
	if !recordedBeat(w, id) {
		t.Fatalf("Beat(%s) = false ending the overflowed outage", id)
	}
	w.sweep(context.Background())

	payloads := historyPayloads(n)
	if len(payloads) != 2 {
		t.Fatalf("history notices = %d, want 2 (the drained backlog, then the overflowed outage)", len(payloads))
	}
	backlog := make([]LateReason, missingQueueSize)
	for i := range backlog {
		backlog[i] = LateEndedBeforeDetection
	}
	checkLateReasons(t, payloads[0], backlog...)
	checkSpans(t, payloads[1], 11*time.Minute)
	checkLateReasons(t, payloads[1], LateUndelivered)
}

func TestQueuedOngoingOutageReportsLiveSilenceWhenPromoted(t *testing.T) {
	t.Parallel()

	const id = "queued-ongoing-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	n.setFail(errors.New("discord down"))

	// Outage A: detected, ended by a ping, notice still undelivered.
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if !recordedBeat(w, id) {
		t.Fatalf("Beat(%s) = false ending outage A", id)
	}

	// Outage B starts and never ends. It queues behind A at 13m of
	// silence, then stays quiet for another 30m while A is undelivered.
	clock.Advance(13 * time.Minute)
	w.sweep(context.Background())
	clock.Advance(30 * time.Minute)
	w.sweep(context.Background())

	// Once the webhook heals, A goes out as history with the interval it
	// actually spanned, and B -- still ongoing when it reaches the head --
	// reports how long the beat has been quiet NOW, not the 13m measured
	// when the crossing was first detected behind A.
	n.setFail(nil)
	w.sweep(context.Background())
	w.sweep(context.Background())
	got := n.snapshot()
	want := []call{
		{kind: "history", id: id, outages: 1},
		{kind: "missing", id: id, elapsed: 43 * time.Minute},
	}
	if len(got) != len(want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("calls[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
	checkSpans(t, onlyHistory(t, n), 11*time.Minute)
}

// budgetProbeBeats builds n beats named from prefix for the send-budget
// tests. Every id must be unique across the package: the metric registry is
// global, and these tests assert per-beat counter deltas.
func budgetProbeBeats(prefix string, n int, deadline time.Duration) []Beat {
	beats := make([]Beat, n)
	for i := range beats {
		beats[i] = Beat{ID: fmt.Sprintf("%s-%02d", prefix, i), Deadline: deadline}
	}
	return beats
}

// TestEveryBeatCanQueueItsRecoveryWithoutADrop pins the size New gives the
// recovered-transition queue. It is sized from the beat count because each beat
// can hold at most one pending recovery, which is what makes the drop path in
// Beat unreachable in production; the whole fleet pinging at once (the fan-out
// source coming back) is the ordinary case that would otherwise hit it and lose
// recovered notices for good, since nothing retries one.
// TestRecoveryQueueOverflowDropKeepsBeatArmed deliberately SHRINKS the channel
// to exercise that drop path, so it asserts the other side of this boundary,
// and every other recovery test uses a single beat, where a capacity of 1 is
// indistinguishable from len(beats).
func TestEveryBeatCanQueueItsRecoveryWithoutADrop(t *testing.T) {
	// Serial (no t.Parallel): asserts a delta on the package-global
	// notification counters, which the parallel tests also move.
	const (
		total    = 5
		deadline = 10 * time.Minute
	)
	beats := budgetProbeBeats("recovery-queue-size-probe", total, deadline)
	clock := newFakeClock()
	n := &fakeNotifier{}
	w := New(beats, n, clock.Now, clock.Now())

	// Alert every beat, so every ping below queues a recovered transition.
	clock.Advance(deadline + time.Minute)
	w.sweep(context.Background())
	if got := len(n.snapshot()); got != total {
		t.Fatalf("missing notices = %d, want one per beat (%d)", got, total)
	}

	// The whole fleet pings before the Run loop services anything: every one of
	// those recoveries must find a slot.
	droppedBefore := counterValue(t, "knell_notifications_dropped_total", "recovered")
	for _, b := range beats {
		if !recordedBeat(w, b.ID) {
			t.Fatalf("Beat(%s) = false", b.ID)
		}
	}
	if got := counterValue(t, "knell_notifications_dropped_total", "recovered"); got != droppedBefore {
		t.Errorf("dropped{recovered} = %v after the whole fleet pinged, want unchanged %v: the queue must hold one recovery per beat, and nothing retries a dropped recovered notice",
			got, droppedBefore)
	}

	drainRecoveries(w)
	recovered := make(map[string]int, total)
	for _, c := range n.snapshot() {
		if c.kind == "recovered" {
			recovered[c.id]++
		}
	}
	for _, b := range beats {
		if got := recovered[b.ID]; got != 1 {
			t.Errorf("beat %s got %d recovered notices, want exactly 1: its queued recovery was dropped for want of a slot", b.ID, got)
		}
	}
}

// sendsBeforeBudgetCut is how many sends one sweep starts when every send
// burns perSend of the budget: the first starts with the budget untouched,
// and each further one starts while the elapsed time is still within it (the
// cut happens on the check AFTER the budget is exceeded, never mid-send). It
// is derived rather than hardcoded so the expectations below follow
// sweepSendBudget if the constant is ever retuned.
func sendsBeforeBudgetCut(perSend time.Duration) int {
	return int(sweepSendBudget/perSend) + 1
}

// slowSends makes every missing send burn perSend of the sweep's budget, the
// stand-in for a webhook that answers slowly or rate-limits. Set it before
// starting any Run loop: the hook is read from the sender goroutine.
func slowSends(n *fakeNotifier, clock *fakeClock, perSend time.Duration) {
	n.onMissing = func() { clock.Advance(perSend) }
}

// slowHistorySends makes every history send burn perSend of the sweep's
// budget, the past-tense twin of slowSends. Set it before starting any Run
// loop: the hook is read from the sender goroutine.
func slowHistorySends(n *fakeNotifier, clock *fakeClock, perSend time.Duration) {
	n.onHistory = func() { clock.Advance(perSend) }
}

// TestHistorySendsAreBoundedByTheSweepBudget pins the send budget on the
// history half of a sweep. History is delivered FIRST, so a backlog of ended
// outages -- the shape a webhook outage leaves behind -- is what actually
// spends the budget in production; without a cut there, one sweep pushes every
// queued beat's past-tense notice through a slow webhook back to back while
// Run's select (and every queued recovery) waits out the whole storm, which is
// the exact failure sweepSendBudget exists to prevent. The deferred count must
// also cover the LIVE notices this cut defers, not only the remaining history
// ones: a cut in the history loop defers both orderings, and that term appears
// nowhere else.
func TestHistorySendsAreBoundedByTheSweepBudget(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, and this asserts deltas on the package-global counters.
	const (
		historyDue = 6
		liveDue    = 4
		perSend    = 2 * time.Second
		deadline   = 10 * time.Minute
	)
	histBeats := budgetProbeBeats("budget-history-probe-h", historyDue, deadline)
	liveBeats := budgetProbeBeats("budget-history-probe-l", liveDue, deadline)
	clock := newFakeClock()
	n := &fakeNotifier{}
	w := New(slices.Concat(histBeats, liveBeats), n, clock.Now, clock.Now())

	wantSent := sendsBeforeBudgetCut(perSend)
	if wantSent >= historyDue {
		t.Fatalf("test precondition: %d sends fit in the %s budget at %s each, want fewer than the %d due history notices",
			wantSent, sweepSendBudget, perSend, historyDue)
	}

	// Give every history beat one outage that is already over when the sweep
	// runs (ping, a full deadline of silence, late ping), while the live beats
	// stay on their boot-armed baseline and are simply overdue.
	for _, b := range histBeats {
		if !recordedBeat(w, b.ID) {
			t.Fatalf("Beat(%s) = false", b.ID)
		}
	}
	clock.Advance(deadline + time.Minute)
	for _, b := range histBeats {
		if !recordedBeat(w, b.ID) {
			t.Fatalf("late Beat(%s) = false", b.ID)
		}
	}
	slowSends(n, clock, perSend)
	slowHistorySends(n, clock, perSend)

	sentBefore := counterValue(t, "knell_notifications_sent_total", "history")
	failedBefore := counterValue(t, "knell_notifications_failed_total", "history")
	droppedBefore := counterValue(t, "knell_notifications_dropped_total", "missing")

	rec := capture.Default(t)
	w.sweep(context.Background())

	got := n.snapshot()
	if len(got) != wantSent {
		t.Fatalf("notices in the budget-limited sweep = %d (%v), want %d history notices: the sweep must stop starting sends once its %s budget is spent",
			len(got), got, wantSent, sweepSendBudget)
	}
	delivered := make(map[string]bool, len(got))
	for i, c := range got {
		if c.kind != "history" {
			t.Errorf("calls[%d] = %+v, want a history notice: the cut must return from the sweep, not fall through to the live notices", i, c)
		}
		delivered[c.id] = true
	}

	// Every beat the cut deferred keeps its outage: the record is still queued
	// for the next sweep and nothing about it was counted. The one thing the cut
	// changes is the record's late reason, which now blames delivery
	// (blameDeferredHistory).
	for _, b := range histBeats {
		st := w.beats[b.ID]
		if delivered[b.ID] {
			if len(st.pendingMissing) != 0 {
				t.Errorf("beat %s was notified but still holds %d record(s)", b.ID, len(st.pendingMissing))
			}
			continue
		}
		if len(st.pendingMissing) != 1 {
			t.Errorf("beat %s was deferred but holds %d record(s), want its ended-outage record retained for the next sweep", b.ID, len(st.pendingMissing))
			continue
		}
		// The cut itself is the reason this notice is late, so the record must
		// blame delivery: it was queued as LateEndedBeforeDetection (no sweep
		// ever saw the outage), and the budget only bites when sends are slow
		// enough to spend the whole window. A record left claiming that nothing
		// was wrong with delivery points the operator away from the webhook that
		// is actually behind.
		if got := st.pendingMissing[0].late; got != LateUndelivered {
			t.Errorf("beat %s deferred by the %s send budget reports %s, want %s: the retried past-tense notice would vouch for a webhook that is demonstrably behind",
				b.ID, sweepSendBudget, reasonName(got), reasonName(LateUndelivered))
		}
	}
	for _, b := range liveBeats {
		st := w.beats[b.ID]
		if st.alerted {
			t.Errorf("live beat %s is marked alerted, but the sweep was cut before the live notices: its outage would never be announced", b.ID)
		}
		if st.openMissing() == nil {
			t.Errorf("live beat %s has no queued open record, so the next sweep has nothing to send", b.ID)
		}
	}

	const msg = "sweep send budget spent"
	wantDeferred := historyDue - wantSent + liveDue
	if !rec.HasAttr(msg, "deferred_beats", strconv.Itoa(wantDeferred)) {
		t.Errorf("budget-cut line does not report the %d beats deferred (%d history + %d live): %v",
			wantDeferred, historyDue-wantSent, liveDue, rec.Records())
	}
	if got := rec.CountLevel(slog.LevelDebug, msg); got != 1 {
		t.Errorf("budget-cut debug lines = %d, want exactly 1 for the sweep: %v", got, rec.Messages())
	}

	if got, want := counterValue(t, "knell_notifications_sent_total", "history"), sentBefore+float64(wantSent); got != want {
		t.Errorf("sent{history} = %v, want %v (one per delivered message)", got, want)
	}
	if got := counterValue(t, "knell_notifications_failed_total", "history"); got != failedBefore {
		t.Errorf("failed{history} = %v after a budget-limited sweep, want unchanged %v (a deferred notice had no delivery attempt to fail)", got, failedBefore)
	}
	if got := counterValue(t, "knell_notifications_dropped_total", "missing"); got != droppedBefore {
		t.Errorf("dropped{missing} = %v after a budget-limited sweep, want unchanged %v (a deferred record is not lost)", got, droppedBefore)
	}

	// The deferral is a deferral: once sends are quick, the remaining history
	// notices go out, exactly one per beat.
	n.onMissing = nil
	n.onHistory = nil
	w.sweep(context.Background())
	perBeat := make(map[string]int, historyDue)
	for _, c := range n.snapshot() {
		if c.kind == "history" {
			perBeat[c.id]++
		}
	}
	for _, b := range histBeats {
		if got := perBeat[b.ID]; got != 1 {
			t.Errorf("beat %s received %d history notices across both sweeps, want exactly 1", b.ID, got)
		}
	}
}

func TestSweepStopsAtItsSendBudgetAndDefersTheRest(t *testing.T) {
	// Serial (no t.Parallel): asserts deltas on the package-global
	// notification counters, which the parallel tests also move.
	//
	// The failure this pins: when whatever fans the heartbeats out dies, every
	// beat crosses its deadline in the SAME sweep, and the single sender used
	// to push all of them through one rate-limited webhook back to back --
	// holding Run's select, and every queued recovery, for minutes. The sweep
	// must stop once its budget is spent instead. The beats it never reached
	// must then be untouched: not counted as failed (an attempt that will be
	// retried), not as dropped (a notice that will never arrive), and not
	// marked alerted, because nothing was attempted for them at all.
	const (
		total    = 12
		perSend  = 2 * time.Second
		deadline = 10 * time.Minute
	)
	beats := budgetProbeBeats("budget-cut-probe", total, deadline)
	clock := newFakeClock()
	n := &fakeNotifier{}
	w := New(beats, n, clock.Now, clock.Now())

	wantSent := sendsBeforeBudgetCut(perSend)
	if wantSent >= total {
		t.Fatalf("test precondition: %d sends fit in the %s budget at %s each, want fewer than the %d due beats",
			wantSent, sweepSendBudget, perSend, total)
	}

	// One sweep with every beat overdue: the boot-armed clock arms them all
	// from construction, so a single advance puts the whole fleet past its
	// deadline at once -- the delivery storm this budget exists for.
	clock.Advance(deadline + time.Minute)
	slowSends(n, clock, perSend)

	sentBefore := counterValue(t, "knell_notifications_sent_total", "missing")
	failedBefore := counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore := counterValue(t, "knell_notifications_dropped_total", "missing")
	outagesBefore := make(map[string]float64, len(beats))
	for _, b := range beats {
		outagesBefore[b.ID] = beatCounterValue(t, "knell_beat_outages_total", b.ID)
	}

	w.sweep(context.Background())

	got := n.snapshot()
	if len(got) != wantSent {
		t.Fatalf("notices in the budget-limited sweep = %d (%v), want %d: the sweep must stop starting sends once its %s budget is spent",
			len(got), got, wantSent, sweepSendBudget)
	}
	notified := make(map[string]bool, len(got))
	for i, c := range got {
		if c.kind != "missing" {
			t.Errorf("calls[%d] = %+v, want a missing notice", i, c)
		}
		notified[c.id] = true
	}
	if len(notified) != len(got) {
		t.Errorf("the sweep sent %d notices across only %d distinct beats: one notice per beat per sweep", len(got), len(notified))
	}

	// A beat the sweep never reached must be in exactly the state it was in
	// before the sweep: not alerted, and still holding the open record its
	// retry comes from. Which beats those are is deliberately not asserted --
	// Watcher stores beats in a map and promises no iteration order.
	for _, b := range beats {
		st := w.beats[b.ID]
		if notified[b.ID] {
			if !st.alerted {
				t.Errorf("beat %s was notified but is not marked alerted, so its outage would be announced twice", b.ID)
			}
			continue
		}
		if st.alerted {
			t.Errorf("beat %s was never reached by the sweep but is marked alerted: its outage would never be announced", b.ID)
		}
		if st.openMissing() == nil {
			t.Errorf("beat %s was never reached by the sweep but has no queued open record, so the next sweep has nothing to send", b.ID)
		}
	}

	if got, want := counterValue(t, "knell_notifications_sent_total", "missing"), sentBefore+float64(wantSent); got != want {
		t.Errorf("sent{missing} = %v, want %v (one per delivered notice: only the beats the sweep reached were delivered)", got, want)
	}
	if got := counterValue(t, "knell_notifications_failed_total", "missing"); got != failedBefore {
		t.Errorf("failed{missing} = %v after a budget-limited sweep, want unchanged %v (a beat the sweep never reached had no delivery attempt to fail)", got, failedBefore)
	}
	if got := counterValue(t, "knell_notifications_dropped_total", "missing"); got != droppedBefore {
		t.Errorf("dropped{missing} = %v after a budget-limited sweep, want unchanged %v (a deferred notice arrives on a later sweep, so nothing is lost for good)", got, droppedBefore)
	}
	// Detection is delivery-independent and happens in collectDue, before the
	// first send: every crossing counts as an outage whether or not this sweep
	// got as far as notifying it.
	for _, b := range beats {
		if got, want := beatCounterValue(t, "knell_beat_outages_total", b.ID), outagesBefore[b.ID]+1; got != want {
			t.Errorf("beat_outages_total{%s} = %v, want %v (a detected outage counts even when its notice is deferred)", b.ID, got, want)
		}
	}
}

func TestBudgetDeferredBeatsAreDeliveredOnALaterSweep(t *testing.T) {
	// Serial (no t.Parallel): asserts deltas on the package-global
	// notification counters, which the parallel tests also move.
	//
	// The whole premise of cutting a sweep short: the beats it did not reach
	// are DEFERRED, not lost. Nothing new retries them -- the existing
	// sweep-level retry does, because alerted flips only on a delivered send --
	// so if a cut beat could not be picked up later the budget would trade a
	// stalled sender for a swallowed outage, which is strictly worse.
	const (
		total    = 12
		perSend  = 2 * time.Second
		deadline = 10 * time.Minute
	)
	beats := budgetProbeBeats("budget-defer-probe", total, deadline)
	clock := newFakeClock()
	n := &fakeNotifier{}
	w := New(beats, n, clock.Now, clock.Now())

	clock.Advance(deadline + time.Minute)
	slowSends(n, clock, perSend)

	sentBefore := counterValue(t, "knell_notifications_sent_total", "missing")
	failedBefore := counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore := counterValue(t, "knell_notifications_dropped_total", "missing")

	// Keep sweeping at the same slow send rate: each sweep delivers another
	// budget's worth and defers the rest, so the storm drains over a handful
	// of ticks instead of one beat per tick.
	perBeat := make(map[string]int, total)
	sweeps := 0
	for len(perBeat) < total {
		if sweeps > total {
			t.Fatalf("still only %d of %d beats delivered after %d sweeps: deferred beats are not being retried", len(perBeat), total, sweeps)
		}
		w.sweep(context.Background())
		sweeps++
		perBeat = make(map[string]int, total)
		for _, c := range n.snapshot() {
			perBeat[c.id]++
		}
	}
	if sweeps < 2 {
		t.Fatalf("all %d beats were delivered in %d sweep(s): the budget never cut, so this test proves nothing about deferral", total, sweeps)
	}

	for _, b := range beats {
		if got := perBeat[b.ID]; got != 1 {
			t.Errorf("beat %s received %d notices across %d sweeps, want exactly 1 (one message per live outage)", b.ID, got, sweeps)
		}
		st := w.beats[b.ID]
		if !st.alerted {
			t.Errorf("beat %s is not marked alerted after its deferred notice was delivered", b.ID)
		}
		if len(st.pendingMissing) != 0 {
			t.Errorf("beat %s still holds %d queued record(s) after delivery, want 0", b.ID, len(st.pendingMissing))
		}
	}

	if got, want := counterValue(t, "knell_notifications_sent_total", "missing"), sentBefore+float64(total); got != want {
		t.Errorf("sent{missing} = %v after every deferred beat was delivered, want %v", got, want)
	}
	if got := counterValue(t, "knell_notifications_failed_total", "missing"); got != failedBefore {
		t.Errorf("failed{missing} = %v across the deferred sweeps, want unchanged %v (a deferral is not a failed delivery)", got, failedBefore)
	}
	if got := counterValue(t, "knell_notifications_dropped_total", "missing"); got != droppedBefore {
		t.Errorf("dropped{missing} = %v across the deferred sweeps, want unchanged %v (a deferral is not a permanent loss)", got, droppedBefore)
	}
}

func TestFastSendsDeliverEveryDueBeatInOneSweep(t *testing.T) {
	t.Parallel()

	// The healthy path must be untouched by the budget: a full fleet of quick
	// webhook posts takes well under a second, so a sweep that finds every
	// beat due still delivers every one of them in that single sweep. A budget
	// that bit here would delay real alerts by a tick for no reason.
	const (
		total    = 64 // the configured maximum (internal/config maxBeats)
		perSend  = 10 * time.Millisecond
		deadline = 10 * time.Minute
	)
	if spent := time.Duration(total) * perSend; spent >= sweepSendBudget {
		t.Fatalf("test precondition: %d sends at %s each spend %s, which is not well under the %s budget", total, perSend, spent, sweepSendBudget)
	}

	beats := budgetProbeBeats("budget-healthy-probe", total, deadline)
	clock := newFakeClock()
	n := &fakeNotifier{}
	w := New(beats, n, clock.Now, clock.Now())

	clock.Advance(deadline + time.Minute)
	slowSends(n, clock, perSend)
	w.sweep(context.Background())

	got := n.snapshot()
	if len(got) != total {
		t.Fatalf("notices in one healthy sweep = %d, want all %d due beats (fast sends must never hit the %s budget)", len(got), total, sweepSendBudget)
	}
	for _, b := range beats {
		st := w.beats[b.ID]
		if !st.alerted {
			t.Errorf("beat %s was not alerted by the healthy sweep", b.ID)
		}
		if len(st.pendingMissing) != 0 {
			t.Errorf("beat %s still holds %d queued record(s) after a delivered send, want 0", b.ID, len(st.pendingMissing))
		}
	}
}

func TestSweepStartsTheSendWhoseCheckLandsExactlyOnTheBudget(t *testing.T) {
	t.Parallel()

	// The budget is spent once the elapsed time is PAST it, so a check landing
	// exactly on it still starts one more send. Every other budget test uses a
	// per-send cost that steps over the boundary rather than onto it (three 2s
	// sends against a 5s budget), which is what leaves an off-by-one here
	// invisible: comparing for "reached" instead of "passed" satisfies all of
	// them while cutting one beat early on every storm sweep, delaying that
	// beat's notice by a whole tick.
	const (
		total    = 3
		deadline = 10 * time.Minute
	)
	wantSent := sendsBeforeBudgetCut(sweepSendBudget)
	if wantSent >= total {
		t.Fatalf("test precondition: %d sends fit in the %s budget at %s each, want fewer than the %d due beats",
			wantSent, sweepSendBudget, sweepSendBudget, total)
	}

	beats := budgetProbeBeats("budget-boundary-probe", total, deadline)
	clock := newFakeClock()
	n := &fakeNotifier{}
	w := New(beats, n, clock.Now, clock.Now())

	clock.Advance(deadline + time.Minute)
	// Every send burns the WHOLE budget, so the check after the first one sees
	// an elapsed time exactly equal to it.
	slowSends(n, clock, sweepSendBudget)
	w.sweep(context.Background())

	if got := len(n.snapshot()); got != wantSent {
		t.Errorf("notices in the sweep = %d, want %d: the send whose check lands exactly on the %s budget must still start",
			got, wantSent, sweepSendBudget)
	}
}

// newStormWatcher builds the delivery-storm fixture both send-ordering tests
// need: one short-deadline beat (recoverID) alerted on its own BEFORE the
// storm, plus storm beats named from prefix that all cross their deadline
// together. An alerted beat yields no notice, so the recovering beat takes no
// part in the storm sweep and its recovery is the only thing competing with
// it. On return, snapshot()[0] is that beat's pre-storm missing notice and
// every later call belongs to the storm -- the offset
// stormNoticesBeforeRecovery skips.
func newStormWatcher(t *testing.T, prefix, recoverID string, storm int, stormWindow time.Duration) (*Watcher, *fakeClock, *fakeNotifier) {
	t.Helper()
	clock := newFakeClock()
	n := &fakeNotifier{}
	beats := append(
		[]Beat{{ID: recoverID, Deadline: time.Minute}},
		budgetProbeBeats(prefix, storm, stormWindow)...,
	)
	w := New(beats, n, clock.Now, clock.Now())
	clock.Advance(2 * time.Minute)
	w.sweep(t.Context())
	if got := n.snapshot(); len(got) != 1 || got[0].kind != "missing" || got[0].id != recoverID {
		t.Fatalf("calls = %v, want one missing notice for %s before the storm", got, recoverID)
	}
	clock.Advance(stormWindow)
	return w, clock, n
}

// stormNoticesBeforeRecovery counts the storm notices delivered ahead of
// recoverID's recovered notice, skipping the pre-storm missing notice
// newStormWatcher leaves as calls[0]. found is false when the recovery never
// arrived at all.
func stormNoticesBeforeRecovery(n *fakeNotifier, recoverID string) (before int, found bool) {
	for _, c := range n.snapshot()[1:] {
		if c.kind == "recovered" && c.id == recoverID {
			return before, true
		}
		before++
	}
	return before, false
}

func TestRunServicesRecoveriesAfterABudgetLimitedSweep(t *testing.T) {
	t.Parallel()

	// The harm the budget actually fixes: while sweep is inside a delivery,
	// Run's select cannot touch the recoveries channel at all, so a recovered
	// notice queued during a delivery storm waits for the WHOLE storm. With
	// the budget, the sweep hands the select back after a bounded number of
	// sends and the recovery goes out then -- ahead of the beats this sweep
	// deferred, not behind all of them.
	synctest.Test(t, func(t *testing.T) {
		const (
			storm       = 12
			perSend     = 2 * time.Second
			stormWindow = 10 * time.Minute
			recoverID   = "budget-recovery-probe"
			tick        = 5 * time.Millisecond
		)
		// Phase 1, on this goroutine: the recovering beat is alerted alone and
		// the whole storm fleet is left due.
		w, clock, n := newStormWatcher(t, "budget-recovery-storm", recoverID, storm, stormWindow)

		// Phase 2: the first send of the storm sweep is when the recovering beat
		// pings -- so the recovery is queued while the sender is deep in the
		// storm.
		sends := 0
		n.onMissing = func() {
			sends++
			if sends == 1 {
				w.Beat(recoverID)
			}
			clock.Advance(perSend)
		}

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.Run(ctx, tick)
		}()

		time.Sleep(tick)
		synctest.Wait()

		stormsBefore, found := stormNoticesBeforeRecovery(n, recoverID)
		if !found {
			t.Fatalf("calls = %v, want the queued recovered notice delivered after the budget-limited sweep returned to the select", n.snapshot())
		}
		if want := sendsBeforeBudgetCut(perSend); stormsBefore != want {
			t.Errorf("storm notices ahead of the recovery = %d, want %d: the recovery must be serviced as soon as the sweep spends its budget, not after all %d storm beats",
				stormsBefore, want, storm)
		}

		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("Run did not stop on ctx cancel")
		}
	})
}

// TestHandleTickPrioritizesQueuedRecovery pins the drain-before-sweep ordering
// of Run's ticker arm, on the helper that arm delegates to. Driving it through
// Run cannot pin it: when a send overruns the tick, Run's top-level select has
// BOTH ticker.C and the queued recovery ready, and an unbiased select may take
// the recovery arm on its own — so a build with the ticker-arm drain DELETED
// still passes a Run-driven test most of the time (measured 17 of 30 runs).
// Calling handleTick directly removes the select randomness: with the drain
// deleted, sweep runs while the recovery is still queued and this test fails
// every run. TestRunServicesRecoveriesAfterABudgetLimitedSweep above stays the
// Run-loop integration check.
func TestHandleTickPrioritizesQueuedRecovery(t *testing.T) {
	t.Parallel()

	const (
		storm       = 12
		perSend     = 2 * time.Second
		stormWindow = 10 * time.Minute
		recoverID   = "overrun-recovery-probe"
	)
	w, clock, n := newStormWatcher(t, "overrun-recovery-storm", recoverID, storm, stormWindow)

	// The state the ticker arm sees when a send overran the tick: the whole
	// storm fleet is due AND a recovery is already sitting in the queue.
	if !recordedBeat(w, recoverID) {
		t.Fatalf("Beat(%s) = false, want the ping recorded so its recovery is queued", recoverID)
	}
	n.onMissing = func() { clock.Advance(perSend) } // spends the sweep budget

	w.handleTick(t.Context())

	stormsBefore, found := stormNoticesBeforeRecovery(n, recoverID)
	if !found {
		t.Fatalf("calls = %v, want the queued recovered notice delivered by the tick's own drain", n.snapshot())
	}
	if stormsBefore != 0 {
		t.Errorf("storm notices ahead of the recovery = %d, want 0: the ticker arm must drain the queued recovery BEFORE the sweep it consumed that tick for",
			stormsBefore)
	}
}

func TestHandleTickSkipsTheSweepOnAnAlreadyCancelledContext(t *testing.T) {
	t.Parallel()

	// The ticker arm drains a queued recovery before sweeping, and that
	// recovery's send is where shutdown is observed, so the context can already
	// be cancelled by the time the sweep would start. It must not start one: a
	// sweep on a dead context detects crossings, counts their outages and marks
	// beats alerted for notices it abandons in the same breath -- and an
	// alerted beat announces nothing after the restart until it crosses its
	// deadline again. Every other handleTick and Run test passes a live
	// context, and the skipped branch has no statement of its own.
	const id = "tick-cancelled-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	clock.Advance(11 * time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w.handleTick(ctx)

	if got := n.snapshot(); len(got) != 0 {
		t.Errorf("calls = %v, want none: a tick on a cancelled context must not start a sweep", got)
	}
	st := w.beats[id]
	if st.openMissing() != nil {
		t.Error("the cancelled tick queued a missing record, so its sweep ran")
	}
	if st.alerted {
		t.Error("the cancelled tick marked the beat alerted, so its outage would go unannounced after the restart")
	}
}

// anchorNotifier records the live Transition of every missing and recovered
// notice verbatim. fakeNotifier collapses both to DownFor, which is why a
// Transition with the right span and the wrong instants passes every other
// assertion in this package.
type anchorNotifier struct {
	missing   []Transition
	recovered []Transition
	mu        sync.Mutex
}

func (n *anchorNotifier) BeatMissing(_ context.Context, _ string, live Transition) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.missing = append(n.missing, live)
	return nil
}

func (n *anchorNotifier) BeatRecovered(_ context.Context, _ string, live Transition) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.recovered = append(n.recovered, live)
	return nil
}

func (*anchorNotifier) BeatOutageHistory(context.Context, string, []Outage) error { return nil }

// TestLiveNoticesCarryTheInstantsTheyClaim pins the two anchors of the live
// Transition, not only the span derived from them. The Notifier contract says
// Started is the beat's last accepted ping and Observed is the instant the
// notice speaks for -- the sweep that saw the beat silent, or the ping that
// ended the outage -- and a renderer is entitled to read either one directly.
// Nothing else in this package does: fakeNotifier records live.DownFor(), so a
// Transition with both instants shifted by the same amount keeps the whole
// suite green while every absolute timestamp derived from it is wrong.
func TestLiveNoticesCarryTheInstantsTheyClaim(t *testing.T) {
	t.Parallel()

	const id = "live-transition-anchor-probe"
	clock := newFakeClock()
	n := &anchorNotifier{}
	w := New([]Beat{{ID: id, Deadline: 10 * time.Minute}}, n, clock.Now, clock.Now())

	if !recordedBeat(w, id) {
		t.Fatalf("Beat(%s) = false", id)
	}
	lastPing := clock.Now()

	clock.Advance(11 * time.Minute)
	sweptAt := clock.Now()
	w.sweep(context.Background())

	clock.Advance(2 * time.Minute)
	endedAt := clock.Now()
	if !recordedBeat(w, id) {
		t.Fatalf("late Beat(%s) = false", id)
	}
	drainRecoveries(w)

	if len(n.missing) != 1 {
		t.Fatalf("missing notices = %d, want 1", len(n.missing))
	}
	if got := n.missing[0]; !got.Started.Equal(lastPing) || !got.Observed.Equal(sweptAt) {
		t.Errorf("missing notice Transition = {Started %s, Observed %s}, want {%s, %s}: Started must be the last accepted ping and Observed the sweep that saw the beat silent",
			got.Started, got.Observed, lastPing, sweptAt)
	}
	if len(n.recovered) != 1 {
		t.Fatalf("recovered notices = %d, want 1", len(n.recovered))
	}
	if got := n.recovered[0]; !got.Started.Equal(lastPing) || !got.Observed.Equal(endedAt) {
		t.Errorf("recovered notice Transition = {Started %s, Observed %s}, want {%s, %s}: Started must be the last accepted ping before the outage and Observed the ping that ended it",
			got.Started, got.Observed, lastPing, endedAt)
	}
}

// TestOutageEndedRequiresARecoveryPointNoEarlierThanItsStart pins the predicate
// notify's history path refuses an unfinished record with (BeatOutageHistory's
// per-entry guard). watch owns the type and the invariant, so its boundary
// belongs here: a record whose recovery point equals its start IS ended (the
// contract is "no earlier than", not "after"), while an all-zero record is NOT
// -- and the zero case is the only one the recovery-point clause alone rejects,
// since a zero Recovered against a real Started already fails the ordering
// clause. Inverting either clause makes the renderer refuse every history
// notice (no past-tense notice ever delivers, and the sweep retries it every
// tick forever) or publish "recovered at 0001-01-01" as a resolved outage.
func TestOutageEndedRequiresARecoveryPointNoEarlierThanItsStart(t *testing.T) {
	t.Parallel()

	started := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	cases := map[string]struct {
		outage Outage
		want   bool
	}{
		"recovered after its start":  {outage: Outage{Started: started, Recovered: started.Add(11 * time.Minute)}, want: true},
		"recovered at its start":     {outage: Outage{Started: started, Recovered: started}, want: true},
		"no recovery point":          {outage: Outage{Started: started}, want: false},
		"recovered before its start": {outage: Outage{Started: started, Recovered: started.Add(-time.Nanosecond)}, want: false},
		"zero record":                {outage: Outage{}, want: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := tc.outage.Ended(); got != tc.want {
				t.Errorf("Outage{Started: %s, Recovered: %s}.Ended() = %v, want %v",
					tc.outage.Started, tc.outage.Recovered, got, tc.want)
			}
		})
	}
}
