package watch

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
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
}

func (n *fakeNotifier) BeatMissing(_ context.Context, id string, silence time.Duration) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fail != nil {
		return n.fail
	}
	if n.onMissing != nil {
		n.onMissing()
	}
	n.calls = append(n.calls, call{kind: "missing", id: id, elapsed: silence})
	return nil
}

func (n *fakeNotifier) BeatRecovered(_ context.Context, id string, downFor time.Duration) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fail != nil {
		return n.fail
	}
	n.calls = append(n.calls, call{kind: "recovered", id: id, elapsed: downFor})
	return nil
}

func (n *fakeNotifier) BeatOutageHistory(_ context.Context, id string, outages []Outage) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fail != nil {
		return n.fail
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
	out := make([]call, len(n.calls))
	copy(out, n.calls)
	return out
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

func newTestWatcher(beats ...Beat) (*Watcher, *fakeClock, *fakeNotifier) {
	clock := newFakeClock()
	notifier := &fakeNotifier{}
	return New(beats, notifier, clock.Now, clock.Now()), clock, notifier
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
	if w.Beat("ghost") {
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

func TestFreshBeatNeverNotifies(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	for range 10 {
		clock.Advance(5 * time.Minute)
		if !w.Beat("api") {
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
	checkSpans(t, onlyHistory(t, n), 11*time.Minute)
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

func TestRecoveryQueueOverflowDropKeepsBeatArmed(t *testing.T) {
	t.Parallel()

	// New sizes the recovery queue from the beat count, so the full-queue
	// path is defensive-only in production. Shrink the queue to one slot
	// short of the beat count before anything runs, so the final ping finds
	// it full and its recovered notification is dropped.
	const beatCount = 3
	beats := make([]Beat, beatCount)
	for i := range beats {
		beats[i] = Beat{ID: fmt.Sprintf("overflow-%02d", i), Deadline: 10 * time.Minute}
	}
	w, clock, n := newTestWatcher(beats...)
	w.recoveries = make(chan recoveryEvent, beatCount-1)

	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if got := len(n.snapshot()); got != len(beats) {
		t.Fatalf("missing notifications = %d, want %d", got, len(beats))
	}

	// Ping every beat without draining the queue: the first beatCount-1
	// pings queue their recovery, the last one finds the queue full and its
	// recovered notification is dropped.
	last := beats[len(beats)-1].ID
	for _, b := range beats {
		if !w.Beat(b.ID) {
			t.Fatalf("Beat(%s) = false", b.ID)
		}
	}

	// The dropped beat goes silent again while the queue is still full.
	// The drop path must un-mark recovering, or this beat could never
	// alert again -- the worst failure a dead-man switch can have.
	clock.Advance(11 * time.Minute)
	before := len(n.snapshot())
	w.sweep(context.Background())
	var reAlerted bool
	for _, c := range n.snapshot()[before:] {
		if c.kind == "missing" && c.id == last {
			reAlerted = true
		}
	}
	if !reAlerted {
		t.Fatalf("dropped-recovery beat %s did not re-alert; recovering flag leaked", last)
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

func (n *failFirstNotifier) BeatMissing(context.Context, string, time.Duration) error {
	n.attempts++
	if n.attempts == 1 {
		return errors.New("discord down")
	}
	return nil
}

func (*failFirstNotifier) BeatRecovered(context.Context, string, time.Duration) error {
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

// blockingNotifier blocks every BeatMissing until released, simulating a
// send stuck on a slow or unreachable webhook.
type blockingNotifier struct {
	entered chan struct{}
	release chan struct{}
}

func (n *blockingNotifier) BeatMissing(ctx context.Context, _ string, _ time.Duration) error {
	n.entered <- struct{}{}
	select {
	case <-n.release:
	case <-ctx.Done():
	}
	return ctx.Err()
}

func (n *blockingNotifier) BeatRecovered(context.Context, string, time.Duration) error {
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

	if !w.Beat("api") {
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
	if outages[0].Silence != 11*time.Minute {
		t.Errorf("detected silence = %s, want the full overdue interval 11m", outages[0].Silence)
	}
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
	if !w.Beat("api") {
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
	if !w.Beat("api") {
		t.Fatal("Beat(api) = false")
	}

	// Outage B begins AND ends entirely while A's missing notice is still
	// undelivered: another full deadline of silence, then a late ping.
	// Detection must not be gated on A's delivery, or B leaves no trace at
	// all -- no notification, no counter movement, the outage erased.
	clock.Advance(11 * time.Minute)
	if !w.Beat("api") {
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
	checkSpans(t, onlyHistory(t, n), 11*time.Minute, 11*time.Minute)
}

func TestThreeOutagesQueueWhileNoticesAreUndelivered(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(Beat{ID: "api", Deadline: 10 * time.Minute})
	w.Beat("api")
	n.setFail(errors.New("discord down"))

	// Outage A (11m): sweep-detected, then ended by a ping.
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if !w.Beat("api") {
		t.Fatal("Beat(api) = false ending outage A")
	}

	// Outage B (13m): detected by a sweep while A is still queued, so the
	// sweep itself must record a crossing behind an undelivered notice.
	clock.Advance(13 * time.Minute)
	w.sweep(context.Background())
	if !w.Beat("api") {
		t.Fatal("Beat(api) = false ending outage B")
	}

	// Outage C (17m): no sweep ever sees it -- a late ping records the
	// whole closed outage while A and B are both still queued.
	clock.Advance(17 * time.Minute)
	if !w.Beat("api") {
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
	checkSpans(t, outages, 11*time.Minute, 13*time.Minute, 17*time.Minute)
	wantSilence := []time.Duration{11 * time.Minute, 13 * time.Minute, 17 * time.Minute}
	for i, want := range wantSilence {
		if outages[i].Silence != want {
			t.Errorf("outage %d detected silence = %s, want %s (each outage keeps its own measurement)", i, outages[i].Silence, want)
		}
	}
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
		if !w.Beat(id) {
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
		if !w.Beat(id) {
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

func TestPendingMissingQueueOverflowIsAccountedNotSilent(t *testing.T) {
	// Serial (no t.Parallel): it asserts deltas on the package-global
	// notification counters, which the parallel tests also move.
	const id = "missing-overflow-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)

	// Fill the queue to its bound: every late ping past the deadline
	// records one whole closed outage, and no sweep runs to drain them.
	for range missingQueueSize {
		clock.Advance(11 * time.Minute)
		if !w.Beat(id) {
			t.Fatalf("Beat(%s) = false", id)
		}
	}
	if got := len(w.beats[id].pendingMissing); got != missingQueueSize {
		t.Fatalf("queued outages = %d, want the full bound %d", got, missingQueueSize)
	}

	// One more outage, of a distinctive length, overflows the queue. That
	// outage already ended, so its record was its last trace: no notice for
	// it will ever arrive. A permanent loss is what the DROPPED counter
	// reports (failed means a delivery attempt that will retry, which this
	// is not), and the outage must still count as detected.
	const droppedSilence = 47 * time.Minute
	failedBefore := counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore := counterValue(t, "knell_notifications_dropped_total", "missing")
	outagesBefore := beatCounterValue(t, "knell_beat_outages_total", id)
	clock.Advance(droppedSilence)
	if !w.Beat(id) {
		t.Fatalf("overflow Beat(%s) = false", id)
	}
	if got, want := counterValue(t, "knell_notifications_dropped_total", "missing"), droppedBefore+1; got != want {
		t.Errorf("dropped{missing} = %v after the queue-full drop, want %v (a lost outage must be accounted)", got, want)
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
	if !w.Beat("downfor-first-ping") {
		t.Fatal("Beat = false")
	}
	clock.Advance(5 * time.Minute)
	if !w.Beat("downfor-first-ping") {
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
	if outage.Silence != 11*time.Minute {
		t.Errorf("detected silence = %s, want exactly 11m (captured when the sweep first detected the outage)", outage.Silence)
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
	for range missingQueueSize {
		clock.Advance(11 * time.Minute)
		if !w.Beat(id) {
			t.Fatalf("Beat(%s) = false", id)
		}
	}

	// The beat now goes silent with no ping to end it, so the sweep -- not
	// Beat -- is the path that detects the crossing while the queue is at
	// its bound. The overflow must not mark the beat alerted: a dead-man
	// switch that swallows an ongoing outage because its queue was briefly
	// full is the worst failure it can have.
	failedBefore := counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore := counterValue(t, "knell_notifications_dropped_total", "missing")
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
	if !w.Beat(id) {
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
