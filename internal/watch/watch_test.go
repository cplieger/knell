package watch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cplieger/knell/internal/config"
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

// call records one notifier invocation.
type call struct {
	kind    string
	id      string
	elapsed time.Duration
}

// fakeNotifier records calls and fails on demand. onMissing, when set, runs
// inside BeatMissing to interleave work with an in-flight send (set it from
// the same goroutine that calls sweep; no concurrent mutation).
type fakeNotifier struct {
	mu        sync.Mutex
	calls     []call
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

func newTestWatcher(beats ...config.Beat) (*Watcher, *fakeClock, *fakeNotifier) {
	clock := newFakeClock()
	notifier := &fakeNotifier{}
	return New(beats, notifier, clock.Now), clock, notifier
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

	w, _, n := newTestWatcher(config.Beat{ID: "api", Deadline: time.Minute})
	if w.Beat("ghost") {
		t.Error("Beat(ghost) = true, want false")
	}
	if got := n.snapshot(); len(got) != 0 {
		t.Errorf("unknown id caused notifications: %v", got)
	}
}

func TestFreshBeatNeverNotifies(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(config.Beat{ID: "api", Deadline: 10 * time.Minute})
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

	w, clock, n := newTestWatcher(config.Beat{ID: "api", Deadline: 10 * time.Minute})
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

	w, clock, n := newTestWatcher(config.Beat{ID: "api", Deadline: 10 * time.Minute})

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

	w, clock, n := newTestWatcher(config.Beat{ID: "api", Deadline: 10 * time.Minute})
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

	w, clock, n := newTestWatcher(config.Beat{ID: "api", Deadline: 10 * time.Minute})
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

	w, clock, n := newTestWatcher(config.Beat{ID: "api", Deadline: 10 * time.Minute})
	w.Beat("api")
	clock.Advance(11 * time.Minute)
	n.setFail(errors.New("discord down"))
	w.sweep(context.Background())
	w.Beat("api")
	n.setFail(nil)
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 2 || got[0].kind != "missing" || got[1].kind != "recovered" {
		t.Fatalf("calls = %v, want the pending missing followed by recovered", got)
	}
}

func TestSecondOutageNotifiesAgain(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(config.Beat{ID: "api", Deadline: 10 * time.Minute})
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

	w, clock, n := newTestWatcher(config.Beat{ID: "api", Deadline: 10 * time.Minute})
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
		config.Beat{ID: "fast", Deadline: time.Minute},
		config.Beat{ID: "slow", Deadline: time.Hour},
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
		w, clock, n := newTestWatcher(config.Beat{ID: "api", Deadline: time.Minute})
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

	w, clock, n := newTestWatcher(config.Beat{ID: "api", Deadline: 10 * time.Minute})
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

	w, clock, n := newTestWatcher(config.Beat{ID: "api", Deadline: 10 * time.Minute})
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

	// One more beat than the queue bound: watch.New enforces no cap of its
	// own (config.ParseBeats does), so recoveryQueueSize+1 beats can all
	// hold a pending recovery at once and the final ping must take the
	// full-queue drop path.
	beats := make([]config.Beat, recoveryQueueSize+1)
	for i := range beats {
		beats[i] = config.Beat{ID: fmt.Sprintf("overflow-%02d", i), Deadline: 10 * time.Minute}
	}
	w, clock, n := newTestWatcher(beats...)

	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if got := len(n.snapshot()); got != len(beats) {
		t.Fatalf("missing notifications = %d, want %d", got, len(beats))
	}

	// Ping every beat without draining the queue: the first
	// recoveryQueueSize pings queue their recovery, the last one finds the
	// queue full and its recovered notification is dropped.
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

func TestFreshnessGaugeUpdatesWhileSenderBlocked(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// Unique beat ids: the metric registry is package-global.
		clock := newFakeClock()
		n := &blockingNotifier{entered: make(chan struct{}, 8), release: make(chan struct{})}
		w := New([]config.Beat{
			{ID: "blocked-sender-a", Deadline: 10 * time.Minute},
			{ID: "blocked-sender-b", Deadline: 30 * time.Minute},
		}, n, clock.Now)

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

func TestMarkDeliveredUnknownBeatIsNoOp(t *testing.T) {
	t.Parallel()

	w, _, n := newTestWatcher(config.Beat{ID: "api", Deadline: 10 * time.Minute})
	ev, raced := w.markDelivered("ghost", time.Time{})
	if raced || ev != (recoveryEvent{}) {
		t.Errorf("markDelivered(ghost) = (%+v, %t), want zero event and false", ev, raced)
	}
	if got := n.snapshot(); len(got) != 0 {
		t.Errorf("unknown-id markDelivered caused notifications: %v", got)
	}
}
