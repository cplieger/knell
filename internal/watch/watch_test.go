package watch

import (
	"context"
	"errors"
	"sync"
	"testing"
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

// fakeNotifier records calls and fails on demand.
type fakeNotifier struct {
	mu    sync.Mutex
	calls []call
	fail  error
}

func (n *fakeNotifier) BeatMissing(_ context.Context, id string, silence time.Duration) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.fail != nil {
		return n.fail
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
		w.Sweep(context.Background())
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
	w.Sweep(context.Background())
	w.Sweep(context.Background())
	clock.Advance(time.Hour)
	w.Sweep(context.Background())

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
	w.Sweep(context.Background())
	if got := n.snapshot(); len(got) != 0 {
		t.Fatalf("notified before boot deadline: %v", got)
	}

	clock.Advance(2 * time.Minute)
	w.Sweep(context.Background())
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
	w.Sweep(context.Background())

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
	w.Sweep(context.Background())
	w.Sweep(context.Background())
	if got := n.snapshot(); len(got) != 0 {
		t.Fatalf("failed sends recorded calls: %v", got)
	}

	n.setFail(nil)
	w.Sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0].kind != "missing" {
		t.Fatalf("calls = %v, want one missing after recovery of the notifier", got)
	}

	// Delivered once: further sweeps stay silent.
	w.Sweep(context.Background())
	if got := n.snapshot(); len(got) != 1 {
		t.Errorf("post-delivery sweep re-sent: %v", got)
	}
}

func TestSecondOutageNotifiesAgain(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(config.Beat{ID: "api", Deadline: 10 * time.Minute})
	w.Beat("api")

	clock.Advance(11 * time.Minute)
	w.Sweep(context.Background())
	w.Beat("api")
	drainRecoveries(w)

	clock.Advance(11 * time.Minute)
	w.Sweep(context.Background())

	got := n.snapshot()
	if len(got) != 3 {
		t.Fatalf("calls = %v, want missing/recovered/missing", got)
	}
	if got[2].kind != "missing" {
		t.Errorf("third call = %+v, want missing", got[2])
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
	w.Sweep(context.Background())

	got := n.snapshot()
	if len(got) != 1 || got[0].id != "fast" {
		t.Fatalf("calls = %v, want only fast missing", got)
	}
}

func TestRunLoopDeliversSweepAndRecovery(t *testing.T) {
	t.Parallel()

	w, clock, n := newTestWatcher(config.Beat{ID: "api", Deadline: time.Minute})
	w.Beat("api")
	clock.Advance(2 * time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx, 5*time.Millisecond)
	}()

	waitFor(t, func() bool {
		calls := n.snapshot()
		return len(calls) == 1 && calls[0].kind == "missing"
	}, "missing notification via Run loop")

	w.Beat("api")
	waitFor(t, func() bool {
		calls := n.snapshot()
		return len(calls) == 2 && calls[1].kind == "recovered"
	}, "recovered notification via Run loop")

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop on ctx cancel")
	}
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
