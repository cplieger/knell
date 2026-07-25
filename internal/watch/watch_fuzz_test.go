package watch

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/knell/internal/config"
)

// checkMissingQueueInvariants asserts the pending-missing queue's structural
// contract: bounded length, at most one still-open outage and only at the
// tail, chronological detection order, and every queued record describing a
// real deadline crossing of this beat.
func checkMissingQueueInvariants(t *testing.T, w *Watcher, id string, deadline time.Duration, ops string) {
	t.Helper()
	q := w.beats[id].pendingMissing
	if len(q) > missingQueueSize {
		t.Fatalf("ops %q: queue holds %d records, want at most %d", ops, len(q), missingQueueSize)
	}
	for i, rec := range q {
		if rec.id != id {
			t.Fatalf("ops %q: record %d has id %q, want %q", ops, i, rec.id, id)
		}
		if rec.silence <= deadline {
			t.Fatalf("ops %q: record %d queued a silence of %s, at or below the %s deadline", ops, i, rec.silence, deadline)
		}
		if rec.recoveredAt.IsZero() && i != len(q)-1 {
			t.Fatalf("ops %q: record %d of %d is still open but is not the tail, so a ping would seal the wrong outage", ops, i, len(q))
		}
		if !rec.recoveredAt.IsZero() && !rec.recoveredAt.After(rec.seen) {
			t.Fatalf("ops %q: record %d recovered at %s, not after its last-seen %s", ops, i, rec.recoveredAt, rec.seen)
		}
		if i > 0 && q[i-1].seen.After(rec.seen) {
			t.Fatalf("ops %q: record %d was last seen before record %d, so the queue is out of chronological order", ops, i, i-1)
		}
	}
}

// FuzzMissingQueue drives the state machine with a fuzzed stream of pings,
// sweeps, clock advances and webhook outages, and pins the pending-missing
// queue's structural invariants after every operation (checkMissingQueue-
// Invariants). The table tests cover named scenarios; this covers the
// interleavings nobody enumerated, which is where a queue with four
// interacting rules (append on detection, seal on ping, retry the head, drop
// the newest when full) breaks.
func FuzzMissingQueue(f *testing.F) {
	f.Add("pAsp")
	f.Add("fpAsAspAsoss")
	f.Add("pAAAsssppAs")
	f.Add("fAsAsAsAsAsAsAsAsAsoss")
	f.Add("pAspArfAsAspos")
	// Enough closed outages to fill the queue and take the overflow path.
	f.Add(strings.Repeat("Ap", missingQueueSize+2))
	f.Fuzz(func(t *testing.T, ops string) {
		const (
			id       = "fuzz-queue-probe"
			deadline = 10 * time.Minute
		)
		w, clock, n := newTestWatcher(config.Beat{ID: id, Deadline: deadline})
		for _, op := range ops {
			switch op {
			case 'p': // a ping
				w.Beat(id)
			case 'A': // a full deadline of silence
				clock.Advance(deadline + time.Minute)
			case 'a': // a partial deadline of silence
				clock.Advance(deadline / 3)
			case 's': // a watch sweep
				w.sweep(context.Background())
			case 'r': // the Run loop draining queued recoveries
				drainRecoveries(w)
			case 'f': // the webhook goes down
				n.setFail(errors.New("discord down"))
			case 'o': // the webhook comes back
				n.setFail(nil)
			default:
				continue
			}
			checkMissingQueueInvariants(t, w, id, deadline, ops)
		}
	})
}
