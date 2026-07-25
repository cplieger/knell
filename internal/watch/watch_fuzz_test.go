package watch

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
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
// interleavings nobody enumerated, which is where a queue with five
// interacting rules (append on detection, seal on ping, retry the head, drop
// the newest when full, pop the whole ended run on a delivered history
// notice) breaks.
func FuzzMissingQueue(f *testing.F) {
	f.Add("pAsp")
	f.Add("fpAsAspAsoss")
	f.Add("pAAAsssppAs")
	f.Add("fAsAsAsAsAsAsAsAsAsoss")
	f.Add("pAspArfAsAspos")
	// Ended outages queued behind a webhook outage, then drained as one
	// collapsed history notice (the multi-record pop).
	f.Add("fpApApApAsossA")
	// Enough closed outages to fill the queue and take the overflow path.
	f.Add(strings.Repeat("Ap", missingQueueSize+2))
	// The same, then drained in a single sweep and re-armed.
	f.Add(strings.Repeat("Ap", missingQueueSize+2) + "ssAs")
	f.Fuzz(func(t *testing.T, ops string) {
		const (
			id       = "fuzz-queue-probe"
			deadline = 10 * time.Minute
		)
		w, clock, n := newTestWatcher(Beat{ID: id, Deadline: deadline})
		for _, op := range ops {
			// The structural invariants below hold just as well if the queue
			// dropped its OLDEST record and kept the incoming one, so they
			// cannot pin the documented drop-the-newest overflow policy.
			// Snapshot the full queue before an op that would overflow it, and
			// require it to come back unchanged: that postcondition fails on a
			// drop-oldest mutation.
			st := w.beats[id]
			beforeQueue := slices.Clone(st.pendingMissing)
			newestShouldDrop := op == 'p' &&
				len(beforeQueue) == missingQueueSize &&
				st.openMissing() == nil &&
				!st.alerted &&
				overdue(clock.Now().Sub(st.lastSeen), st.deadline)

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
			if newestShouldDrop {
				got := w.beats[id].pendingMissing
				if len(got) != len(beforeQueue) {
					t.Fatalf("ops %q: newest-drop changed queue length from %d to %d", ops, len(beforeQueue), len(got))
				}
				for i := range beforeQueue {
					if got[i] != beforeQueue[i] {
						t.Fatalf("ops %q: newest-drop changed queued record %d from %+v to %+v", ops, i, beforeQueue[i], got[i])
					}
				}
			}
		}
	})
}
