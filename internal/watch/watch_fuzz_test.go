package watch

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/knell/internal/metrics"
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
		if rec.silence.DownFor() <= deadline {
			t.Fatalf("ops %q: record %d queued a silence of %s, at or below the %s deadline", ops, i, rec.silence.DownFor(), deadline)
		}
		if rec.recoveredAt.IsZero() && i != len(q)-1 {
			t.Fatalf("ops %q: record %d of %d is still open but is not the tail, so a ping would seal the wrong outage", ops, i, len(q))
		}
		if !rec.recoveredAt.IsZero() && !rec.recoveredAt.After(rec.silence.Started) {
			t.Fatalf("ops %q: record %d recovered at %s, not after its last-seen %s", ops, i, rec.recoveredAt, rec.silence.Started)
		}
		if i > 0 && q[i-1].silence.Started.After(rec.silence.Started) {
			t.Fatalf("ops %q: record %d was last seen before record %d, so the queue is out of chronological order", ops, i, i-1)
		}
		// Chronological order alone still allows two queued records to cover
		// overlapping intervals, which a collapsed history notice would report
		// as an outage that started before the previous one ended. A record's
		// outage must begin at or after the previous record's recovery point.
		if i > 0 && !q[i-1].recoveredAt.IsZero() && q[i-1].recoveredAt.After(rec.silence.Started) {
			t.Fatalf("ops %q: record %d starts at %s, inside record %d which recovered at %s: a history notice would report overlapping outages",
				ops, i, rec.silence.Started, i-1, q[i-1].recoveredAt)
		}
	}
}

// checkSwitchStaysArmed asserts the dead-man switch's liveness property after
// a sweep: an overdue beat is never left with nothing pending. Either it is
// alerted (its live notice went out), or its current outage is still queued as
// the open record, or the outage is detected and waiting for a queue slot
// (overflowAccounted). Anything else means the sweep saw the beat overdue and
// silently dropped its outage -- the worst failure a dead-man switch can have,
// and the one the queue's structural invariants cannot catch, because a queue
// that never recorded the outage at all satisfies every one of them.
func checkSwitchStaysArmed(t *testing.T, w *Watcher, id string, now time.Time, ops string) {
	t.Helper()
	st := w.beats[id]
	if !overdue(now.Sub(st.lastSeen), st.deadline) {
		return
	}
	if st.alerted || st.openMissing() != nil || st.overflowAccounted {
		return
	}
	t.Fatalf("ops %q: beat is overdue after a sweep but is not alerted, has no queued open record and is not marked as awaiting a queue slot: its outage was swallowed", ops)
}

// checkHistoryPayloads asserts the Notifier contract on what the notifier
// actually RECEIVED, across every history notice delivered so far: outages is
// never empty, every reported outage is closed with a positive span, and the
// outages form one chronological, non-overlapping sequence across notices. The
// last clause is the one the queue's structural invariants cannot express: an
// outage reported in two different notices (a delivered run that was not
// consumed) shows up as a notice whose first outage starts before the
// previously reported one recovered. That clause assumes a SINGLE beat:
// fakeNotifier.histories is not keyed by beat and two beats' outages may
// legitimately overlap, so only call this on a one-beat watcher.
func checkHistoryPayloads(t *testing.T, n *fakeNotifier, ops string) {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	var lastRecovered time.Time
	for i, outages := range n.histories {
		if len(outages) == 0 {
			t.Fatalf("ops %q: history notice %d reported no outages at all, but the contract says outages is never empty", ops, i)
		}
		for j, o := range outages {
			// DownFor is Recovered.Sub(Started), so this preserves the old
			// closed-and-ordered predicate through the span the notice renders.
			// The separate positive-Silence predicate became inapplicable when
			// that unread detection-time field was removed; its stronger
			// internal counterpart remains in checkMissingQueueInvariants.
			if o.DownFor() <= 0 {
				t.Fatalf("ops %q: history notice %d outage %d reports a span of %s (started %s, recovered %s): a past-tense notice must only report ended outages, each with a positive span",
					ops, i, j, o.DownFor(), o.Started, o.Recovered)
			}
			if !lastRecovered.IsZero() && o.Started.Before(lastRecovered) {
				t.Fatalf("ops %q: history notice %d outage %d starts at %s, before the previously reported outage recovered at %s: the same outage was reported twice, or two notices overlap",
					ops, i, j, o.Started, lastRecovered)
			}
			lastRecovered = o.Recovered
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
	// A crossing detected while an earlier recovery is still queued, then
	// drained by the Run loop, interleaved with silences that stay inside the
	// deadline: the recovering window and the partial-silence op are the two
	// the corpus above never reaches, and the weekly runner discards whatever
	// it explores, so only a committed seed covers them on every PR.
	f.Add("pAspAsrsaspAs")
	f.Add("pAsparAsrpaAsr")
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
				checkSwitchStaysArmed(t, w, id, clock.Now(), ops)
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
			checkHistoryPayloads(t, n, ops)
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

// beatFreshSeriesCount counts the beat_fresh series currently in the
// exposition. It is the label-cardinality ground truth: a series minted for an
// id nobody configured is permanent, unbounded cardinality in knell and in
// every observer scraping it, so the count must not move for an unknown id.
func beatFreshSeriesCount() int {
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	n := 0
	for line := range strings.Lines(rec.Body.String()) {
		if strings.HasPrefix(line, "knell_beat_fresh{") {
			n++
		}
	}
	return n
}

// FuzzBeatIDMintsNoSeriesForUnconfiguredID fuzzes the untrusted boundary the
// beat id arrives on. webapi hands Beat the decoded URL path segment verbatim,
// so an escaped slash, a NUL, a newline, a quote or a 300-byte segment all
// reach this function as an id. That id then becomes a Prometheus label value,
// which makes the configured-id map the only thing between an arbitrary
// request path and permanent label cardinality -- and a raw newline or quote
// in a label value corrupts the exposition carrying the quorum signal. Both
// invariants are independent of the map itself: acceptance matches the
// configured set exactly, and no call for any other id changes the series
// count.
func FuzzBeatIDMintsNoSeriesForUnconfiguredID(f *testing.F) {
	const probe = "fuzz-id-boundary-probe"
	f.Add(probe)
	f.Add("")
	f.Add("a/b")
	f.Add("api\nx")
	f.Add(`api"x`)
	f.Add("api\x00")
	f.Add(strings.Repeat("x", 300))
	f.Add(`knell_beat_fresh{beat="injected"} 1`)
	f.Fuzz(func(t *testing.T, id string) {
		w, _, _ := newTestWatcher(Beat{ID: probe, Deadline: 10 * time.Minute})
		before := beatFreshSeriesCount()
		if got, want := recordedBeat(w, id), id == probe; got != want {
			t.Fatalf("Beat(%q) = %v, want %v (only a configured id may be accepted)", id, got, want)
		}
		if after := beatFreshSeriesCount(); after != before {
			t.Fatalf("Beat(%q) moved the beat_fresh series count from %d to %d: an unconfigured id minted a label series", id, before, after)
		}
	})
}
