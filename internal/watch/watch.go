// Package watch implements the dead-man state machine: it tracks when each
// configured beat last pinged, declares a beat missing once its deadline of
// silence passes, and notifies on the missing and recovered transitions.
//
// The deadline clock for every beat starts at construction (process boot),
// so a beat that never pings at all still alerts one deadline after boot.
// That deliberately closes the classic dead-man blind spot where a receiver
// restart silently disarms the switch until the first ping re-arms it.
package watch

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/cplieger/knell/internal/config"
	"github.com/cplieger/knell/internal/metrics"
)

// Notifier delivers the two transition notifications. Implementations are
// expected to retry transient failures internally and return the final
// outcome.
type Notifier interface {
	// BeatMissing reports that id has been silent for silence (its deadline
	// has passed).
	BeatMissing(ctx context.Context, id string, silence time.Duration) error
	// BeatRecovered reports that id pinged again after having been declared
	// missing, downFor after its last accepted ping.
	BeatRecovered(ctx context.Context, id string, downFor time.Duration) error
}

// DefaultTick is the watch loop's check cadence. Deadlines are minutes to
// hours, so a fixed 15s sweep bounds alert latency without configuration.
const DefaultTick = 15 * time.Second

// recoveryQueueSize bounds the pending recovered-transition queue. Each
// configured beat can hold at most one pending recovery, so the config cap
// is also the queue bound.
const recoveryQueueSize = config.MaxBeats

// Notification kinds are the label values on the sent/failed notification
// counters; dashboards and the KnellNotifyFailing alert key on them.
const (
	kindMissing   = "missing"
	kindRecovered = "recovered"
)

// beatState is the per-beat tracking record.
type beatState struct {
	lastSeen time.Time
	deadline time.Duration
	alerted  bool
	// recovering marks a recovered transition that is queued or in flight;
	// sweep must not start another missing transition until it is
	// delivered, so transitions reach Discord in chronological order.
	recovering bool
}

// recoveryEvent is a queued recovered transition, measured at ping arrival.
type recoveryEvent struct {
	id      string
	downFor time.Duration
}

// Watcher tracks beat freshness and drives transition notifications. Beat is
// safe for concurrent use; Run is the single background sender so notify
// calls never hold the lock.
type Watcher struct {
	notifier   Notifier
	now        func() time.Time
	beats      map[string]*beatState
	recoveries chan recoveryEvent
	mu         sync.Mutex
}

// New builds a Watcher for the configured beats. The deadline clock of every
// beat starts at now(); pass time.Now in production.
func New(beats []config.Beat, notifier Notifier, now func() time.Time) *Watcher {
	w := &Watcher{
		notifier:   notifier,
		now:        now,
		beats:      make(map[string]*beatState, len(beats)),
		recoveries: make(chan recoveryEvent, recoveryQueueSize),
	}
	start := now()
	for _, b := range beats {
		w.beats[b.ID] = &beatState{lastSeen: start, deadline: b.Deadline}
		metrics.BeatFresh.Set(1, b.ID)
		metrics.BeatLastSeen.Set(float64(start.Unix()), b.ID)
		metrics.BeatsReceived.Add(0, b.ID)
	}
	// Pre-mint the notification counter series at zero so an increase()
	// alert sees the very first failure: a counter series born at a
	// nonzero value has no earlier sample to diff against.
	for _, kind := range []string{kindMissing, kindRecovered} {
		metrics.NotificationsSent.Add(0, kind)
		metrics.NotificationsFailed.Add(0, kind)
	}
	return w
}

// Beat records a ping for id. It returns false when id is not a configured
// beat (the caller answers 404 and nothing is recorded). A ping on an
// alerted beat queues the recovered notification for the Run loop, so this
// never blocks on the webhook.
func (w *Watcher) Beat(id string) bool {
	w.mu.Lock()
	st, ok := w.beats[id]
	if !ok {
		w.mu.Unlock()
		return false
	}
	now := w.now()
	downFor := now.Sub(st.lastSeen)
	wasAlerted := st.alerted
	st.lastSeen = now
	st.alerted = false
	if wasAlerted {
		st.recovering = true
	}
	// Publish the gauges under the lock so concurrent pings cannot write
	// them out of state order (an older timestamp overwriting a newer one).
	metrics.BeatsReceived.Inc(id)
	metrics.BeatFresh.Set(1, id)
	metrics.BeatLastSeen.Set(float64(now.Unix()), id)
	w.mu.Unlock()

	if wasAlerted {
		select {
		case w.recoveries <- recoveryEvent{id: id, downFor: downFor}:
		default:
			// Cannot happen while the queue bound matches the beat cap
			// (one pending recovery per beat), but never block a ping.
			// The dropped recovery is no longer pending, so un-mark it or
			// the beat could never alert again.
			w.mu.Lock()
			st.recovering = false
			w.mu.Unlock()
			metrics.NotificationsFailed.Inc(kindRecovered)
			slog.Warn("recovery queue full, dropping recovered notification", "beat", id)
		}
	}
	return true
}

// Run drives the watch loop until ctx is cancelled: a sweep every tick plus
// immediate delivery of queued recovered transitions. It is the only
// goroutine that calls the notifier.
func (w *Watcher) Run(ctx context.Context, tick time.Duration) {
	// Freshness gauges refresh on their own ticker: one overdue send can
	// block the sender loop for tens of seconds (3x10s attempts + backoff,
	// or 30s rate-limit waits), and the fresh gauge is the documented
	// ground truth precisely when the webhook path is down.
	go func() {
		gauges := time.NewTicker(tick)
		defer gauges.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-gauges.C:
				w.refreshFreshness()
			}
		}
	}()
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-w.recoveries:
			w.sendRecovered(ctx, ev)
		case <-ticker.C:
			w.sweep(ctx)
		}
	}
}

// refreshFreshness updates the per-beat freshness gauges without touching
// notification state, so the metric ground truth stays current even while
// the sender loop is blocked on a slow or unreachable webhook.
func (w *Watcher) refreshFreshness() {
	w.mu.Lock()
	now := w.now()
	for id, st := range w.beats {
		publishFreshness(id, now.Sub(st.lastSeen), st.deadline)
	}
	w.mu.Unlock()
}

// publishFreshness publishes the freshness gauge for id given its observed
// silence and deadline, reporting whether the beat is still fresh. It is the
// single home of the freshness boundary and gauge mapping shared by sweep
// and refreshFreshness, so the quorum ground truth cannot drift between the
// two writers. Callers hold w.mu.
func publishFreshness(id string, silence, deadline time.Duration) bool {
	if silence <= deadline {
		metrics.BeatFresh.Set(1, id)
		return true
	}
	metrics.BeatFresh.Set(0, id)
	return false
}

// overdueBeat is a beat collectOverdue found past its deadline, captured
// with the lastSeen observed when the sweep decided to notify.
type overdueBeat struct {
	seen    time.Time
	id      string
	silence time.Duration
}

// sweep checks every beat against its deadline and sends the missing
// notification for newly overdue beats. A failed send is not marked
// alerted, so the next sweep retries it; the beat stays in one Discord
// message per outage because alerted flips only on a delivered send.
// A delivered send that a ping raced (the beat pinged while the notice was
// in flight) emits the recovered transition immediately and leaves the beat
// armed for the next outage. Run calls it on every tick; in-package tests
// call it directly.
func (w *Watcher) sweep(ctx context.Context) {
	for _, beat := range w.collectOverdue() {
		if w.sendMissing(ctx, beat) {
			return
		}
	}
}

// collectOverdue publishes every beat's freshness gauge and returns the
// beats past their deadline that need a missing notification (not yet
// alerted and not mid-recovery).
func (w *Watcher) collectOverdue() []overdueBeat {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	var overdue []overdueBeat
	for id, st := range w.beats {
		silence := now.Sub(st.lastSeen)
		if publishFreshness(id, silence, st.deadline) || st.alerted || st.recovering {
			continue
		}
		overdue = append(overdue, overdueBeat{id: id, silence: silence, seen: st.lastSeen})
	}
	return overdue
}

// sendMissing delivers one due missing transition and reports whether
// shutdown cancellation should stop the sweep.
func (w *Watcher) sendMissing(ctx context.Context, beat overdueBeat) bool {
	if err := w.notifier.BeatMissing(ctx, beat.id, beat.silence); err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("missing notification abandoned, shutting down", "beat", beat.id)
			return true
		}
		metrics.NotificationsFailed.Inc(kindMissing)
		slog.Error("missing notification failed, will retry next sweep",
			"beat", beat.id, "silence", beat.silence.String(), "error", err)
		return false
	}
	metrics.NotificationsSent.Inc(kindMissing)
	slog.Info("beat missing, notified", "beat", beat.id, "silence", beat.silence.String())
	if event, raced := w.markDelivered(beat.id, beat.seen); raced {
		w.sendRecovered(ctx, event)
	}
	return false
}

// markDelivered records the outcome of a delivered missing send for id,
// given the lastSeen observed when the sweep decided to notify. Normally it
// marks the beat alerted. When a ping raced the send (lastSeen moved), Beat
// saw alerted=false and queued no recovery, and marking alerted now would
// swallow the NEXT outage's missing notice — so the beat stays re-armed and
// the pending recovered transition is returned for immediate delivery.
func (w *Watcher) markDelivered(id string, seen time.Time) (recoveryEvent, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st, ok := w.beats[id]
	if !ok {
		return recoveryEvent{}, false
	}
	if st.lastSeen.Equal(seen) {
		st.alerted = true
		return recoveryEvent{}, false
	}
	st.recovering = true
	return recoveryEvent{id: id, downFor: st.lastSeen.Sub(seen)}, true
}

// finishRecovery clears the pending-recovery mark for id, re-enabling sweep
// to start the beat's next missing transition.
func (w *Watcher) finishRecovery(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if st, ok := w.beats[id]; ok {
		st.recovering = false
	}
}

// sendRecovered delivers one queued recovered transition. Best-effort by
// design: the critical direction of a dead-man switch is missing, which has
// sweep-level retry; a lost recovery notice self-explains once the next
// missing alert arrives.
func (w *Watcher) sendRecovered(ctx context.Context, ev recoveryEvent) {
	defer w.finishRecovery(ev.id)
	if err := w.notifier.BeatRecovered(ctx, ev.id, ev.downFor); err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("recovered notification abandoned, shutting down", "beat", ev.id)
			return
		}
		metrics.NotificationsFailed.Inc(kindRecovered)
		slog.Error("recovered notification failed",
			"beat", ev.id, "down_for", ev.downFor.String(), "error", err)
		return
	}
	metrics.NotificationsSent.Inc(kindRecovered)
	slog.Info("beat recovered, notified", "beat", ev.id, "down_for", ev.downFor.String())
}
