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

// beatState is the per-beat tracking record.
type beatState struct {
	lastSeen time.Time
	deadline time.Duration
	alerted  bool
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
	w.mu.Unlock()

	metrics.BeatsReceived.Inc(id)
	metrics.BeatFresh.Set(1, id)
	metrics.BeatLastSeen.Set(float64(now.Unix()), id)

	if wasAlerted {
		select {
		case w.recoveries <- recoveryEvent{id: id, downFor: downFor}:
		default:
			// Cannot happen while the queue bound matches the beat cap
			// (one pending recovery per beat), but never block a ping.
			slog.Warn("recovery queue full, dropping recovered notification", "beat", id)
		}
	}
	return true
}

// Run drives the watch loop until ctx is cancelled: a sweep every tick plus
// immediate delivery of queued recovered transitions. It is the only
// goroutine that calls the notifier.
func (w *Watcher) Run(ctx context.Context, tick time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-w.recoveries:
			w.sendRecovered(ctx, ev)
		case <-ticker.C:
			w.Sweep(ctx)
		}
	}
}

// Sweep checks every beat against its deadline and sends the missing
// notification for newly overdue beats. A failed send is not marked
// alerted, so the next sweep retries it; the beat stays in one Discord
// message per outage because alerted flips only on a delivered send.
// Exported for tests; Run calls it on every tick.
func (w *Watcher) Sweep(ctx context.Context) {
	now := w.now()

	type due struct {
		id      string
		silence time.Duration
	}
	var overdue []due

	w.mu.Lock()
	for id, st := range w.beats {
		silence := now.Sub(st.lastSeen)
		if silence <= st.deadline {
			metrics.BeatFresh.Set(1, id)
			continue
		}
		metrics.BeatFresh.Set(0, id)
		if !st.alerted {
			overdue = append(overdue, due{id: id, silence: silence})
		}
	}
	w.mu.Unlock()

	for _, d := range overdue {
		if err := w.notifier.BeatMissing(ctx, d.id, d.silence); err != nil {
			metrics.NotificationsFailed.Inc("missing")
			slog.Error("missing notification failed, will retry next sweep",
				"beat", d.id, "silence", d.silence.String(), "error", err)
			continue
		}
		metrics.NotificationsSent.Inc("missing")
		slog.Info("beat missing, notified", "beat", d.id, "silence", d.silence.String())
		w.mu.Lock()
		if st, ok := w.beats[d.id]; ok {
			// Mark alerted even if a ping raced in during the send: the
			// missing message is out, so the next ping's recovered message
			// keeps the story consistent.
			st.alerted = true
		}
		w.mu.Unlock()
	}
}

// sendRecovered delivers one queued recovered transition. Best-effort by
// design: the critical direction of a dead-man switch is missing, which has
// sweep-level retry; a lost recovery notice self-explains once the next
// missing alert arrives.
func (w *Watcher) sendRecovered(ctx context.Context, ev recoveryEvent) {
	if err := w.notifier.BeatRecovered(ctx, ev.id, ev.downFor); err != nil {
		metrics.NotificationsFailed.Inc("recovered")
		slog.Error("recovered notification failed",
			"beat", ev.id, "down_for", ev.downFor.String(), "error", err)
		return
	}
	metrics.NotificationsSent.Inc("recovered")
	slog.Info("beat recovered, notified", "beat", ev.id, "down_for", ev.downFor.String())
}
