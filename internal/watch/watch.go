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

// Beat is one watched beat as the state machine needs it: an id and the
// silence deadline that declares it missing. It is the watch package's own
// input type, so the state machine and its tests do not depend on how (or
// from where) the composition root parsed the configuration.
type Beat struct {
	ID       string
	Deadline time.Duration
}

// missingQueueSize bounds the per-beat queue of detected-but-undelivered
// missing transitions. Every queued entry costs a full deadline of silence
// (30s minimum, minutes to hours in practice) plus a ping, while the head
// is retried every tick, so a queue this deep only forms during a sustained
// webhook outage. It is deliberately small: the oldest notices are the
// actionable ones, and the bound keeps a stuck webhook from growing state
// without limit. Overflow is accounted, never silent, the same way a full
// recovery queue is: a closed outage is dropped, while an ongoing one is
// re-detected and queued once a slot opens.
const missingQueueSize = 8

// Notification kinds are the label values on the sent/failed notification
// counters; dashboards and the KnellNotifyFailing alert key on them.
const (
	kindMissing   = "missing"
	kindRecovered = "recovered"
)

// beatState is the per-beat tracking record.
type beatState struct {
	lastSeen time.Time
	// pendingMissing is the FIFO of detected missing transitions whose
	// notification is not yet delivered, oldest first. It retains the
	// evidence of an outage so a ping cannot erase it by moving lastSeen.
	// Detection appends to it independently of delivery: an outage that
	// both begins and ends while an earlier notice is still queued gets
	// its own entry instead of being collapsed into the earlier one.
	pendingMissing []overdueBeat
	deadline       time.Duration
	alerted        bool
	// recovering marks a recovered transition that is queued or in flight;
	// sweep must not send another missing transition until it is
	// delivered, so transitions reach Discord in chronological order.
	recovering bool
	// overflowAccounted records that the current outage has already moved
	// the failure counter and warning for a full pending queue. A closed
	// outage may be lost, while an ongoing outage remains detectable and is
	// queued after space opens; either case is accounted only once until a
	// successful push or the closing ping clears the flag.
	overflowAccounted bool
}

// headMissing returns the oldest queued missing transition — the one the
// sweep delivers and retries — or nil when nothing is queued.
func (st *beatState) headMissing() *overdueBeat {
	if len(st.pendingMissing) == 0 {
		return nil
	}
	return &st.pendingMissing[0]
}

// openMissing returns the queued record of an outage that has not ended
// yet, or nil when every queued outage already carries its recovery point.
// Records are appended in outage order and only a ping closes one, so the
// open record can only ever be the tail.
func (st *beatState) openMissing() *overdueBeat {
	if len(st.pendingMissing) == 0 {
		return nil
	}
	tail := &st.pendingMissing[len(st.pendingMissing)-1]
	if tail.recoveredAt.IsZero() {
		return tail
	}
	return nil
}

// pushMissing appends a detected missing transition, reporting false when
// the queue is already at its bound (the caller accounts for the overflow).
func (st *beatState) pushMissing(rec overdueBeat) bool {
	if len(st.pendingMissing) >= missingQueueSize {
		return false
	}
	st.pendingMissing = append(st.pendingMissing, rec)
	return true
}

// popMissing removes and returns the head, promoting the next queued
// transition. It returns the zero record on an empty queue: only the single
// sender pops, and only for a head collectOverdue handed it, so an empty pop
// cannot happen and the zero record degrades to the pre-queue behavior.
func (st *beatState) popMissing() overdueBeat {
	head := st.headMissing()
	if head == nil {
		return overdueBeat{}
	}
	rec := *head
	st.pendingMissing = st.pendingMissing[1:]
	return rec
}

// recordMissing queues a detected missing transition for the beat, or
// accounts for a queue-full event once when the queue is full. Dropping the
// newest keeps the queued chronology and the in-flight retry of the head
// intact, and every overflow moves the same failure counter (and log) a
// dropped recovery does, so saturation is never silent — once per affected
// outage, not once per sweep. A closed outage that overflows is lost; an
// ongoing one is re-detected and queued once a slot opens. Callers hold w.mu.
func recordMissing(st *beatState, rec overdueBeat) {
	if st.pushMissing(rec) {
		st.overflowAccounted = false
		return
	}
	if st.overflowAccounted {
		// The same outage a previous sweep already accounted for: counting
		// it every tick would report one affected outage as dozens.
		return
	}
	st.overflowAccounted = true
	metrics.NotificationsFailed.Inc(kindMissing)
	slog.Warn("pending missing queue full, missing notification not queued",
		"beat", rec.id, "queued", missingQueueSize, "silence", rec.silence.String(),
		"since", rec.seen)
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

// New builds a Watcher for the given beats. The deadline clock of every
// beat starts at now(); pass time.Now in production. The pending
// recovered-transition queue is sized from the beat count: each beat can
// hold at most one pending recovery, so that bound is exactly enough for a
// ping on every beat to queue without blocking.
func New(beats []Beat, notifier Notifier, now func() time.Time) *Watcher {
	w := &Watcher{
		notifier:   notifier,
		now:        now,
		beats:      make(map[string]*beatState, len(beats)),
		recoveries: make(chan recoveryEvent, len(beats)),
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
	previousSeen := st.lastSeen
	downFor := now.Sub(previousSeen)
	wasAlerted := st.alerted
	// A late ping ends an outage. When the sweep already recorded that
	// outage, seal the record it has not delivered yet; when the crossing
	// is one no sweep has seen at all, record the whole closed outage so
	// this ping cannot erase it. Recording no longer depends on the queue
	// being empty, so an outage that both begins AND ends while an earlier
	// notice is undelivered still reaches Discord instead of vanishing.
	if open := st.openMissing(); open != nil {
		open.recoveredAt = now
	} else if !wasAlerted && overdue(downFor, st.deadline) {
		recordMissing(st, overdueBeat{
			id:          id,
			silence:     downFor,
			seen:        previousSeen,
			recoveredAt: now,
		})
	}
	// This ping ends the beat's outage, so a later queue-full overflow
	// belongs to the NEXT outage and is accounted on its own.
	st.overflowAccounted = false
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
			// Cannot happen while the queue bound matches the beat count
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
	defer w.mu.Unlock()
	now := w.now()
	for id, st := range w.beats {
		publishFreshness(id, now.Sub(st.lastSeen), st.deadline)
	}
}

// overdue reports whether an observed silence has passed the beat's deadline.
// It is the single home of the freshness boundary: publishFreshness maps it to
// the quorum gauge and Beat uses it to decide whether a late ping closed an
// outage no sweep has seen, so the two paths cannot drift apart.
func overdue(silence, deadline time.Duration) bool {
	return silence > deadline
}

// publishFreshness publishes the freshness gauge for id given its observed
// silence and deadline, reporting whether the beat is still fresh. It is the
// single home of the gauge mapping shared by sweep and refreshFreshness, and
// it reads the boundary from overdue, so the quorum ground truth cannot drift
// between the two writers. Callers hold w.mu.
func publishFreshness(id string, silence, deadline time.Duration) bool {
	if !overdue(silence, deadline) {
		metrics.BeatFresh.Set(1, id)
		return true
	}
	metrics.BeatFresh.Set(0, id)
	return false
}

// overdueBeat is one detected missing transition: a beat past its deadline,
// captured with the lastSeen observed when the crossing was detected. It
// stays queued on the beat until its notification is delivered.
// recoveredAt is set once a ping ends the outage (zero while it is still
// ongoing), so the closing ping's recovery notice survives the wait.
type overdueBeat struct {
	seen        time.Time
	recoveredAt time.Time
	id          string
	silence     time.Duration
}

// sweep checks every beat against its deadline and sends the missing
// notification for newly overdue beats. A failed send is not marked
// alerted, so the next sweep retries it; the beat stays in one Discord
// message per outage because alerted flips only on a delivered send.
// A delivered send for an outage that is already over (a ping raced the
// notice, or ended the outage while it waited its turn) emits the recovered
// transition immediately and leaves the beat armed for the next outage.
// One missing notification per beat per sweep: a beat holding several
// undelivered outages drains them oldest-first, a tick apart. Run calls it
// on every tick; in-package tests call it directly.
func (w *Watcher) sweep(ctx context.Context) {
	for _, beat := range w.collectOverdue() {
		if w.sendMissing(ctx, beat) {
			return
		}
	}
}

// collectOverdue publishes every beat's freshness gauge and returns, per
// beat, the one missing notification due now: the head of its pending queue
// (a newly detected deadline crossing, an earlier crossing whose send failed
// and is being retried, or an earlier crossing still waiting its turn behind
// the notices queued before it). Detection is independent of delivery, so a
// crossing is recorded even while earlier notices are still queued; only
// beats mid-recovery are held back, keeping transitions chronological.
func (w *Watcher) collectOverdue() []overdueBeat {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	var due []overdueBeat
	for id, st := range w.beats {
		silence := now.Sub(st.lastSeen)
		fresh := publishFreshness(id, silence, st.deadline)
		// An overdue beat whose current outage is not on the queue yet is
		// a fresh crossing to record. Recording it here rather than
		// skipping the beat is what keeps a second outage alive while an
		// earlier notice is still undelivered.
		if !fresh && !st.alerted && st.openMissing() == nil {
			recordMissing(st, overdueBeat{id: id, silence: silence, seen: st.lastSeen})
		}
		head := st.headMissing()
		if head == nil {
			continue
		}
		// Only the still-open current outage refreshes its silence, so a
		// retry reports how long the beat has been quiet. Once a ping seals
		// the record, its silence freezes at the reading taken when the
		// outage was detected — the recovered notice carries the full span
		// (see markDelivered), so the pair stays readable together.
		if head.recoveredAt.IsZero() && head.seen.Equal(st.lastSeen) {
			head.silence = silence
		}
		// Held while an earlier recovery is queued or in flight, so
		// transitions reach Discord in chronological order.
		if st.recovering {
			continue
		}
		due = append(due, *head)
	}
	return due
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
// given the lastSeen observed when the sweep decided to notify. It pops the
// delivered transition, promoting any later queued outage to the head for
// the next sweep. Normally it marks the beat alerted. When the outage is
// already over — the popped record carries the recovery point a ping sealed
// into it, including a ping that raced this very send — marking alerted
// would swallow the NEXT outage's missing notice, so the beat stays
// re-armed and the pending recovered transition is returned for immediate
// delivery.
func (w *Watcher) markDelivered(id string, seen time.Time) (recoveryEvent, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.beats[id]
	delivered := st.popMissing()
	if delivered.recoveredAt.IsZero() && st.lastSeen.Equal(seen) {
		st.alerted = true
		return recoveryEvent{}, false
	}
	st.recovering = true
	// The sealed record is the authoritative recovery point (the FIRST ping
	// after the outage); st.lastSeen is only a defensive fallback for a
	// popped record that carries none, since a ping that moves lastSeen
	// seals the open head in the same critical section.
	recoveredAt := st.lastSeen
	if !delivered.recoveredAt.IsZero() {
		recoveredAt = delivered.recoveredAt
	}
	return recoveryEvent{id: id, downFor: recoveredAt.Sub(seen)}, true
}

// finishRecovery clears the pending-recovery mark for id, re-enabling sweep
// to start the beat's next missing transition.
func (w *Watcher) finishRecovery(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.beats[id].recovering = false
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
