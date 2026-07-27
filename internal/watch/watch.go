// Package watch implements the dead-man state machine: it tracks when each
// configured beat last pinged, declares a beat missing once its deadline of
// silence passes, and notifies on the missing and recovered transitions.
//
// The deadline clock for every beat starts at construction (process boot),
// so a beat that never pings at all still alerts one deadline after boot.
// That deliberately closes the classic dead-man blind spot where a receiver
// restart silently disarms the switch until the first ping re-arms it.
//
// Notifications are split by whether the incident is still open. A live
// outage gets the present-tense missing notice and, when the beat returns,
// its recovered notice. Outages that had already ended before their notice
// could be delivered (a webhook outage held them back) are reported once as
// history, so a resolved incident is never announced as a new live failure.
package watch

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/cplieger/knell/internal/metrics"
)

// Notifier delivers the transition notifications. Implementations are
// expected to retry transient failures internally and return the final
// outcome.
//
// The first two methods report a LIVE incident as it happens; the third
// reports outages that were already over by the time their notice could be
// delivered, so an implementation must render it in the past tense. Nothing
// else distinguishes the two: a live outage never reaches
// BeatOutageHistory, and a history notice is never followed by
// BeatRecovered calls for the outages it covers.
type Notifier interface {
	// BeatMissing reports that id has been silent for silence (its deadline
	// has passed) and the outage is still open.
	BeatMissing(ctx context.Context, id string, silence time.Duration) error
	// BeatRecovered reports that id pinged again after having been declared
	// missing, downFor after its last accepted ping.
	BeatRecovered(ctx context.Context, id string, downFor time.Duration) error
	// BeatOutageHistory reports outages of id that had already ended before
	// their notice could be delivered, collapsed into a single past-tense
	// notice. outages is chronological and never empty, and every entry
	// carries its own recovery point, so the implementation must not
	// present them as ongoing.
	BeatOutageHistory(ctx context.Context, id string, outages []Outage) error
}

// Outage is one already-ended outage as the state machine observed it, the
// unit BeatOutageHistory reports. It is the watch package's own output type,
// so the notifier renders the outage from the state machine's shape without
// the state machine depending on how (or where) it is rendered.
type Outage struct {
	// Started is the last accepted ping before the outage: the instant the
	// beat's silence began.
	Started time.Time
	// Recovered is the first accepted ping after the outage: the instant it
	// ended. Always after Started.
	Recovered time.Time
	// Silence is how long the beat had been quiet when the deadline crossing
	// was detected. It is a reading taken during the outage, so it is at or
	// below the outage's full span; DownFor is the span itself.
	Silence time.Duration
}

// DownFor is the outage's full span: from the last ping before it to the
// first ping after it. It is the figure a past-tense notice reports ("was
// missing for ..."), derived here so every renderer measures it the same way.
func (o Outage) DownFor() time.Duration {
	return o.Recovered.Sub(o.Started)
}

// LongestOutage returns the longest span among outages, or zero for an empty
// slice. It lives here, next to the type, so the notifier's summary and the
// sender's log line report the same figure by construction.
func LongestOutage(outages []Outage) time.Duration {
	var longest time.Duration
	for _, o := range outages {
		longest = max(longest, o.DownFor())
	}
	return longest
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
// without limit. Overflow drops the NEWEST record; recordEndedOutage and
// recordOngoingOutage own what a full queue MEANS for their case.
const missingQueueSize = 8

// overdueBeat is one detected missing transition: a beat past its deadline,
// captured with the lastSeen observed when the crossing was detected. It
// stays queued on the beat until its notification is delivered.
// recoveredAt is set once a ping ends the outage (zero while it is still
// ongoing), so the ended outage's span survives the wait and its notice can
// report it in the past tense instead of as a live failure.
type overdueBeat struct {
	seen        time.Time
	recoveredAt time.Time
	id          string
	silence     time.Duration
}

// beatState is the per-beat tracking record.
type beatState struct {
	lastSeen time.Time
	// pendingMissing is the FIFO of detected missing transitions whose
	// notification is not yet delivered, oldest first. It retains the
	// evidence of an outage so a ping cannot erase it by moving lastSeen.
	// Detection appends to it independently of delivery: an outage that
	// both begins and ends while an earlier notice is still queued gets
	// its own entry, so its own span survives. Delivery then collapses the
	// ended entries at the head into a single history notice; the records
	// stay distinct, only the message is one.
	pendingMissing []overdueBeat
	deadline       time.Duration
	alerted        bool
	// recovering marks a recovered transition that is queued or in flight;
	// sweep must not send another missing transition until it is
	// delivered, so transitions reach Discord in chronological order.
	recovering bool
	// overflowAccounted records that the current outage has already been
	// counted as detected (metrics.RecordOutage) and has already reported the
	// full pending queue, so a sweep that re-detects the same still-unqueued
	// outage every tick neither counts it twice nor repeats its log line.
	// recordOngoingOutage is its only setter: only that path repeats for one
	// and the same outage. A successful push clears it, and so does the ping
	// that ends the outage, so the next outage is accounted on its own.
	overflowAccounted bool
}

// headMissing returns the oldest queued missing transition — the one whose
// state decides the beat's next notice (open: a live missing notice to
// deliver and retry; ended: the start of a history run) — or nil when
// nothing is queued.
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
// sender pops, and only for a head collectDue handed it, so an empty pop
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

// dropMissing removes the first n queued records: the run of already-ended
// outages a delivered history notice covered. Only the single sender pops,
// and it drops exactly the count it collected from the head, so n can never
// exceed the queue length; the clamp keeps a future caller in range.
func (st *beatState) dropMissing(n int) {
	st.pendingMissing = st.pendingMissing[min(n, len(st.pendingMissing)):]
}

// closedRun returns the already-ended outages queued in an unbroken run at
// the head, oldest first, or nil when the head is still an open outage. They
// are the beat's history: one collapsed past-tense notice covers all of them
// (see sendHistory), instead of replaying each as a live missing transition.
// Records are appended in outage order and only the tail can be open, so the
// run is the whole queue minus an open tail; the loop stops at the first open
// record regardless, so the run cannot silently depend on that invariant.
func (st *beatState) closedRun() []Outage {
	var run []Outage
	for i := range st.pendingMissing {
		rec := st.pendingMissing[i]
		if rec.recoveredAt.IsZero() {
			break
		}
		run = append(run, Outage{
			Started:   rec.seen,
			Recovered: rec.recoveredAt,
			Silence:   rec.silence,
		})
	}
	return run
}

// queueDetectedOutage counts a detected outage and queues its missing
// transition, reporting whether the queue had room. It is the shared prefix of
// the two record functions below, which own the rest: what a full queue MEANS.
// Dropping the newest record keeps the queued chronology and the in-flight
// retry of the head intact. Callers hold w.mu.
//
// The outage counter moves exactly once per detected outage, whether or not
// the notice found a queue slot: overflowAccounted already means "this same
// outage was detected and counted by an earlier call", which covers both a
// re-detection while the queue stays full and the later call that finally
// queues that outage.
func queueDetectedOutage(st *beatState, rec overdueBeat) bool {
	if !st.overflowAccounted {
		metrics.RecordOutage(rec.id)
	}
	if !st.pushMissing(rec) {
		return false
	}
	st.overflowAccounted = false
	return true
}

// recordEndedOutage queues the missing transition of an outage a ping has
// already ended, the record that is now that outage's only trace: a full queue
// loses it for good, so it counts a dropped notification and warns. The warning
// is ungated: it is reachable at most once per outage (only a ping brings a
// closed record, and the same ping re-arms the beat), so gating it would hide
// the loss in exactly the sequence that produces it. Contrast
// recordOngoingOutage. Callers hold w.mu.
func recordEndedOutage(st *beatState, rec overdueBeat) {
	if queueDetectedOutage(st, rec) {
		return
	}
	metrics.RecordNotificationDropped(metrics.KindMissing)
	slog.Warn("pending missing queue full, ended outage dropped, its notification will never be delivered",
		"beat", rec.id, "queued", missingQueueSize, "silence", rec.silence.String(),
		"since", rec.seen, "recovered", rec.recoveredAt)
}

// recordOngoingOutage queues the missing transition of an outage that is still
// in progress: a full queue costs nothing but a deferral, since the outage
// stays detected (openMissing stays nil) and the next sweep with a free slot
// records and delivers it. Nothing was dropped, so no notification counter
// moves, and the back-pressure is logged at DEBUG once per affected outage via
// overflowAccounted rather than once per tick. Contrast recordEndedOutage.
// Callers hold w.mu.
func recordOngoingOutage(st *beatState, rec overdueBeat) {
	if queueDetectedOutage(st, rec) {
		return
	}
	if st.overflowAccounted {
		// The same ongoing outage a previous sweep already reported: logging
		// it every tick would spam one affected outage as dozens.
		return
	}
	st.overflowAccounted = true
	slog.Debug("pending missing queue full, ongoing outage stays detected and is queued once a slot frees",
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

// New builds a Watcher for the given beats. start is the process-start
// baseline every beat's first deadline counts from (the caller captures it at
// process entry, before any startup work that could delay wiring); now is the
// ongoing clock, pass time.Now in production. The pending
// recovered-transition queue is sized from the beat count: each beat can
// hold at most one pending recovery, so that bound is exactly enough for a
// ping on every beat to queue without blocking.
func New(beats []Beat, notifier Notifier, now func() time.Time, start time.Time) *Watcher {
	w := &Watcher{
		notifier:   notifier,
		now:        now,
		beats:      make(map[string]*beatState, len(beats)),
		recoveries: make(chan recoveryEvent, len(beats)),
	}
	for _, b := range beats {
		w.beats[b.ID] = &beatState{lastSeen: start, deadline: b.Deadline}
		// InitBeat publishes the beat's boot-armed baseline (fresh from
		// start) and pre-mints its per-beat counters at zero, so increase()
		// has an earlier sample from a cold start.
		metrics.InitBeat(b.ID, start)
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
		recordEndedOutage(st, overdueBeat{
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
	metrics.RecordBeat(id, now)
	w.mu.Unlock()

	if wasAlerted {
		select {
		case w.recoveries <- recoveryEvent{id: id, downFor: downFor}:
		default:
			// Cannot happen while the queue bound matches the beat count
			// (one pending recovery per beat), but never block a ping.
			// The dropped recovery is no longer pending, so un-mark it or
			// the beat could never alert again. Nothing retries a dropped
			// recovery notice, so this is a permanent loss like a dropped
			// missing record, not a failed delivery attempt.
			w.mu.Lock()
			st.recovering = false
			w.mu.Unlock()
			metrics.RecordNotificationDropped(metrics.KindRecovered)
			slog.Warn("recovery queue full, dropping recovered notification",
				"beat", id, "down_for", downFor.String())
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
			w.logUndelivered()
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

// logUndelivered reports the notices this process will never deliver, on the
// way out. Both queues die with the process: a pendingMissing record whose
// outage already ended is that outage's only trace (the boot-armed clock
// re-detects an ONGOING outage after the restart only if it outlasts one full
// post-restart deadline, and never a closed one), and a queued recovered
// transition is gone with the channel. This is the one
// permanent-loss path no delivery counter can show — notifications_dropped_total
// means "discarded by a full queue" (metrics.go, README) — so the log line is
// the operator's only trace of it.
func (w *Watcher) logUndelivered() {
	type pending struct {
		id      string
		lost    int
		ongoing int
	}
	var beats []pending
	total, lostTotal := 0, 0
	w.mu.Lock()
	for id, st := range w.beats {
		// Only a CLOSED record is a permanent loss; the open tail (openMissing)
		// is an outage still in progress, which the godoc above explains.
		// overflowAccounted counts as one more ongoing record: a detected outage
		// the sweep could not queue lives in that flag alone, and a drain can
		// empty the slice while that outage is still in progress, so shutdown
		// must not report it as nothing at all.
		lost, ongoing := len(st.pendingMissing), 0
		if st.openMissing() != nil {
			lost--
			ongoing++
		}
		if st.overflowAccounted {
			ongoing++
		}
		if lost+ongoing == 0 {
			continue
		}
		beats = append(beats, pending{id: id, lost: lost, ongoing: ongoing})
		total += lost + ongoing
		lostTotal += lost
	}
	w.mu.Unlock()
	// Drain the queue so the warning can NAME the beats whose recovered
	// notice dies with the process: nothing consumes the channel after Run
	// returns, and a bare count leaves the operator unable to tell which
	// beat is still showing as down in Discord.
	var lostRecoveries []string
drain:
	for {
		select {
		case ev := <-w.recoveries:
			lostRecoveries = append(lostRecoveries, ev.id)
		default:
			break drain
		}
	}
	queuedRecoveries := len(lostRecoveries)
	slog.Info("watch loop stopped",
		"undelivered_records", total, "permanent_loss", lostTotal,
		"ongoing_records", total-lostTotal,
		"queued_recoveries", queuedRecoveries)
	for _, p := range beats {
		if p.lost == 0 {
			// Named rather than silent: a beat that returns inside the
			// post-restart grace window ends the outage with no notice ever
			// sent (README "Alerting", KnellRestartChurn).
			slog.Info("shutting down with an ongoing outage, its notice arrives only if the outage outlives the post-restart deadline",
				"beat", p.id, "still_ongoing", p.ongoing)
			continue
		}
		slog.Warn("shutting down with undelivered ended-outage records, no notice for them will ever arrive",
			"beat", p.id, "records", p.lost, "still_ongoing", p.ongoing)
	}
	if queuedRecoveries > 0 {
		slog.Warn("shutting down with queued recovered notifications, they will never be delivered",
			"queued", queuedRecoveries, "beats", lostRecoveries)
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
	fresh := !overdue(silence, deadline)
	metrics.SetBeatFresh(id, fresh)
	return fresh
}

// sweep checks every beat against its deadline and sends the one
// notification each beat owes now. A failed send is not marked alerted, so
// the next sweep retries it; the beat stays in one Discord message per live
// outage because alerted flips only on a delivered send.
// A delivered send for an outage that is already over (a ping raced the
// notice) emits the recovered transition immediately and leaves the beat
// armed for the next outage.
// One notification per beat per sweep, and history goes first so the oldest
// events lead: a beat whose queue head is an outage that already ended
// delivers every consecutive ended outage as ONE past-tense notice in this
// sweep, which is why a live outage queued behind a backlog waits a single
// tick rather than one tick per stale record. Run calls sweep on every tick;
// in-package tests call it directly.
func (w *Watcher) sweep(ctx context.Context) {
	live, history := w.collectDue()
	for _, past := range history {
		if w.sendHistory(ctx, past) {
			return
		}
	}
	for _, beat := range live {
		if w.sendMissing(ctx, beat) {
			return
		}
	}
}

// beatOutages is one beat's run of already-ended outages, the payload of a
// single collapsed history notice.
type beatOutages struct {
	id      string
	outages []Outage
}

// collectDue publishes every beat's freshness gauge and returns this sweep's
// due notifications, at most one per beat: either the beat's live missing
// notice (the head of its pending queue is an outage still in progress: a
// newly detected deadline crossing, or an earlier one whose send failed and
// is being retried) or its history notice (the head already ended, so every
// consecutive ended record at the head is collapsed into one notice).
// Detection is independent of delivery, so a crossing is recorded even while
// earlier notices are still queued; only beats mid-recovery are held back,
// keeping transitions chronological.
func (w *Watcher) collectDue() (live []overdueBeat, history []beatOutages) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	for id, st := range w.beats {
		silence := now.Sub(st.lastSeen)
		fresh := publishFreshness(id, silence, st.deadline)
		// An overdue beat whose current outage is not on the queue yet is
		// a fresh crossing to record. Recording it here rather than
		// skipping the beat is what keeps a second outage alive while an
		// earlier notice is still undelivered.
		if !fresh && !st.alerted && st.openMissing() == nil {
			recordOngoingOutage(st, overdueBeat{id: id, silence: silence, seen: st.lastSeen})
		}
		head := st.headMissing()
		if head == nil {
			continue
		}
		// Only the still-open current outage refreshes its silence, so a
		// retry reports how long the beat has been quiet. Once a ping seals
		// the record, its silence freezes at the reading taken when the
		// outage was detected — a history notice reports the outage's full
		// span (Outage.DownFor) instead, so the frozen reading is only
		// supplementary detail.
		// The lastSeen match is a defensive second guard, the twin of
		// markDelivered's: an open head is always the tail (openMissing), and
		// a ping seals the open tail in the same critical section that moves
		// lastSeen, so today it cannot disagree with recoveredAt. Keep it, so
		// a record whose start no longer matches lastSeen can never have a
		// later beat's silence written over its own reading.
		if head.recoveredAt.IsZero() && head.seen.Equal(st.lastSeen) {
			head.silence = silence
		}
		// Held while an earlier recovery is queued or in flight, so
		// transitions reach Discord in chronological order.
		if st.recovering {
			continue
		}
		// An ended head is history: collapse its whole run into one notice.
		// An open head is a live incident and keeps the present-tense path.
		if run := st.closedRun(); len(run) > 0 {
			history = append(history, beatOutages{id: id, outages: run})
			continue
		}
		live = append(live, *head)
	}
	return live, history
}

// sendMissing delivers one due missing transition and reports whether
// shutdown cancellation should stop the sweep.
func (w *Watcher) sendMissing(ctx context.Context, beat overdueBeat) bool {
	if err := w.notifier.BeatMissing(ctx, beat.id, beat.silence); err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("missing notification abandoned, shutting down", "beat", beat.id)
			return true
		}
		metrics.RecordNotificationFailed(metrics.KindMissing)
		slog.Error("missing notification failed, will retry next sweep",
			"beat", beat.id, "silence", beat.silence.String(), "error", err)
		return false
	}
	metrics.RecordNotificationSent(metrics.KindMissing)
	slog.Info("beat missing, notified", "beat", beat.id, "silence", beat.silence.String())
	if event, raced := w.markDelivered(beat.id, beat.seen); raced {
		w.sendRecovered(ctx, event)
	}
	return false
}

// sendHistory delivers one beat's collapsed history notice — every outage
// that had already ended when its turn came, in a single past-tense message —
// and reports whether shutdown cancellation should stop the sweep. The
// records stay queued until the notice is delivered, so a failure (or a
// shutdown) retries the whole run on the next sweep exactly like a live
// missing notice, and nothing is lost. The delivered notice states that the
// outages are over, so no recovered notification follows for them; the
// counters therefore move once for the message while metrics.RecordOutage
// already moved once per outage at detection.
func (w *Watcher) sendHistory(ctx context.Context, past beatOutages) bool {
	if err := w.notifier.BeatOutageHistory(ctx, past.id, past.outages); err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("outage history notification abandoned, shutting down", "beat", past.id)
			return true
		}
		metrics.RecordNotificationFailed(metrics.KindHistory)
		slog.Error("outage history notification failed, will retry next sweep",
			"beat", past.id, "outages", len(past.outages), "error", err)
		return false
	}
	metrics.RecordNotificationSent(metrics.KindHistory)
	slog.Info("ended outages notified as history",
		"beat", past.id, "outages", len(past.outages),
		"longest", LongestOutage(past.outages).String())
	w.dropDelivered(past.id, len(past.outages))
	return false
}

// dropDelivered removes the n head records a delivered history notice
// covered. Only the sender pops and it drops exactly what it collected from
// the head, so concurrent pings (which append at the tail, or seal the open
// tail) cannot shift the run out from under it.
func (w *Watcher) dropDelivered(id string, n int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.beats[id].dropMissing(n)
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
		metrics.RecordNotificationFailed(metrics.KindRecovered)
		slog.Error("recovered notification failed",
			"beat", ev.id, "down_for", ev.downFor.String(), "error", err)
		return
	}
	metrics.RecordNotificationSent(metrics.KindRecovered)
	slog.Info("beat recovered, notified", "beat", ev.id, "down_for", ev.downFor.String())
}
