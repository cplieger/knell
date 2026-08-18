// Package watch implements the dead-man state machine: it tracks when each
// configured beat last pinged, declares a beat missing once its deadline of
// silence passes, and notifies on the missing and recovered transitions.
//
// The deadline clock for every beat starts at the process-start baseline supplied
// to New, so a beat that never pings still alerts one deadline after process
// start even when startup work delays watcher construction. That closes the
// classic blind spot where a receiver restart silently disarms the switch.
//
// Notifications are split by whether the incident is still open: a live outage
// gets the present-tense notices, while outages already over are reported once as
// history, each carrying whether its own delivery was refused. Every log line reporting a
// delivery failure or an abandoned notice carries a retryable attribute -- the
// LEVEL is deliberately not that signal -- so a log rule can tell "wait for it"
// from "reconstruct the window".
package watch

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/cplieger/knell/internal/obs"
)

// --- Notification contract and the value types it carries ---

// Notifier delivers the transition notifications. Implementations are expected
// to retry transient failures internally and return the final outcome. Every
// non-cancellation returned error is logged VERBATIM by this package, so an
// implementation must return only log-safe errors: the webhook endpoint carries its
// credential in its own URL path. The first two methods report a LIVE incident; the
// third reports outages already over, so it must be rendered in the past tense.
type Notifier interface {
	// BeatMissing reports that id has been silent since live.Started and was
	// still silent when the sweep observed it at live.Observed, so the outage
	// is still open. live.DownFor() is the silence to report.
	BeatMissing(ctx context.Context, id string, live Transition) error
	// BeatRecovered reports that id pinged again at live.Observed after having
	// been declared missing, ending the silence that began at live.Started.
	BeatRecovered(ctx context.Context, id string, live Transition) error
	// BeatOutageHistory reports outages of id that were already over by the
	// time anything about them could be sent, collapsed into a single
	// past-tense notice. outages is chronological and never empty, and every
	// entry carries its own recovery point, start and delivery blame, which the
	// implementation must report rather than assume. Those guarantees are the
	// CALLER's, so an implementation must not re-check them: every error it
	// returns is read as a delivery failure.
	BeatOutageHistory(ctx context.Context, id string, outages []Outage) error
}

// Transition is the live incident a missing or a recovered notice reports: the
// beat's silence as the two instants the state machine measured it between,
// rather than a duration the call site computed. The notifier therefore derives
// the figure it renders exactly the way it derives an ended outage's. Both
// instants are always set, and WHICH observation Observed is comes from the
// method carrying it, never from a zero value.
type Transition struct {
	// Started is the last accepted ping before the outage: the instant the
	// beat's silence began, or the process-start baseline for a beat that has
	// never pinged. Outage.Started is the same instant.
	Started time.Time
	// Observed is the instant this notice speaks for: for a missing notice the
	// sweep that observed the beat still silent, for a recovered notice the
	// ping that ended the outage. At or after Started.
	Observed time.Time
}

// DownFor is how long the beat had been unseen at Observed, the figure a live
// notice reports. Outage.DownFor derives its own span through this method, so
// the live and history notices cannot report the same span two different ways.
func (t Transition) DownFor() time.Duration {
	return t.Observed.Sub(t.Started)
}

// Outage is one already-ended outage as the state machine observed it, the unit
// BeatOutageHistory reports. It is this package's own output type, so the
// notifier renders the outage without the state machine depending on how it is
// rendered. Transition is its live counterpart: same Started anchor, same
// DownFor measurement, plus the two facts only an ended outage has.
type Outage struct {
	// Started is the last accepted ping before the outage: the instant the
	// beat's silence began (Transition.Started is the same instant).
	Started time.Time
	// Recovered is the first accepted ping after the outage: the instant it
	// ended. Always after Started.
	Recovered time.Time
	// Undelivered reports that a delivery attempt for this outage was made and
	// REFUSED, so the notice points the operator at the webhook. False means
	// nothing was ever attempted for it — no sweep saw the outage, or a sweep
	// saw it and deferred it — and the notice must say so instead of sending
	// the reader hunting through a webhook that was working. Those are the only
	// two next steps a reader has, so they are the only two the record carries.
	// It is false at every producer and only blameDelivery ever sets it, which
	// is what keeps a notice from vouching for a webhook that just refused it.
	Undelivered bool
}

// DownFor is the outage's full span: from the last ping before it to the first
// ping after it. Derived through Transition.DownFor -- the one home of the
// measurement -- so every renderer measures it the same way.
func (o Outage) DownFor() time.Duration {
	return Transition{Started: o.Started, Observed: o.Recovered}.DownFor()
}

// LongestOutage returns the longest span among outages, or zero for an empty
// slice. It lives next to the type so the notifier's summary and the sender's
// log line report the same figure by construction.
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

// sweepSendBudget bounds how long ONE sweep keeps STARTING sends before it gives
// the Run loop its select back, leaving the beats it did not reach for the next
// sweep. sweep is the single sender and Run cannot service the recoveries channel
// while it sits inside a delivery, so without a bound the failure that makes
// every beat go missing at once becomes 64 posts in a tight loop. 5s is a third
// of DefaultTick, checked only BETWEEN beats. Nothing is lost by a cut: alerted
// flips only on a delivered send, so an unreached beat keeps its queued record.
const sweepSendBudget = 5 * time.Second

// Beat is one watched beat as the state machine needs it: an id and the silence
// deadline that declares it missing. It is this package's own input type, so the
// state machine does not depend on how the configuration was parsed.
type Beat struct {
	ID       string
	Deadline time.Duration
}

// MaxHistoryBatch is the largest number of ended outages one BeatOutageHistory
// notice can carry: the per-beat queue bound, and therefore the batch size a
// notifier's rendering budget must cover.
const MaxHistoryBatch = 8

// missingQueueSize bounds the per-beat queue of detected-but-undelivered missing
// transitions. Every queued entry costs a full deadline of silence plus a ping
// while the head is retried every tick, so a queue this deep only forms during a
// sustained webhook outage. It is deliberately small: the oldest notices are the
// actionable ones. Overflow drops the NEWEST record; recordEndedOutage and
// recordOngoingOutage own what a full queue MEANS for their case.
const missingQueueSize = MaxHistoryBatch

// --- Per-beat detection state: the pending-missing queue and its accounting ---

// overdueBeat is one detected missing transition: a beat past its deadline,
// captured as the silence interval it was measured over. While the outage remains
// open, collectBeatDue refreshes silence.Observed on each sweep so a retried live
// notice reports current silence. recoveredAt freezes the first ping that ends the
// outage (zero while ongoing), so the ended outage's span survives the wait.
// undelivered says whether that notice's delivery was attempted and refused.
type overdueBeat struct {
	silence     Transition
	recoveredAt time.Time
	id          string
	undelivered bool
}

// beatState is the per-beat tracking record.
type beatState struct {
	lastSeen time.Time
	// lastAttempt is when this beat last had a notification send ATTEMPTED for
	// it, zero while none ever has been. It is the sweep's ordering key:
	// stamping it when the attempt STARTS is what bounds how long a
	// continuously-due beat can wait.
	lastAttempt time.Time
	// pendingMissing is the FIFO of detected missing transitions whose
	// notification is not yet delivered, oldest first. It retains the evidence
	// of an outage so a ping cannot erase it by moving lastSeen, and detection
	// appends independently of delivery, so an outage that both begins and ends
	// while an earlier notice is queued keeps its own span.
	pendingMissing []overdueBeat
	deadline       time.Duration
	alerted        bool
	// recovering marks a recovered transition that is queued or in flight;
	// sweep must not send another missing transition until it is
	// delivered, so transitions reach Discord in chronological order.
	recovering bool
	// overflowAccounted records that the current outage has already been
	// counted as detected and has already reported the full queue, so a sweep
	// that re-detects the same still-unqueued outage every tick neither counts
	// it twice nor repeats its log line. A successful push clears it, and so
	// does the ping that ends the outage.
	overflowAccounted bool
}

// headMissing returns the oldest queued missing transition -- the one whose
// state decides the beat's next notice -- or nil when nothing is queued.
func (st *beatState) headMissing() *overdueBeat {
	if len(st.pendingMissing) == 0 {
		return nil
	}
	return &st.pendingMissing[0]
}

// openMissing returns the queued record of an outage that has not ended yet, or
// nil when every queued outage already carries its recovery point. Records are
// appended in outage order and only a ping closes one, so the open record can
// only ever be the tail.
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

// pushMissing appends a detected missing transition, reporting false when the
// queue is already at its bound (the caller accounts for the overflow). rec is
// taken by pointer only to avoid copying the record; it is COPIED into the
// queue, never aliased.
func (st *beatState) pushMissing(rec *overdueBeat) bool {
	if len(st.pendingMissing) >= missingQueueSize {
		return false
	}
	st.pendingMissing = append(st.pendingMissing, *rec)
	return true
}

// popMissing removes and returns the head, promoting the next queued
// transition. The queue must be non-empty: only the single sender pops, so an
// empty pop is a caller bug and panics rather than degrading -- a zero record
// carries no recovery point, so markDelivered would read it as an outage still
// open and mark the beat alerted with nothing queued, silently retiring the
// switch for that beat.
func (st *beatState) popMissing() overdueBeat {
	rec := st.pendingMissing[0]
	st.pendingMissing = st.pendingMissing[1:]
	return rec
}

// dropMissing removes the first n queued records: the run of already-ended
// outages a delivered history notice covered. Only the single sender pops, and
// it drops exactly the count it collected from the head, so a larger n is a
// caller bug and panics rather than silently discarding records.
func (st *beatState) dropMissing(n int) {
	st.pendingMissing = st.pendingMissing[n:]
}

// blameDelivery marks the first n queued records undelivered: the records whose
// own notice was ATTEMPTED and refused and which stay queued for the next sweep.
// Once that has happened they ARE late because of delivery -- a record still
// reporting that nothing was attempted would tell the operator "the webhook is
// not the place to look" in a message delivery had just refused. n can never
// exceed the queue length, so a larger n panics.
func (st *beatState) blameDelivery(n int) {
	for i := range n {
		st.pendingMissing[i].undelivered = true
	}
}

// closedRun returns the already-ended outages queued in an unbroken run at the
// head, oldest first, or nil when the head is still open. They are the beat's
// history: one collapsed past-tense notice covers all of them. The loop stops at
// the first open record, so the run cannot silently depend on only the tail being
// open. This is the site that SEALS a run into the shape BeatOutageHistory is
// handed, and records append in outage order, so the shape holds by construction.
func (st *beatState) closedRun() []Outage {
	var run []Outage
	for i := range st.pendingMissing {
		rec := st.pendingMissing[i]
		if rec.recoveredAt.IsZero() {
			break
		}
		run = append(run, Outage{
			Started:     rec.silence.Started,
			Recovered:   rec.recoveredAt,
			Undelivered: rec.undelivered,
		})
	}
	return run
}

// queueDetectedOutage counts a detected outage and queues its missing
// transition, reporting whether the queue had room. It is the shared prefix of
// the two record functions below, which own the rest: what a full queue MEANS.
// Dropping the newest record keeps the queued chronology intact. The outage
// counter moves exactly once per detected outage whether or not the notice found
// a slot. Callers hold w.mu.
func (st *beatState) queueDetectedOutage(rec *overdueBeat) bool {
	if !st.overflowAccounted {
		obs.RecordOutage(rec.id)
	}
	if !st.pushMissing(rec) {
		return false
	}
	st.overflowAccounted = false
	return true
}

// recordEndedOutage queues the missing transition of an outage a ping has
// already ended, the record that is now that outage's only trace: a full queue
// loses it for good, so it counts a dropped outage RECORD and warns. The record,
// not a message: nothing was ever attempted for it, and a history message can
// cover several records. It reports whether the record was dropped so the CALLER
// can announce it after releasing w.mu.
func (st *beatState) recordEndedOutage(rec *overdueBeat) bool {
	if st.queueDetectedOutage(rec) {
		return false
	}
	obs.RecordOutageRecordDropped(rec.id)
	return true
}

// logEndedOutageDropped announces a dropped ended-outage record. Beat emits it
// AFTER w.mu is released, like logOngoingOutageDeferred on the sweep path: slogx
// installs a synchronous stderr handler, so a stalled container log driver would
// otherwise block ping admission, the gauge refresh and the pre-drain
// StopAccepting for as long as the write blocks.
func logEndedOutageDropped(rec *overdueBeat) {
	// The two instants go in as time.Time values, not RFC3339 strings: slog
	// stores a time.Time as a typed Time attr, so the UTC pin holds, sub-second
	// precision survives, and a future JSON handler emits a real timestamp. The
	// kv form rather than slog.Time is the fleet's sloglint kv-only rule.
	slog.Warn("pending missing queue full, ended outage dropped, its notification will never be delivered",
		"beat", rec.id, "queued", missingQueueSize, "silence", rec.silence.DownFor().String(),
		"since", rec.silence.Started.UTC(),
		"recovered", rec.recoveredAt.UTC(),
		// Nothing was attempted and nothing survives to attempt: this loss is
		// permanent, unlike the retryable send failures that log at Error.
		"retryable", false)
}

// recordOngoingOutage queues the missing transition of an outage that is still
// in progress: a full queue costs nothing but a deferral, since the outage stays
// detected (openMissing stays nil) and the next sweep with a free slot records
// and delivers it. Nothing was dropped, so no counter moves, and the
// back-pressure is announced at DEBUG once per affected outage -- this reports
// whether that announcement is owed, and the CALLER emits it outside w.mu.
func (st *beatState) recordOngoingOutage(rec *overdueBeat) bool {
	if st.queueDetectedOutage(rec) {
		return false
	}
	if st.overflowAccounted {
		// The same ongoing outage a previous sweep already reported: logging
		// it every tick would spam one affected outage as dozens.
		return false
	}
	st.overflowAccounted = true
	return true
}

// logOngoingOutageDeferred announces the back-pressure of a full queue, emitted
// by observeBeat's caller outside w.mu for the reason logEndedOutageDropped
// gives. Debug, not Warn: nothing was lost.
func logOngoingOutageDeferred(rec *overdueBeat) {
	// A time.Time value, for the reason logEndedOutageDropped's log gives.
	slog.Debug("pending missing queue full, ongoing outage stays detected and is queued once a slot frees",
		"beat", rec.id, "queued", missingQueueSize, "silence", rec.silence.DownFor().String(),
		"since", rec.silence.Started.UTC())
}

// recoveryEvent is a queued recovered transition: the silence the arriving
// ping ended, measured at ping arrival and handed to BeatRecovered unchanged.
type recoveryEvent struct {
	silence Transition
	id      string
}

// --- The watcher: construction, ping admission, and the Run loop ---

// Watcher tracks beat freshness and drives transition notifications. Beat is
// safe for concurrent use; Run is the single background sender so notify
// calls never hold the lock.
type Watcher struct {
	notifier   Notifier
	now        func() time.Time
	beats      map[string]*beatState
	recoveries chan recoveryEvent
	mu         sync.Mutex
	// accepting is the authoritative beat-admission state, and the only one: it
	// is guarded by mu so it orders against the beat mutation itself, making
	// StopAccepting, Beat and the shutdown snapshot one serialized sequence.
	accepting bool
}

// New builds a Watcher for the given beats. start is the process-start baseline
// every beat's first deadline counts from (the caller captures it at process
// entry, before any startup work that could delay wiring); now is the ongoing
// clock. The pending recovered-transition queue is sized from the beat count:
// each beat can hold at most one pending recovery, so that bound is exactly
// enough for a ping on every beat to queue without blocking.
func New(beats []Beat, notifier Notifier, now func() time.Time, start time.Time) *Watcher {
	w := &Watcher{
		notifier:   notifier,
		now:        now,
		beats:      make(map[string]*beatState, len(beats)),
		recoveries: make(chan recoveryEvent, len(beats)),
		accepting:  true,
	}
	// The boot-armed clock's own claim, MEASURED rather than assumed: wiring can
	// reach New more than a deadline after the baseline, and a gauge that reports
	// fresh then contributes a false vote to the quorum sum.
	bootSilence := now().Sub(start)
	for _, b := range beats {
		w.beats[b.ID] = &beatState{lastSeen: start, deadline: b.Deadline}
		// InitBeat publishes the beat's boot-armed baseline and pre-mints its
		// per-beat counters at zero, so increase() has an earlier sample.
		obs.InitBeat(b.ID, b.Deadline, start)
		// A beat that has never pinged is fresh until its first deadline passes.
		// It goes through the same door as every later verdict, so overdue stays
		// its only source of the boundary.
		publishFreshness(b.ID, bootSilence, b.Deadline)
	}
	return w
}

// BeatOutcome is what recording a ping resulted in: the three states the beat
// endpoint answers for. A single value rather than a pair of booleans, so the
// combination the state machine never produces ("recorded, but admission is
// closed") cannot be constructed or mishandled by a caller.
type BeatOutcome uint8

const (
	// BeatClosed means shutdown has closed admission for the rest of the
	// process's life; nothing was recorded. It is deliberately the zero value:
	// an omitted outcome fails closed.
	BeatClosed BeatOutcome = iota
	// BeatUnknown means id is not a configured beat; nothing was recorded and no
	// metric series was minted for the id (it is a label).
	BeatUnknown
	// BeatRecorded means the ping was accepted and the beat's state was updated.
	BeatRecorded
)

// Beat records a ping for id and reports which of the three outcomes it was:
// BeatUnknown when id is not a configured beat, BeatClosed once shutdown has
// closed admission (nothing recorded in either case), BeatRecorded otherwise. A
// ping on an alerted beat queues the recovered notification for the Run loop, so
// this never blocks on the webhook. Admission is decided HERE, under the mutex
// that guards the state mutation, because that is the only way it can be atomic
// with respect to shutdown.
func (w *Watcher) Beat(id string) BeatOutcome {
	w.mu.Lock()
	if !w.accepting {
		w.mu.Unlock()
		return BeatClosed
	}
	st, ok := w.beats[id]
	if !ok {
		w.mu.Unlock()
		return BeatUnknown
	}
	now := w.now()
	previousSeen := st.lastSeen
	// The silence this ping ends, as the two instants that bound it. Every
	// figure below reads its span from here, so the deadline check, the queued
	// record and the recovered notice cannot measure it differently.
	silence := Transition{Started: previousSeen, Observed: now}
	wasAlerted := st.alerted
	// A late ping ends an outage: seal the record the sweep already queued, or
	// record the whole closed outage when no sweep has seen the crossing, so
	// this ping cannot erase it. Recording does not depend on the queue being
	// empty. Read overflowAccounted before the reset below clears it.
	var droppedEnded *overdueBeat
	if open := st.openMissing(); open != nil {
		open.recoveredAt = now
	} else if !wasAlerted && overdue(silence.DownFor(), st.deadline) {
		// undelivered stays false: no notice for this outage was ever
		// attempted, whether or not a sweep saw it before the queue filled.
		ended := overdueBeat{
			id:          id,
			silence:     silence,
			recoveredAt: now,
		}
		if st.recordEndedOutage(&ended) {
			droppedEnded = &ended
		}
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
	obs.RecordBeat(id, now)
	publishFreshness(id, 0, st.deadline)
	// Queue the recovered transition INSIDE the critical section that mutated the
	// beat, so a ping that got here is wholly visible to the shutdown tally. The
	// send is non-blocking (a blocking one would deadlock the watcher against its
	// only reader) and the slot is guaranteed: New sizes w.recoveries at one slot
	// per beat, and a beat holds at most one at a time.
	if wasAlerted {
		select {
		case w.recoveries <- recoveryEvent{id: id, silence: silence}:
		default:
			// Release the mutex before failing: net/http recovers a panic
			// raised in a handler, and a mutex left held would turn a loud
			// bug report into a silently wedged watcher.
			w.mu.Unlock()
			panic("watch: recovery queue full for beat " + id + ": capacity is one slot per configured beat and a beat holds at most one recovery, so this is unreachable unless that invariant was broken")
		}
	}
	w.mu.Unlock()

	if droppedEnded != nil {
		logEndedOutageDropped(droppedEnded)
	}
	return BeatRecorded
}

// StopAccepting closes beat admission for the rest of the process's life. It
// takes the mutex Beat mutates under, so once it returns no ping can still be
// between its admission check and its state change: that ordering is what makes
// logUndelivered's tally complete rather than merely narrow. It is exported
// because the shutdown SEQUENCE is the composition root's: main closes admission
// from webhttp's pre-drain hook, before the HTTP drain. Run calls it too, and a
// second call is a no-op because it assigns a state rather than transitioning one.
func (w *Watcher) StopAccepting() {
	w.mu.Lock()
	w.accepting = false
	w.mu.Unlock()
}

// Run drives the watch loop until ctx is cancelled: a sweep every tick plus
// immediate delivery of queued recovered transitions. It is the only
// goroutine that calls the notifier.
func (w *Watcher) Run(ctx context.Context, tick time.Duration) {
	// Observation runs on its own ticker: one overdue send can block the
	// sender loop for tens of seconds (3x10s attempts + backoff, or 30s
	// rate-limit waits), and neither the fresh gauge nor a deadline crossing
	// may wait on the very path whose failure is what parked the sender.
	go func() {
		observations := time.NewTicker(tick)
		defer observations.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-observations.C:
				w.observeBeats()
			}
		}
	}()
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Close admission before tallying: StopAccepting and the snapshot
			// serialize on the same mutex, so nothing can be recorded between
			// them. The composition root normally closed admission already, but
			// Run must not depend on someone else having gone first.
			w.StopAccepting()
			w.logUndelivered()
			return
		case ev := <-w.recoveries:
			w.sendRecovered(ctx, ev)
		case <-ticker.C:
			w.handleTick(ctx)
		}
	}
}

// handleTick is the ticker arm of Run, extracted so the drain-before-sweep
// ordering can be tested without racing Run's own select: a send may overrun
// the next tick while a recovery queues, so the consumed tick is preserved but
// the queued recovery goes first. Called only from Run, so it inherits the
// single-sender guarantee.
func (w *Watcher) handleTick(ctx context.Context) {
	select {
	case ev := <-w.recoveries:
		w.sendRecovered(ctx, ev)
	default:
	}
	if ctx.Err() == nil {
		w.sweep(ctx)
	}
}

// observeBeats observes every beat without sending anything, the observation
// ticker's whole work: the freshness gauge and a newly detected deadline
// crossing both stay current while the sender loop sits inside a slow or
// unreachable webhook, which is exactly when they carry the switch.
func (w *Watcher) observeBeats() {
	var deferredOverflow []overdueBeat
	w.mu.Lock()
	now := w.now()
	for id, st := range w.beats {
		if overflow := observeBeat(id, st, now); overflow != nil {
			deferredOverflow = append(deferredOverflow, *overflow)
		}
	}
	w.mu.Unlock()
	// Emitted outside the lock, for the reason collectDue's own loop gives.
	for i := range deferredOverflow {
		logOngoingOutageDeferred(&deferredOverflow[i])
	}
}

// --- Shutdown: tallying and reporting the notices that die with the process ---

// logUndelivered reports the notices this process will never deliver, on the way
// out. Both queues die with the process: a pendingMissing record whose outage
// already ended is that outage's only trace (the boot-armed clock re-detects an
// ONGOING outage after a restart, never a closed one), and a queued recovered
// transition is gone with the channel. No counter can show this loss, so the log
// line is the operator's only trace of it. The tally is complete by construction:
// admission is closed before the snapshot, both taking Beat's own mutex.
func (w *Watcher) logUndelivered() {
	beats, total, lostTotal := w.snapshotUndelivered()
	lostRecoveries := drainRecoveryIDs(w.recoveries)
	queuedRecoveries := len(lostRecoveries)
	slog.Info("watch loop stopped",
		"undelivered_records", total, "permanent_loss", lostTotal,
		"ongoing_records", total-lostTotal,
		"queued_recoveries", queuedRecoveries)
	for _, p := range beats {
		logPendingLoss(p)
	}
	if queuedRecoveries > 0 {
		slog.Warn("shutting down with queued recovered notifications, they will never be delivered",
			"queued", queuedRecoveries, "beats", lostRecoveries, "retryable", false)
	}
}

// pendingLoss is one beat's undelivered-record tally at shutdown: lost counts
// the CLOSED records whose notice will never arrive, ongoing the records whose
// outage is still in progress.
type pendingLoss struct {
	id      string
	lost    int
	ongoing int
}

// snapshotUndelivered tallies every beat's undelivered records under the mutex
// and returns them with the process-wide totals, so the logging below runs
// entirely outside the lock.
func (w *Watcher) snapshotUndelivered() (beats []pendingLoss, total, lostTotal int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for id, st := range w.beats {
		// Only a CLOSED record is a permanent loss; the open tail is an outage
		// still in progress. overflowAccounted counts as one more ongoing
		// record: a detected outage the sweep could not queue lives in that flag
		// alone, so shutdown must not report it as nothing at all.
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
		beats = append(beats, pendingLoss{id: id, lost: lost, ongoing: ongoing})
		total += lost + ongoing
		lostTotal += lost
	}
	return beats, total, lostTotal
}

// drainRecoveryIDs empties the recovery queue and names the beats it held, so
// the shutdown warning can NAME the beats whose recovered notice dies with the
// process: nothing consumes the channel after Run returns, and a bare count
// leaves the operator unable to tell which beat is still showing as down in
// Discord.
func drainRecoveryIDs(ch <-chan recoveryEvent) []string {
	var ids []string
	for {
		select {
		case ev := <-ch:
			ids = append(ids, ev.id)
		default:
			return ids
		}
	}
}

// logPendingLoss emits one beat's shutdown line, at the level its loss class
// deserves: an ongoing-only outage is informational, an ended-outage record is
// a permanent loss.
func logPendingLoss(p pendingLoss) {
	if p.lost == 0 {
		// Named rather than silent: a beat that returns inside the
		// post-restart grace window ends the outage with no notice ever
		// sent (README "Alerting", KnellRestartChurn).
		slog.Info("shutting down with an ongoing outage, its notice arrives only if the outage outlives the post-restart deadline",
			"beat", p.id, "still_ongoing", p.ongoing)
		return
	}
	slog.Warn("shutting down with undelivered ended-outage records, no notice for them will ever arrive",
		"beat", p.id, "records", p.lost, "still_ongoing", p.ongoing, "retryable", false)
}

// --- Detection and delivery: the freshness boundary, the sweep, and the senders ---

// overdue reports whether an observed silence has passed the beat's deadline.
// It is the single home of the freshness boundary: publishFreshness maps it to
// the quorum gauge and Beat uses it to decide whether a late ping closed an
// outage no sweep has seen, so the two paths cannot drift apart.
func overdue(silence, deadline time.Duration) bool {
	return silence > deadline
}

// publishFreshness publishes the freshness gauge for id given its observed
// silence and deadline, reporting whether the beat is still fresh. It is the
// ONLY writer of that gauge (New, Beat and observeBeat all publish through it),
// and it reads the boundary from overdue, so the quorum ground truth cannot
// drift between the paths that publish it. Callers hold w.mu, except New, which
// publishes before the Watcher is shared.
func publishFreshness(id string, silence, deadline time.Duration) bool {
	fresh := !overdue(silence, deadline)
	obs.SetBeatFresh(id, fresh)
	return fresh
}

// sweep checks every beat against its deadline and sends the one notification
// each beat owes now. A failed send is not marked alerted, so the next sweep
// retries it; the beat stays at one Discord message per live outage because
// alerted flips only on a delivered send. Sending is bounded by sweepSendBudget,
// and that an unreached beat is sent by the next sweep is a GUARANTEE the ORDER
// makes: the worklist is least-recently-attempted first and every send stamps the
// beat's attempt time before it starts.
func (w *Watcher) sweep(ctx context.Context) {
	due := w.collectDue()
	budget := w.now().Add(sweepSendBudget)
	for i := range due {
		// Every unsent entry is one beat, history and live alike (collectDue
		// yields at most one notice per beat), so the whole remainder of the
		// ordering is what a cut here defers.
		if w.budgetSpent(budget, len(due)-i) {
			return
		}
		// Before the send, not after: the beat has taken its turn either way,
		// and a send that is slow, fails, or is abandoned mid-flight must not
		// keep its place at the front of the next sweep's ordering.
		w.markAttempted(due[i].beatID(), w.now())
		if due[i].history != nil {
			if w.sendHistory(ctx, *due[i].history) {
				return
			}
			continue
		}
		if w.sendMissing(ctx, due[i].live) {
			return
		}
	}
}

// budgetSpent reports whether this sweep has spent its send budget and must stop
// before STARTING the next beat's send, logging the cut once with the number of
// beats deferred. A deferred beat moves NO counter, keeps alerted where it was,
// keeps its queued record, and keeps its claim about delivery, because a cut is
// not an attempt: 5s across the 64-beat cap is 78ms per notification, so a storm
// can spend the whole budget against a healthy webhook. DEBUG, not a fault.
func (w *Watcher) budgetSpent(budget time.Time, deferred int) bool {
	if !w.now().After(budget) {
		return false
	}
	slog.Debug("sweep send budget spent, remaining beats deferred to the next sweep",
		"budget", sweepSendBudget.String(), "deferred_beats", deferred)
	return true
}

// beatOutages is one beat's run of already-ended outages, the payload of a
// single collapsed history notice.
type beatOutages struct {
	id      string
	outages []Outage
}

// dueNotice is the ONE notice a beat owes this sweep, carried with the keys
// sweep orders the work by. Exactly one of history and live is set: the beat's
// own queue head decides which (collectBeatDue), exactly as it always has.
// What dueNotice adds is only that both shapes now share ONE ordering, so no
// class of notice can starve the other.
type dueNotice struct {
	// started is when the outage this notice reports began: the first outage
	// of a history run, or the live record's own lastSeen anchor. It is the
	// tie-break among beats with the same attempt history, so the oldest
	// outage leads.
	started time.Time
	// lastAttempt is the beat's attempt stamp when this sweep collected it
	// (see beatState.lastAttempt), the primary ordering key.
	lastAttempt time.Time
	// history is the collapsed run of already-ended outages when the beat's
	// queue head is closed, nil for a live notice.
	history *beatOutages
	// live is the still-open record when the beat's queue head is open, nil
	// for a history notice. It points at the copy collectBeatDue took under
	// the lock, never at a queued record.
	live *overdueBeat
}

// beatID is the beat this notice belongs to, read from whichever payload the
// beat's queue head produced. Not stored a third time: the id already lives on
// both payload types, and the notice is small enough to pass by value to the
// ordering function only while it stays that way (gocritic hugeParam).
func (n *dueNotice) beatID() string {
	if n.history != nil {
		return n.history.id
	}
	return n.live.id
}

// byAttemptThenAge orders one sweep's due notices, and it is the whole of the
// sweep's fairness mechanism: least-recently-attempted first, so a beat that has
// taken a turn falls behind every beat still waiting for one; then oldest outage,
// so the longest-running incident is served first; then a past-tense notice ahead
// of a live one that began at the very same instant, which only decides beats
// tying on both keys above and is a tie-break, NOT a global priority.
func byAttemptThenAge(a, b dueNotice) int {
	if c := a.lastAttempt.Compare(b.lastAttempt); c != 0 {
		return c
	}
	if c := a.started.Compare(b.started); c != 0 {
		return c
	}
	switch {
	case a.history != nil && b.history == nil:
		return -1
	case a.history == nil && b.history != nil:
		return 1
	default:
		return 0
	}
}

// markAttempted stamps the beat's attempt time, which is what keeps the sweep's
// ordering fair across ticks: collectDue reads the stamp back on every later
// sweep, so a beat whose send was slow, failed, or was abandoned moves behind the
// beats that have not had a turn. No separate cursor is stored anywhere: the
// priority function IS the mechanism.
func (w *Watcher) markAttempted(id string, at time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.beats[id].lastAttempt = at
}

// collectDue publishes every beat's freshness gauge and returns this sweep's due
// notifications in the order they must be attempted, at most one per beat: the
// live missing notice when the queue head is an outage still in progress, or the
// history notice when the head already ended. Only beats mid-recovery are held
// back. The RETURNED ORDER is load-bearing: w.beats is a map, so byAttemptThenAge
// turns an arbitrary order into a total one keyed on each beat's attempt stamp.
func (w *Watcher) collectDue() []dueNotice {
	var due []dueNotice
	var deferredOverflow []overdueBeat
	w.mu.Lock()
	now := w.now()
	for id, st := range w.beats {
		live, run, overflow := collectBeatDue(id, st, now)
		if overflow != nil {
			deferredOverflow = append(deferredOverflow, *overflow)
		}
		switch {
		case len(run) > 0:
			due = append(due, dueNotice{
				started:     run[0].Started,
				lastAttempt: st.lastAttempt,
				history:     &beatOutages{id: id, outages: run},
			})
		case live != nil:
			due = append(due, dueNotice{
				started:     live.silence.Started,
				lastAttempt: st.lastAttempt,
				live:        live,
			})
		}
	}
	w.mu.Unlock()
	// Ordered outside the lock: every key was copied out above, so the sort
	// touches no shared state and must not hold the mutex Beat,
	// observeBeats and StopAccepting all take.
	slices.SortFunc(due, byAttemptThenAge)
	// Emitted outside the lock: a synchronous stderr write must not hold the
	// mutex Beat, observeBeats and StopAccepting all take.
	for i := range deferredOverflow {
		logOngoingOutageDeferred(&deferredOverflow[i])
	}
	return due
}

// observeBeat is one beat's observation, run by BOTH the observation ticker and
// every sweep: it publishes the freshness gauge and records a deadline crossing
// the queue has not seen yet, returning a record whose overflow the CALLER must
// log outside w.mu. Idempotent by that record's own guard, so the two callers
// cannot count one outage twice. Callers hold w.mu.
func observeBeat(id string, st *beatState, now time.Time) *overdueBeat {
	// This observation's reading of the beat's silence, as the two instants
	// that bound it: the last accepted ping and now. The freshness gauge, a
	// queued record and the live notice all read their span from it.
	silence := Transition{Started: st.lastSeen, Observed: now}
	fresh := publishFreshness(id, silence.DownFor(), st.deadline)
	// An overdue beat whose current outage is not on the queue yet is a fresh
	// crossing to record, which is what keeps a second outage alive while an
	// earlier notice is undelivered. The record is OPEN, so undelivered stays
	// false: nothing has been attempted for it yet.
	if !fresh && !st.alerted && st.openMissing() == nil {
		pending := overdueBeat{id: id, silence: silence}
		if st.recordOngoingOutage(&pending) {
			return &pending
		}
	}
	return nil
}

// collectBeatDue is collectDue's per-beat state transition: it observes the beat
// through observeBeat, refreshes an open record's reading, and reports what this
// sweep owes the beat -- a live missing notice, a run of ended outages, or
// neither. The caller holds w.mu; the returned record is a COPY, so no pointer
// into pendingMissing escapes. The third result is a record whose overflow the
// CALLER must log outside w.mu.
func collectBeatDue(id string, st *beatState, now time.Time) (live *overdueBeat, history []Outage, deferredOverflow *overdueBeat) {
	deferredOverflow = observeBeat(id, st, now)
	head := st.headMissing()
	if head == nil {
		return nil, nil, deferredOverflow
	}
	// Only the still-open current outage refreshes its silence, so a retry
	// reports how long the beat has been quiet: its observation instant moves
	// forward while its start never does. An unsealed head IS the still-open
	// outage: Beat seals the tail in the section that advances lastSeen.
	if head.recoveredAt.IsZero() {
		head.silence.Observed = now
	}
	// Held while an earlier recovery is queued or in flight, so transitions reach
	// Discord in chronological order. No rewrite is owed: a record this hold
	// defers has had nothing attempted for it either way.
	if st.recovering {
		return nil, nil, deferredOverflow
	}
	// An ended head is history: collapse its whole run into one notice.
	// An open head is a live incident and keeps the present-tense path.
	if run := st.closedRun(); len(run) > 0 {
		return nil, run, deferredOverflow
	}
	// Copy the record before it leaves the critical section, so the sweep
	// never holds a pointer into pendingMissing.
	due := *head
	return &due, nil, deferredOverflow
}

// sendMissing delivers one due missing transition and reports whether shutdown
// cancellation should stop the sweep. beat points at the record collectBeatDue
// COPIED out under the lock, so reading it here without the lock is safe. A real
// failure also blames delivery for the record it left queued: the alert was
// attempted and refused, so if a ping ends the outage before the retry lands, the
// past-tense notice must point at the webhook. Cancellation is exempt: a shutdown
// is not a delivery failure.
func (w *Watcher) sendMissing(ctx context.Context, beat *overdueBeat) bool {
	if err := w.notifier.BeatMissing(ctx, beat.id, beat.silence); err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("missing notification abandoned, shutting down",
				"beat", beat.id, "retryable", false)
			return true
		}
		obs.RecordNotificationFailed(obs.KindMissing)
		w.markUndelivered(beat.id, 1)
		slog.Error("missing notification failed, will retry next sweep",
			"beat", beat.id, "silence", beat.silence.DownFor().String(), "error", err,
			"retryable", true)
		return false
	}
	obs.RecordNotificationSent(obs.KindMissing)
	slog.Info("beat missing, notified", "beat", beat.id, "silence", beat.silence.DownFor().String())
	if event, raced := w.markDelivered(beat.id); raced {
		w.sendRecovered(ctx, event)
	}
	return false
}

// sendHistory delivers one beat's collapsed history notice -- every outage that
// had already ended when its turn came, in a single past-tense message -- and
// reports whether shutdown cancellation should stop the sweep. The records stay
// queued until the notice is delivered, so a failure retries the whole run next
// sweep, and the notice states the outages are over, so no recovered notification
// follows. A real failure also marks the queued records it covered undelivered.
func (w *Watcher) sendHistory(ctx context.Context, past beatOutages) bool {
	if err := w.notifier.BeatOutageHistory(ctx, past.id, past.outages); err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("outage history notification abandoned, shutting down",
				"beat", past.id, "retryable", false)
			return true
		}
		obs.RecordNotificationFailed(obs.KindHistory)
		w.markUndelivered(past.id, len(past.outages))
		slog.Error("outage history notification failed, will retry next sweep",
			"beat", past.id, "outages", len(past.outages), "error", err,
			"retryable", true)
		return false
	}
	obs.RecordNotificationSent(obs.KindHistory)
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

// markUndelivered blames delivery for the n head records whose own notice was
// just attempted and refused, so the retry reports the true reason it is late. n
// is exactly the records that notice covered: 1 for a live missing notice and
// len(outages) for a history notice. A record queued after the failed send stays
// unblamed: that notice never tried to deliver it. Without this, an outage whose
// alert the webhook refused and which a ping then ended would report that
// nothing was attempted, vouching for the webhook that had just refused it.
func (w *Watcher) markUndelivered(id string, n int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.beats[id].blameDelivery(n)
}

// markDelivered records the outcome of a delivered missing send for id. It pops
// the delivered transition, promoting any later queued outage to the head, and
// normally marks the beat alerted. When the outage is already over -- the popped
// record carries the recovery point a ping sealed into it, including a ping that
// raced this very send -- marking alerted would swallow the NEXT outage's missing
// notice, so the beat stays re-armed and the recovered transition is returned.
func (w *Watcher) markDelivered(id string) (recoveryEvent, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.beats[id]
	delivered := st.popMissing()
	if delivered.recoveredAt.IsZero() {
		st.alerted = true
		return recoveryEvent{}, false
	}
	st.recovering = true
	// The sealed record carries the authoritative recovery point: the FIRST
	// ping after the outage, written by Beat in the same critical section that
	// advanced lastSeen. The recovered notice reports the same silence the
	// missing notice just reported, ending at that point instead of at the sweep.
	return recoveryEvent{id: id, silence: Transition{Started: delivered.silence.Started, Observed: delivered.recoveredAt}}, true
}

// finishRecovery clears the pending-recovery mark for id, re-enabling sweep
// to start the beat's next missing transition.
func (w *Watcher) finishRecovery(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.beats[id].recovering = false
}

// sendRecovered delivers one queued recovered transition. Best-effort by design:
// the critical direction of a dead-man switch is missing, which has sweep-level
// retry. Recovered is FIRE-ONCE, which makes a failed send terminal here: the
// event is dequeued before the send and finishRecovery runs unconditionally, so a
// non-cancellation failure counts as DROPPED rather than failed. Cancellation is
// in no counter and no tally either, so the Info line below is its ONLY trace.
func (w *Watcher) sendRecovered(ctx context.Context, ev recoveryEvent) {
	defer w.finishRecovery(ev.id)
	if err := w.notifier.BeatRecovered(ctx, ev.id, ev.silence); err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("recovered notification abandoned, shutting down, nothing retries it so no notice for this recovery will ever arrive",
				"beat", ev.id, "down_for", ev.silence.DownFor().String(), "retryable", false)
			return
		}
		obs.RecordNotificationDropped(obs.KindRecovered)
		slog.Error("recovered notification failed, nothing retries it and no notice for this recovery will ever arrive",
			"beat", ev.id, "down_for", ev.silence.DownFor().String(), "error", err,
			"retryable", false)
		return
	}
	obs.RecordNotificationSent(obs.KindRecovered)
	slog.Info("beat recovered, notified", "beat", ev.id, "down_for", ev.silence.DownFor().String())
}
