// Package watch implements the dead-man state machine: it tracks when each
// configured beat last pinged, declares a beat missing once its deadline of
// silence passes, and notifies on the missing and recovered transitions.
//
// The deadline clock for every beat starts at the process-start baseline
// supplied to New, so a beat that never pings still alerts one deadline after
// process start even when startup work delays watcher construction.
// That deliberately closes the classic dead-man blind spot where a receiver
// restart silently disarms the switch until the first ping re-arms it.
//
// Notifications are split by whether the incident is still open. A live
// outage gets the present-tense missing notice and, when the beat returns,
// its recovered notice. Outages that were already over by the time anything
// about them could be sent are reported once as history, so a resolved
// incident is never announced as a new live failure. A notice is late for one
// of two reasons — a webhook outage held its alert back, or the outage ended
// between two sweeps and no alert was ever due — and each record carries
// which one (LateReason), because the operator's next step differs.
//
// Every log line reporting a delivery failure or a notice abandoned for good
// carries a retryable attribute: true when the record survives and the next
// sweep sends it again (the missing and history send failures), false when
// nothing is left to attempt and no notice for that transition will ever
// arrive (a discarded record, a dropped or failed recovered notice, the
// notices abandoned at shutdown). The LEVEL is deliberately not that signal — a
// retried send stays at Error rather than spamming Warn every sweep — so this
// attribute is what a log rule keys on to tell "wait for it" from
// "reconstruct the window".
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
// Every method's returned error is logged VERBATIM by this package (sendMissing,
// sendHistory and sendRecovered log it as the "error" attribute), so an
// implementation must return only log-safe errors: the webhook endpoint knell
// posts to carries its credential in its own URL path, and an error that wraps
// that URL would publish the credential into the log stream. internal/notify
// reduces every transport error through httpx.LogSafeError for exactly this
// reason; a second implementation has to do the same.
//
// The first two methods report a LIVE incident as it happens; the third
// reports outages that were already over by the time their notice could be
// sent, so an implementation must render it in the past tense. Nothing
// else distinguishes the two: a live outage never reaches
// BeatOutageHistory, and a history notice is never followed by
// BeatRecovered calls for the outages it covers.
//
// Every method carries one of this package's own value types — a Transition
// for the two live notices, Outage records for history — and each of those
// derives its own span through DownFor. So the tense lives in the method NAME
// while "how long was the beat down" has a single home, and an implementation
// cannot measure the live case one way and the history case another.
type Notifier interface {
	// BeatMissing reports that id has been silent since live.Started and was
	// still silent when the sweep observed it at live.Observed (its deadline
	// has passed), so the outage is still open. live.DownFor() is the silence
	// to report.
	BeatMissing(ctx context.Context, id string, live Transition) error
	// BeatRecovered reports that id pinged again at live.Observed after having
	// been declared missing, ending the silence that began at live.Started
	// (its last accepted ping). live.DownFor() is how long it was down.
	BeatRecovered(ctx context.Context, id string, live Transition) error
	// BeatOutageHistory reports outages of id that were already over by the
	// time anything about them could be sent, collapsed into a single
	// past-tense notice. outages is chronological and never empty, and every
	// entry carries its own recovery point, so the implementation must not
	// present them as ongoing. Each entry also carries its own LateReason,
	// which the implementation must report rather than assume: one batch can
	// hold both.
	BeatOutageHistory(ctx context.Context, id string, outages []Outage) error
}

// Transition is the live incident a missing or a recovered notice reports:
// the beat's silence as the two instants the state machine measured it
// between, rather than a duration the call site computed. The notifier
// therefore derives the figure it renders exactly the way it derives an ended
// outage's (Outage.DownFor), and a new rendering fact about a live notice is
// a field here instead of a new interface parameter.
//
// Both instants are always set: a Transition never carries an empty field, and
// WHICH observation Observed is comes from the method carrying it, never from a
// zero value. Nothing here says whether the outage is over — BeatMissing and
// BeatRecovered do, by being different methods.
type Transition struct {
	// Started is the last accepted ping before the outage: the instant the
	// beat's silence began, or the process-start baseline for a beat that has
	// never pinged. Outage.Started is the same instant, for an outage that has
	// already ended.
	Started time.Time
	// Observed is the instant this notice speaks for: for a missing notice the
	// sweep that observed the beat still silent, for a recovered notice the
	// ping that ended the outage. At or after Started.
	Observed time.Time
}

// DownFor is how long the beat had been unseen at Observed, the figure a live
// notice reports ("silent for ...", "after ... of silence"). It is the same
// measurement Outage.DownFor makes for an outage that has ended — last
// accepted ping to the instant being reported — and Outage.DownFor derives it
// through this method, so the live and history notices cannot report the same
// span two different ways.
func (t Transition) DownFor() time.Duration {
	return t.Observed.Sub(t.Started)
}

// LateReason names WHY an outage was already over by the time anything about
// it could be sent — why its notice is past tense rather than a live alarm.
// Both values route identically (see collectDue) and both render in the past
// tense; they differ in what the operator should DO, which is the one thing a
// past-tense notice cannot state correctly without being told.
type LateReason uint8

const (
	// LateUndelivered means the notice is late because delivery was behind.
	// Either a sweep detected the outage while it was still open, so a live
	// missing notice WAS due for it and had not been delivered when the ping
	// ended the outage (the send failed, the beat was held behind an earlier
	// recovery, or sweepSendBudget deferred it to a later sweep), or the
	// past-tense notice reporting it failed to send and is being retried
	// (sendHistory upgrades the records it left queued). Every one of those is
	// a delivery that did not complete in time, so the notice points the
	// operator at the webhook.
	//
	// It is the zero value deliberately: a future producer that queues a
	// record without naming a reason then points at delivery instead of
	// vouching for it, and a delivery path that is quietly behind is the one
	// thing a dead-man switch must never claim is healthy.
	LateUndelivered LateReason = iota
	// LateEndedBeforeDetection means no sweep ever saw this outage. It crossed
	// its deadline and was ended by a ping inside the gap between two sweeps
	// (DefaultTick), so no live notice was ever due and delivery was never
	// involved. Reporting it as a delivery problem would send an operator
	// hunting through a webhook that was working.
	//
	// It is the one reason a record can LOSE: the statement holds only while
	// nothing about the outage has failed to send, so the first failed history
	// send upgrades it to LateUndelivered (sendHistory). A reason only ever
	// moves in that direction — toward blaming delivery — because a notice
	// that vouches for delivery must be able to defend the claim.
	LateEndedBeforeDetection
)

// Outage is one already-ended outage as the state machine observed it, the
// unit BeatOutageHistory reports. It is the watch package's own output type,
// so the notifier renders the outage from the state machine's shape without
// the state machine depending on how (or where) it is rendered. Transition is
// its live counterpart: same Started anchor, same DownFor measurement, plus
// the two facts only an ended outage has (its recovery point and LateReason).
type Outage struct {
	// Started is the last accepted ping before the outage: the instant the
	// beat's silence began (Transition.Started is the same instant).
	Started time.Time
	// Recovered is the first accepted ping after the outage: the instant it
	// ended. Always after Started.
	Recovered time.Time
	// LateReason says why this outage is reported after the fact instead of
	// as a live incident. The renderer must not guess it: the two reasons
	// lead an operator to opposite next steps.
	LateReason LateReason
}

// DownFor is the outage's full span: from the last ping before it to the
// first ping after it. It is the figure a past-tense notice reports ("was
// missing for ..."), derived through Transition.DownFor — the one home of the
// measurement — so every renderer, live or history, measures it the same way.
func (o Outage) DownFor() time.Duration {
	return Transition{Started: o.Started, Observed: o.Recovered}.DownFor()
}

// Ended reports whether the outage carries a usable recovery point: one that
// is set and no earlier than Started. It is the executable form of the
// invariant the Recovered field documents, and it lives here, next to the
// type, so a renderer that refuses an unfinished record and the state machine
// that builds them agree by construction rather than by comment.
func (o Outage) Ended() bool {
	return !o.Recovered.IsZero() && !o.Recovered.Before(o.Started)
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

// sweepSendBudget bounds how long ONE sweep keeps STARTING sends before it
// gives the Run loop its select back, leaving the beats it did not reach for
// the next sweep. sweep is the single sender and Run cannot service the
// recoveries channel while the sweep sits inside a delivery, so without a
// bound the failure that makes every beat go missing at once — whatever fans
// the heartbeats out dies, so every configured beat crosses its deadline in
// the SAME sweep (internal/config caps a fleet at 64) — becomes 64 posts to
// one webhook in a tight loop, each notification worth up to 3x(10s attempt +
// 30s rate-limit wait), with every recovery notice stuck behind the storm.
//
// 5s is a third of the 15s DefaultTick: it leaves two thirds of every interval
// for queued recoveries and keeps the next sweep near its schedule, while
// still being several sends wide so a storm drains over a handful of ticks
// rather than one beat per tick. It is a floor on responsiveness, not a cap on
// sweep duration: the budget is checked only BETWEEN beats, so the send in
// flight when it expires runs to completion and one sweep can overrun it.
// Interrupting a send is deliberately not attempted — the goal is to stop
// ADDING work, not to abandon a notice already being delivered.
//
// Nothing is lost by cutting a sweep short. alerted flips only on a delivered
// send, so a beat the sweep never reached keeps its queued record and the next
// sweep delivers it; the retry that a failed send already relies on carries
// the deferral too. In the healthy case the budget never bites — 64 quick
// webhook posts take well under a second — so normal delivery is unchanged.
const sweepSendBudget = 5 * time.Second

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
// captured as the silence interval it was measured over. silence.Started is
// the fixed lastSeen anchor the outage began at; while the outage remains
// open, collectBeatDue refreshes silence.Observed on each sweep so a retried
// live notice reports current silence rather than the reading of the sweep
// that first detected the crossing. It stays queued on the beat until its
// notification is delivered, and sendMissing hands the record's own interval
// to BeatMissing, so the live notice reports the interval the state machine
// recorded instead of a span re-derived at the send.
// recoveredAt freezes the first ping that ends the outage (zero while it is
// still ongoing), after which history derives the full span from Started to
// recoveredAt: the ended outage's span survives the wait and its notice can
// report it in the past tense instead of as a live failure. late says WHY
// that past-tense notice is late, which only the code that records the
// crossing knows (see LateReason).
type overdueBeat struct {
	silence     Transition
	recoveredAt time.Time
	id          string
	late        LateReason
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
// rec is taken by pointer only to avoid copying the record (gocritic
// hugeParam); the record is COPIED into the queue, never aliased.
func (st *beatState) pushMissing(rec *overdueBeat) bool {
	if len(st.pendingMissing) >= missingQueueSize {
		return false
	}
	st.pendingMissing = append(st.pendingMissing, *rec)
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

// blameDelivery sets the late reason of the first n queued records to
// LateUndelivered: the run whose own past-tense notice just failed to send and
// stays queued for the next sweep. Once that has happened the records ARE late
// because of delivery, whatever held them back before, so the retried notice
// can say so — a record still claiming LateEndedBeforeDetection would tell the
// operator "nothing was wrong with delivery" in a message delivery had just
// refused once.
//
// The write only ever goes toward blaming delivery, never back: that is the
// direction the zero value already points (see LateUndelivered), so the reason
// can only get safer, and a record whose reason is already LateUndelivered is
// unchanged. The same clamp as dropMissing keeps a future caller in range.
func (st *beatState) blameDelivery(n int) {
	for i := range min(n, len(st.pendingMissing)) {
		st.pendingMissing[i].late = LateUndelivered
	}
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
			Started:    rec.silence.Started,
			Recovered:  rec.recoveredAt,
			LateReason: rec.late,
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
//
// rec travels by pointer through this trio only to avoid copying the record at
// every hop (gocritic hugeParam); nothing here retains the pointer.
func (st *beatState) queueDetectedOutage(rec *overdueBeat) bool {
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
// loses it for good, so it counts a dropped outage RECORD and warns. The record,
// not a message: nothing was ever built or attempted for it, and a history
// message can cover several records, so the per-record counter is the honest
// unit (metrics.RecordOutageRecordDropped). The warning is ungated: it is
// reachable at most once per outage (only a ping brings a
// closed record, and the same ping re-arms the beat), so gating it would hide
// the loss in exactly the sequence that produces it. Contrast
// recordOngoingOutage. Callers hold w.mu.
func (st *beatState) recordEndedOutage(rec *overdueBeat) {
	if st.queueDetectedOutage(rec) {
		return
	}
	metrics.RecordOutageRecordDropped(rec.id)
	// The two instants go in as time.Time values, not RFC3339 strings: slog
	// stores a time.Time as a typed Time attr (slog.AnyValue special-cases it),
	// so the UTC pin holds, sub-second precision survives, and a future JSON
	// handler emits a real timestamp instead of a pre-rendered string. The kv
	// form rather than slog.Time is the fleet's sloglint kv-only rule.
	slog.Warn("pending missing queue full, ended outage dropped, its notification will never be delivered",
		"beat", rec.id, "queued", missingQueueSize, "silence", rec.silence.DownFor().String(),
		"since", rec.silence.Started.UTC(),
		"recovered", rec.recoveredAt.UTC(),
		// Nothing was attempted and nothing survives to attempt: this loss is
		// permanent, unlike the retryable send failures that log at Error.
		"retryable", false)
}

// recordOngoingOutage queues the missing transition of an outage that is still
// in progress: a full queue costs nothing but a deferral, since the outage
// stays detected (openMissing stays nil) and the next sweep with a free slot
// records and delivers it. Nothing was dropped, so no delivery counter and no
// dropped-record counter moves, and the back-pressure is logged at DEBUG once
// per affected outage via
// overflowAccounted rather than once per tick. Contrast recordEndedOutage.
// Callers hold w.mu.
func (st *beatState) recordOngoingOutage(rec *overdueBeat) {
	if st.queueDetectedOutage(rec) {
		return
	}
	if st.overflowAccounted {
		// The same ongoing outage a previous sweep already reported: logging
		// it every tick would spam one affected outage as dozens.
		return
	}
	st.overflowAccounted = true
	// A time.Time value, for the reason recordEndedOutage's log gives.
	slog.Debug("pending missing queue full, ongoing outage stays detected and is queued once a slot frees",
		"beat", rec.id, "queued", missingQueueSize, "silence", rec.silence.DownFor().String(),
		"since", rec.silence.Started.UTC())
}

// lateReasonForUnqueuedOutage names why the notice for an outage that has NO
// queued record of its own will be late, for the ping that is about to record
// the whole closed outage. Reaching that arm means no sweep left an open
// record for this outage, which is normally because no sweep ran while it was
// open: it crossed its deadline and ended between two ticks, so no live notice
// was ever due and delivery was never involved (LateEndedBeforeDetection).
//
// The exception is overflowAccounted: a sweep DID detect this same outage and
// could not queue it because the queue was full, which only happens while a
// sustained delivery failure keeps eight records undelivered. That outage's
// notice is late because delivery was not keeping up, so it reports
// LateUndelivered — calling it undetected would vouch for a webhook that is
// demonstrably behind. Callers hold w.mu.
func (st *beatState) lateReasonForUnqueuedOutage() LateReason {
	if st.overflowAccounted {
		return LateUndelivered
	}
	return LateEndedBeforeDetection
}

// recoveryEvent is a queued recovered transition: the silence the arriving
// ping ended, measured at ping arrival and handed to BeatRecovered unchanged.
type recoveryEvent struct {
	silence Transition
	id      string
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
	// accepting is the authoritative beat-admission state, guarded by mu so
	// it orders against the beat mutation itself: stopAccepting, Beat and the
	// shutdown snapshot are one serialized sequence, which is what a context
	// check in the HTTP handler cannot be (see Beat and stopAccepting).
	accepting bool
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
		accepting:  true,
	}
	for _, b := range beats {
		w.beats[b.ID] = &beatState{lastSeen: start, deadline: b.Deadline}
		// InitBeat publishes the beat's boot-armed baseline (start as
		// last-seen) and pre-mints its per-beat counters at zero, so
		// increase() has an earlier sample from a cold start.
		metrics.InitBeat(b.ID, b.Deadline, start)
		// The boot-armed clock's own claim: a beat that has never pinged is
		// fresh until its first deadline passes. It goes through the same
		// door as every later verdict, so overdue stays its only source.
		publishFreshness(b.ID, 0, b.Deadline)
	}
	return w
}

// Beat records a ping for id. recorded is false when id is not a configured
// beat (the caller answers 404 and nothing is recorded); accepting is false
// once shutdown has closed admission (the caller answers 503 and nothing is
// recorded). A ping on an alerted beat queues the recovered notification for
// the Run loop, so this never blocks on the webhook.
//
// Admission is decided HERE, under the same mutex as the state mutation and
// the recovered enqueue, because that is the only way the decision can be
// atomic with respect to shutdown: a caller-side context check can pass and
// then be descheduled while Run takes its undelivered-work snapshot and
// returns, so the ping would land behind a tally that has already been
// reported. Since stopAccepting takes this mutex too, every ping either
// completes before admission closes — and is therefore visible to
// logUndelivered — or observes accepting=false and records nothing at all.
func (w *Watcher) Beat(id string) (recorded, accepting bool) {
	w.mu.Lock()
	if !w.accepting {
		w.mu.Unlock()
		return false, false
	}
	st, ok := w.beats[id]
	if !ok {
		w.mu.Unlock()
		return false, true
	}
	now := w.now()
	previousSeen := st.lastSeen
	// The silence this ping ends, as the two instants that bound it: the beat's
	// last accepted ping and this one. Every figure below reads its span from
	// here (Transition.DownFor), so the deadline check, the queued record and
	// the recovered notice cannot measure the same silence differently.
	silence := Transition{Started: previousSeen, Observed: now}
	wasAlerted := st.alerted
	// A late ping ends an outage. When the sweep already recorded that
	// outage, seal the record it has not delivered yet; when the crossing
	// is one no sweep has seen at all, record the whole closed outage so
	// this ping cannot erase it. Recording does not depend on the queue being
	// empty, so an outage that both begins AND ends while an earlier notice
	// is undelivered still reaches Discord instead of vanishing; a FULL queue
	// is the one loss, counted and warned by recordEndedOutage.
	//
	// The two arms are the two reasons a history notice is late, and they are
	// only distinguishable here: sealing an open record leaves the reason the
	// sweep recorded (LateUndelivered — a live notice was due and had not been
	// delivered), while recording a closed outage names the reason from what
	// the sweep managed to see of it (lateReasonForUnqueuedOutage). Read
	// overflowAccounted before the reset below clears it.
	if open := st.openMissing(); open != nil {
		open.recoveredAt = now
	} else if !wasAlerted && overdue(silence.DownFor(), st.deadline) {
		st.recordEndedOutage(&overdueBeat{
			id:          id,
			silence:     silence,
			recoveredAt: now,
			late:        st.lateReasonForUnqueuedOutage(),
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
	publishFreshness(id, 0, st.deadline)
	// Queue the recovered transition INSIDE the critical section that mutated
	// the beat: admission closed under this same mutex, so a ping that got
	// here is wholly visible to the shutdown tally. Enqueueing after the
	// unlock would reopen the race one step further along — the channel's only
	// reader could have drained and returned in between. The channel send is
	// non-blocking, so holding the lock across it cannot stall a ping.
	recoveryDropped := false
	if wasAlerted {
		select {
		case w.recoveries <- recoveryEvent{id: id, silence: silence}:
		default:
			// Cannot happen while the queue bound matches the beat count
			// (one pending recovery per beat), but never block a ping.
			// The dropped recovery is no longer pending, so un-mark it or
			// the beat could never alert again.
			st.recovering = false
			recoveryDropped = true
		}
	}
	w.mu.Unlock()

	if recoveryDropped {
		// Reported outside the lock: neither the counter nor the log line is
		// part of the state transition. Nothing retries a dropped recovery
		// notice, so this is a permanent loss like a dropped missing record,
		// not a failed delivery attempt.
		metrics.RecordNotificationDropped(metrics.KindRecovered)
		slog.Warn("recovery queue full, dropping recovered notification, nothing retries it and no notice for this recovery will ever arrive",
			"beat", id, "down_for", silence.DownFor().String(), "retryable", false)
	}
	return true, true
}

// stopAccepting closes beat admission for the rest of the process's life.
// It takes the mutex Beat mutates under, so once it returns no ping can still
// be between its admission check and its state change: that ordering is what
// makes logUndelivered's tally complete rather than merely narrow.
func (w *Watcher) stopAccepting() {
	w.mu.Lock()
	w.accepting = false
	w.mu.Unlock()
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
			// Close admission before tallying: stopAccepting and the snapshot
			// serialize on the same mutex, so nothing can be recorded between
			// them (see Beat).
			w.stopAccepting()
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
// permanent-loss path no counter can show: notifications_dropped_total
// counts a MESSAGE that is lost for good once its delivery was ATTEMPTED and
// failed (sendRecovered) or its queued transition was discarded
// (Beat's full recovery channel), and outage_records_dropped_total counts a
// RECORD discarded by a full pending queue (recordEndedOutage), while a record
// still sitting in a queue here was
// never attempted and never discarded, so the log line is the operator's only
// trace of it.
//
// The tally below is complete by construction, and mechanically so: Run closes
// admission (stopAccepting) before taking the snapshot, and both take the same
// mutex Beat mutates under, so a ping either finished before admission closed
// — and is counted here — or observes accepting=false, records nothing, and is
// refused with 503. webapi.New additionally gates /beat/{id} on the shared
// application context, which refuses the long drain window early (before the
// body is even read) rather than deep in the watcher; the atomic boundary is
// Beat's, whichever order the HTTP drain and this loop's exit happen to run in.
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
		// Only a CLOSED record is a permanent loss; the open tail (openMissing)
		// is an outage still in progress, which logUndelivered's godoc explains.
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

// overdue reports whether an observed silence has passed the beat's deadline.
// It is the single home of the freshness boundary: publishFreshness maps it to
// the quorum gauge and Beat uses it to decide whether a late ping closed an
// outage no sweep has seen, so the two paths cannot drift apart.
func overdue(silence, deadline time.Duration) bool {
	return silence > deadline
}

// publishFreshness publishes the freshness gauge for id given its observed
// silence and deadline, reporting whether the beat is still fresh. It is the
// ONLY writer of that gauge (New, Beat, refreshFreshness and collectBeatDue
// all publish through it), and it reads the boundary from overdue, so the
// quorum ground truth cannot drift between the paths that publish it. Callers
// hold w.mu, except New, which publishes before the Watcher is shared.
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
//
// Sending is bounded by sweepSendBudget: once it is spent the sweep stops
// starting sends and returns, so Run can service the recoveries channel
// instead of waiting out a whole storm of missing notices. The cut lands at
// the tail of this sweep's ordering — a beat is never skipped to reach a later
// one — and a beat left unreached is untouched, so the next sweep sends it.
func (w *Watcher) sweep(ctx context.Context) {
	live, history := w.collectDue()
	budget := w.now().Add(sweepSendBudget)
	for i, past := range history {
		// Every unsent entry is one beat, history and live alike (collectDue
		// yields at most one notice per beat), so the whole remainder of both
		// orderings is what a cut here defers.
		if w.budgetSpent(budget, len(history)-i+len(live)) {
			w.blameDeferredHistory(history[i:])
			return
		}
		if w.sendHistory(ctx, past) {
			return
		}
	}
	for i := range live {
		if w.budgetSpent(budget, len(live)-i) {
			return
		}
		if w.sendMissing(ctx, &live[i]) {
			return
		}
	}
}

// budgetSpent reports whether this sweep has spent its send budget and must
// stop before STARTING the next beat's send, logging the cut once with the
// number of beats deferred to the next sweep. Callers check it between beats
// only, so a send already in flight is never interrupted.
//
// A deferred beat moves NO counter and keeps alerted where it was: nothing was
// attempted for it, so it is neither a failed delivery (which would promise a
// retry that is already the plain behavior here) nor a dropped notice (which
// would claim it will never arrive). Its outage survives the cut: the queued
// record is still there for the next sweep, which is why a cut can never
// swallow an outage. The one thing a cut does rewrite is the late reason of the
// records behind a deferred HISTORY notice, upgraded to LateUndelivered
// (blameDeferredHistory), because this budget only bites when delivery is slow
// and a past-tense notice must not vouch for a webhook that is behind. DEBUG
// matches recordOngoingOutage:
// like a full pending queue during a webhook outage, this is back-pressure
// that costs a tick, not a fault.
//
// A deferral is also a third way a notice becomes late: if the beat pings
// before the next sweep reaches it, its queued open record is sealed and the
// outage is reported as history. That record was queued by collectDue, so it
// reports LateUndelivered, which is the true reason — the alert that was due
// was not delivered before the beat returned — and pointing at the webhook
// fits: this budget only bites when sends are slow enough to spend 5s, which a
// healthy webhook never is (TestFastSendsDeliverEveryDueBeatInOneSweep).
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
		due, run := collectBeatDue(id, st, now)
		if len(run) > 0 {
			history = append(history, beatOutages{id: id, outages: run})
			continue
		}
		if due != nil {
			live = append(live, *due)
		}
	}
	return live, history
}

// collectBeatDue is collectDue's per-beat state transition: it publishes the
// beat's freshness gauge, records a newly detected crossing, refreshes an
// open record's reading, and reports what this sweep owes the beat — a live
// missing notice, a run of ended outages to collapse into one history notice,
// or neither. The caller holds w.mu; the returned record is a COPY, so no
// pointer into pendingMissing escapes the critical section.
func collectBeatDue(id string, st *beatState, now time.Time) (*overdueBeat, []Outage) {
	// This sweep's reading of the beat's silence, as the two instants that
	// bound it: the last accepted ping and this sweep. The freshness gauge,
	// a queued record and the live notice all read their span from it.
	silence := Transition{Started: st.lastSeen, Observed: now}
	fresh := publishFreshness(id, silence.DownFor(), st.deadline)
	// An overdue beat whose current outage is not on the queue yet is
	// a fresh crossing to record. Recording it here rather than
	// skipping the beat is what keeps a second outage alive while an
	// earlier notice is still undelivered.
	//
	// A record this path queues is OPEN, so it can only ever become
	// history if a ping ends the outage before its live notice was
	// delivered: the send failed, st.recovering held the beat behind an
	// earlier recovery, or sweepSendBudget deferred it to a later sweep.
	// All three are one fact — the alert that was due had not been
	// delivered — so the reason is LateUndelivered for every one of them,
	// stated here rather than left to the zero value.
	if !fresh && !st.alerted && st.openMissing() == nil {
		st.recordOngoingOutage(&overdueBeat{
			id: id, silence: silence, late: LateUndelivered,
		})
	}
	head := st.headMissing()
	if head == nil {
		return nil, nil
	}
	// Only the still-open current outage refreshes its silence, so a
	// retry reports how long the beat has been quiet: its observation
	// instant moves forward while its start — the record's own anchor —
	// never does. Once a ping seals the record, that reading freezes at
	// the observation taken when the outage was detected; a history notice
	// reports the outage's full span (Outage.DownFor) instead, so the
	// frozen reading is only supplementary detail.
	// The lastSeen match is a defensive second guard, the twin of
	// markDelivered's: an open head is always the tail (openMissing), and
	// a ping seals the open tail in the same critical section that moves
	// lastSeen, so today it cannot disagree with recoveredAt. Keep it, so
	// a record whose start no longer matches lastSeen can never have a
	// later beat's observation written over its own reading.
	if head.recoveredAt.IsZero() && head.silence.Started.Equal(st.lastSeen) {
		head.silence.Observed = now
	}
	// Held while an earlier recovery is queued or in flight, so
	// transitions reach Discord in chronological order.
	if st.recovering {
		return nil, nil
	}
	// An ended head is history: collapse its whole run into one notice.
	// An open head is a live incident and keeps the present-tense path.
	if run := st.closedRun(); len(run) > 0 {
		return nil, run
	}
	// Copy the record before it leaves the critical section, so the sweep
	// never holds a pointer into pendingMissing.
	due := *head
	return &due, nil
}

// sendMissing delivers one due missing transition and reports whether
// shutdown cancellation should stop the sweep. beat points into the sweep's
// OWN slice of records collectDue copied out under the lock — never at a
// queued record — so reading it here without the lock is safe; the pointer
// only avoids copying the record again (gocritic hugeParam).
func (w *Watcher) sendMissing(ctx context.Context, beat *overdueBeat) bool {
	if err := w.notifier.BeatMissing(ctx, beat.id, beat.silence); err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("missing notification abandoned, shutting down",
				"beat", beat.id, "retryable", false)
			return true
		}
		metrics.RecordNotificationFailed(metrics.KindMissing)
		slog.Error("missing notification failed, will retry next sweep",
			"beat", beat.id, "silence", beat.silence.DownFor().String(), "error", err,
			"retryable", true)
		return false
	}
	metrics.RecordNotificationSent(metrics.KindMissing)
	slog.Info("beat missing, notified", "beat", beat.id, "silence", beat.silence.DownFor().String())
	if event, raced := w.markDelivered(beat.id, beat.silence.Started); raced {
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
//
// A real failure also rewrites the late reason of the records it left queued
// (markHistoryUndelivered): they are now late because this notice's own
// delivery failed, so the retry must not still claim nothing was wrong with
// delivery. Cancellation is exempt from that as well as from the counter: a
// shutdown is not a delivery failure, so the queued records keep their
// original reason until logUndelivered reports their loss.
func (w *Watcher) sendHistory(ctx context.Context, past beatOutages) bool {
	if err := w.notifier.BeatOutageHistory(ctx, past.id, past.outages); err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("outage history notification abandoned, shutting down",
				"beat", past.id, "retryable", false)
			return true
		}
		metrics.RecordNotificationFailed(metrics.KindHistory)
		w.markHistoryUndelivered(past.id, len(past.outages))
		slog.Error("outage history notification failed, will retry next sweep",
			"beat", past.id, "outages", len(past.outages), "error", err,
			"retryable", true)
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

// markHistoryUndelivered blames delivery for the n head records a FAILED
// history notice left queued, so the retry reports the true reason it is late
// (see beatState.blameDelivery). The n records it rewrites are exactly the ones
// the failed notice covered, for the reason dropDelivered gives. A record
// queued after the failed send keeps its own reason — this notice never tried
// to deliver it.
func (w *Watcher) markHistoryUndelivered(id string, n int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.beats[id].blameDelivery(n)
}

// blameDeferredHistory blames delivery for the records of every history notice
// this sweep's send budget cut deferred (see beatState.blameDelivery). The
// budget only bites once sends are slow enough to spend 5s, so a notice it
// defers IS late because delivery is behind: a record still claiming
// LateEndedBeforeDetection would tell the operator "nothing was wrong with
// delivery" about a notice a struggling webhook pushed to a later sweep. Each
// entry's outage count is exactly the run collectDue took from that beat's
// head, for the reason dropDelivered gives.
//
// Cancellation is deliberately NOT routed here (sendHistory returns before the
// next budget check): a shutdown is not delivery falling behind, and those
// records die with the process anyway.
func (w *Watcher) blameDeferredHistory(deferred []beatOutages) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, past := range deferred {
		w.beats[past.id].blameDelivery(len(past.outages))
	}
}

// markDelivered records the outcome of a delivered missing send for id, given
// the start of the silence that notice reported (the lastSeen the sweep
// measured it from). It pops the delivered transition, promoting any later
// queued outage to the head for the next sweep. Normally it marks the beat
// alerted. When the outage is
// already over — the popped record carries the recovery point a ping sealed
// into it, including a ping that raced this very send — marking alerted
// would swallow the NEXT outage's missing notice, so the beat stays
// re-armed and the pending recovered transition is returned for immediate
// delivery.
func (w *Watcher) markDelivered(id string, started time.Time) (recoveryEvent, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.beats[id]
	delivered := st.popMissing()
	if delivered.recoveredAt.IsZero() && st.lastSeen.Equal(started) {
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
	// The recovered notice reports the same silence the missing notice just
	// reported, ending at the recovery point instead of at the sweep.
	return recoveryEvent{id: id, silence: Transition{Started: started, Observed: recoveredAt}}, true
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
//
// Recovered is FIRE-ONCE, which is what makes a failed send terminal here.
// The event is dequeued before the send and finishRecovery runs
// unconditionally on the way out, so nothing holds a record to retry from: a
// failed attempt means no recovered notice for that outage will ever arrive.
// That is the dropped counter's meaning, not the failed counter's, so a
// non-cancellation failure counts as DROPPED — the same accounting a
// recovery discarded by a full queue gets in Beat. Contrast sendMissing and
// sendHistory, whose records stay queued: their failures are genuinely
// retried on the next sweep and belong on failed.
//
// Cancellation is exempt from both counters: a shutdown is not a delivery
// failure. It is NOT covered by logUndelivered's tally either, though — the
// event was already taken off w.recoveries by Run's select, so the drain
// there cannot see it and queued_recoveries excludes it. The Info line below is
// this loss's ONLY trace, which is why it names the beat and its span.
// The failure log below stays at Error rather than the Warn the queue-full
// drops use, because unlike them something WAS attempted and the webhook is
// broken.
func (w *Watcher) sendRecovered(ctx context.Context, ev recoveryEvent) {
	defer w.finishRecovery(ev.id)
	if err := w.notifier.BeatRecovered(ctx, ev.id, ev.silence); err != nil {
		if errors.Is(err, context.Canceled) {
			slog.Info("recovered notification abandoned, shutting down, nothing retries it so no notice for this recovery will ever arrive",
				"beat", ev.id, "down_for", ev.silence.DownFor().String(), "retryable", false)
			return
		}
		metrics.RecordNotificationDropped(metrics.KindRecovered)
		slog.Error("recovered notification failed, nothing retries it and no notice for this recovery will ever arrive",
			"beat", ev.id, "down_for", ev.silence.DownFor().String(), "error", err,
			"retryable", false)
		return
	}
	metrics.RecordNotificationSent(metrics.KindRecovered)
	slog.Info("beat recovered, notified", "beat", ev.id, "down_for", ev.silence.DownFor().String())
}
