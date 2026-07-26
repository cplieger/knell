package watch

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cplieger/knell/internal/metrics"
	"github.com/cplieger/slogx/capture"
)

// findLabeledValue scrapes the metrics exposition and returns the value token
// of name{label="<value>"}, reporting whether the series is present at all.
func findLabeledValue(name, label, value string) (string, bool) {
	rec := httptest.NewRecorder()
	metrics.Registry.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	prefix := name + `{` + label + `="` + value + `"} `
	for line := range strings.Lines(rec.Body.String()) {
		if v, ok := strings.CutPrefix(line, prefix); ok {
			return strings.TrimSpace(v), true
		}
	}
	return "", false
}

// labeledValue scrapes the metrics exposition and returns the value token
// of name{label="<value>"}, failing the test when the series is absent.
func labeledValue(t *testing.T, name, label, value string) string {
	t.Helper()
	v, ok := findLabeledValue(name, label, value)
	if !ok {
		t.Fatalf("series %s{%s=%q} not in exposition", name, label, value)
	}
	return v
}

func TestBeatFreshGaugeTracksOverdueAndRecovery(t *testing.T) {
	t.Parallel()

	// Unique beat id: the metric registry is package-global, so a label
	// value no other test uses keeps this test's series isolated even
	// under t.Parallel.
	const id = "metrics-quorum-probe"
	w, clock, _ := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})

	if got := labeledValue(t, "knell_beat_fresh", "beat", id); got != "1" {
		t.Fatalf("beat_fresh at boot = %s, want 1", got)
	}
	bootSeen := labeledValue(t, "knell_beat_last_seen_timestamp_seconds", "beat", id)

	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if got := labeledValue(t, "knell_beat_fresh", "beat", id); got != "0" {
		t.Fatalf("beat_fresh when overdue = %s, want 0", got)
	}

	if !w.Beat(id) {
		t.Fatal("Beat returned false for configured id")
	}
	if got := labeledValue(t, "knell_beat_fresh", "beat", id); got != "1" {
		t.Fatalf("beat_fresh after ping = %s, want 1", got)
	}
	if got := labeledValue(t, "knell_beat_last_seen_timestamp_seconds", "beat", id); got == bootSeen {
		t.Errorf("beat_last_seen after ping = %s, still the boot baseline", got)
	}
}

func TestCanceledNotificationsAreNotCountedAsFailed(t *testing.T) {
	// Serial (no t.Parallel): it asserts deltas on the package-global
	// failure counters, which the parallel tests also increment.
	const id = "cancel-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	clock.Advance(11 * time.Minute)

	failedBefore := labeledValue(t, "knell_notifications_failed_total", "kind", "missing")
	n.setFail(context.Canceled)
	w.sweep(context.Background())
	if got := labeledValue(t, "knell_notifications_failed_total", "kind", "missing"); got != failedBefore {
		t.Errorf("failed{missing} = %s after canceled send, want unchanged %s (a shutdown must not page KnellNotifyFailing)", got, failedBefore)
	}

	// The abandoned send did not mark the beat alerted: once the notifier
	// heals, the outage is still reported.
	n.setFail(nil)
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0].kind != "missing" {
		t.Fatalf("calls = %v, want the missing notice retried after a shutdown-abandoned send", got)
	}

	// Recovered direction: queue a recovery, cancel its delivery; the
	// failed counter must not move either.
	w.Beat(id)
	failedBefore = labeledValue(t, "knell_notifications_failed_total", "kind", "recovered")
	n.setFail(context.Canceled)
	drainRecoveries(w)
	if got := labeledValue(t, "knell_notifications_failed_total", "kind", "recovered"); got != failedBefore {
		t.Errorf("failed{recovered} = %s after canceled send, want unchanged %s", got, failedBefore)
	}
}

func TestSweepExactDeadlineBoundaryIsFresh(t *testing.T) {
	t.Parallel()

	// silence == deadline is still fresh ("within its deadline" is
	// inclusive); only silence strictly past the deadline is overdue.
	const id = "boundary-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})

	clock.Advance(10 * time.Minute)
	w.sweep(context.Background())
	if got := labeledValue(t, "knell_beat_fresh", "beat", id); got != "1" {
		t.Fatalf("beat_fresh at silence == deadline = %s, want 1 (inclusive boundary)", got)
	}
	if calls := n.snapshot(); len(calls) != 0 {
		t.Fatalf("exact-deadline sweep notified: %v", calls)
	}

	clock.Advance(time.Nanosecond)
	w.sweep(context.Background())
	if got := labeledValue(t, "knell_beat_fresh", "beat", id); got != "0" {
		t.Fatalf("beat_fresh just past deadline = %s, want 0", got)
	}
	calls := n.snapshot()
	if len(calls) != 1 || calls[0].kind != "missing" {
		t.Fatalf("calls just past deadline = %v, want one missing", calls)
	}
}

var refreshProbeSequence atomic.Uint64

func TestRefreshFreshnessUpdatesGaugeWithoutNotifying(t *testing.T) {
	t.Parallel()

	id := "refresh-probe-" + strconv.FormatUint(refreshProbeSequence.Add(1), 10)
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})

	// Construction pre-mints the received counter at zero so increase()
	// alerts have a baseline sample before the first ping.
	if got := labeledValue(t, "knell_beats_received_total", "beat", id); got != "0" {
		t.Errorf("beats_received_total at boot = %s, want 0", got)
	}

	// refreshFreshness alone must flip the gauge when the beat goes
	// overdue -- without sending any notification (that is sweep's job).
	// This is the documented ground-truth path while the sender loop is
	// blocked on a slow webhook.
	clock.Advance(11 * time.Minute)
	w.refreshFreshness()
	if got := labeledValue(t, "knell_beat_fresh", "beat", id); got != "0" {
		t.Fatalf("beat_fresh after refreshFreshness = %s, want 0", got)
	}
	if calls := n.snapshot(); len(calls) != 0 {
		t.Fatalf("refreshFreshness sent notifications: %v", calls)
	}

	// A ping restores the gauge; refreshFreshness must keep it at 1.
	if !w.Beat(id) {
		t.Fatal("Beat returned false for configured id")
	}
	w.refreshFreshness()
	if got := labeledValue(t, "knell_beat_fresh", "beat", id); got != "1" {
		t.Fatalf("beat_fresh after ping + refresh = %s, want 1", got)
	}
}

func TestUnknownBeatMintsNoMetricSeries(t *testing.T) {
	t.Parallel()

	// The beat id is a metric label, so a ping for an id that is not
	// configured must record nothing at all: minting a series on an unknown id
	// turns arbitrary /beat/<anything> request paths into unbounded, permanent
	// label cardinality in knell and in every observer scraping it. Beat's
	// false return is asserted elsewhere; this pins the half that has no
	// symptom at all until the series count explodes.
	const unknown = "unknown-cardinality-probe"
	w, _, _ := newTestWatcher(Beat{ID: "known-cardinality-probe", Deadline: 10 * time.Minute})
	if w.Beat(unknown) {
		t.Fatalf("Beat(%s) = true, want false for an unconfigured id", unknown)
	}
	for _, name := range []string{
		"knell_beats_received_total",
		"knell_beat_fresh",
		"knell_beat_last_seen_timestamp_seconds",
		"knell_beat_outages_total",
	} {
		if got, ok := findLabeledValue(name, "beat", unknown); ok {
			t.Errorf("%s{beat=%q} = %s after a ping for an unconfigured id, want the series absent (the id is a label)", name, unknown, got)
		}
	}
}

// counterValue parses the exposition value of name{kind="<kind>"} as a float.
func counterValue(t *testing.T, name, kind string) float64 {
	t.Helper()
	v, err := strconv.ParseFloat(labeledValue(t, name, "kind", kind), 64)
	if err != nil {
		t.Fatalf("parsing %s{kind=%q} value: %v", name, kind, err)
	}
	return v
}

// beatCounterValue parses the exposition value of name{beat="<beat>"} as a
// float, for the per-beat counters (outages, received pings).
func beatCounterValue(t *testing.T, name, beat string) float64 {
	t.Helper()
	v, err := strconv.ParseFloat(labeledValue(t, name, "beat", beat), 64)
	if err != nil {
		t.Fatalf("parsing %s{beat=%q} value: %v", name, beat, err)
	}
	return v
}

func TestDeliveredNotificationsIncrementSentCounters(t *testing.T) {
	// Serial (no t.Parallel): asserts deltas on the package-global sent
	// counters, which the parallel tests also increment.
	const id = "sent-counter-probe"
	w, clock, _ := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	clock.Advance(11 * time.Minute)

	missingBefore := counterValue(t, "knell_notifications_sent_total", "missing")
	w.sweep(context.Background())
	if got := counterValue(t, "knell_notifications_sent_total", "missing"); got != missingBefore+1 {
		t.Errorf("sent{missing} = %v after delivered send, want %v (the sent counter is the delivery ground truth dashboards read)", got, missingBefore+1)
	}

	recoveredBefore := counterValue(t, "knell_notifications_sent_total", "recovered")
	w.Beat(id)
	drainRecoveries(w)
	if got := counterValue(t, "knell_notifications_sent_total", "recovered"); got != recoveredBefore+1 {
		t.Errorf("sent{recovered} = %v after delivered recovery, want %v", got, recoveredBefore+1)
	}
}

func TestFailedMissingNotificationIncrementsFailedCounter(t *testing.T) {
	// Serial (no t.Parallel): asserts deltas on the package-global failed
	// counter. A real (non-canceled) delivery failure must move it: the
	// KnellNotifyFailing alert increases() over exactly this series.
	const id = "failed-counter-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	clock.Advance(11 * time.Minute)

	failedBefore := counterValue(t, "knell_notifications_failed_total", "missing")
	sentBefore := counterValue(t, "knell_notifications_sent_total", "missing")
	n.setFail(errors.New("discord down"))
	w.sweep(context.Background())
	if got := counterValue(t, "knell_notifications_failed_total", "missing"); got != failedBefore+1 {
		t.Errorf("failed{missing} = %v after failed send, want %v (KnellNotifyFailing increases() over this counter)", got, failedBefore+1)
	}
	if got := counterValue(t, "knell_notifications_sent_total", "missing"); got != sentBefore {
		t.Errorf("sent{missing} = %v after failed send, want unchanged %v", got, sentBefore)
	}
}

func TestQueueFullDropIsLoggedAsAWarning(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, which every other test writes through.
	const id = "missing-overflow-log-probe"
	w, clock, _ := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	for range missingQueueSize {
		clock.Advance(11 * time.Minute)
		if !w.Beat(id) {
			t.Fatalf("Beat(%s) = false", id)
		}
	}

	rec := capture.Default(t)
	clock.Advance(47 * time.Minute)
	if !w.Beat(id) {
		t.Fatalf("overflow Beat(%s) = false", id)
	}

	// The dropped record belongs to an outage a ping already ended, so it is
	// the outage's last trace and no notice for it will ever arrive: a
	// permanent loss stays WARN, never the debug level the not-lost
	// back-pressure case uses.
	if got := rec.CountLevel(slog.LevelWarn, "pending missing queue full"); got != 1 {
		t.Fatalf("queue-full warnings = %d, want exactly 1 (a dropped outage must name itself in the log, not only in a counter): %v", got, rec.Messages())
	}
	if got := rec.CountLevel(slog.LevelDebug, "pending missing queue full"); got != 0 {
		t.Errorf("queue-full debug lines = %d, want 0 (a permanently lost notice is news, not back-pressure): %v", got, rec.Messages())
	}
	if !rec.Contains("will never be delivered") {
		t.Errorf("drop warning does not say the notification is lost for good: %v", rec.Messages())
	}
	if !rec.HasAttr("pending missing queue full", "beat", id) {
		t.Errorf("drop warning does not name the beat: %v", rec.Records())
	}
	if !rec.HasAttr("pending missing queue full", "silence", "47m0s") {
		t.Errorf("drop warning does not report the dropped outage's silence: %v", rec.Records())
	}
}

func TestQueueFullOverflowIsAccountedOncePerAffectedOutage(t *testing.T) {
	// Serial (no t.Parallel): asserts deltas on the package-global
	// notification counters and captures the process-global slog default. An
	// affected outage must be accounted ONCE, not once per 15s sweep: every
	// sweep re-detects the same still-unqueued crossing, and re-reporting it
	// would present a single outage as dozens of events.
	const id = "missing-overflow-cadence-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	for range missingQueueSize {
		clock.Advance(11 * time.Minute)
		if !w.Beat(id) {
			t.Fatalf("Beat(%s) = false", id)
		}
	}

	// The webhook is down, so nothing drains and the beat's current outage
	// cannot be queued: three sweeps re-detect that one ongoing outage,
	// which stays detectable and is queued once a slot opens.
	n.setFail(errors.New("discord down"))
	clock.Advance(11 * time.Minute)
	failedBefore := counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore := counterValue(t, "knell_notifications_dropped_total", "missing")
	historyBefore := counterValue(t, "knell_notifications_failed_total", "history")
	outagesBefore := beatCounterValue(t, "knell_beat_outages_total", id)
	rec := capture.Default(t)
	const sweeps = 3
	for range sweeps {
		w.sweep(context.Background())
	}
	// Nothing was lost and nothing was attempted for the ongoing outage, so
	// neither the failed nor the dropped counter may move for it. The queue
	// head is an ended outage, so every sweep's failed send is a history
	// message: one failure per message, on the history kind only.
	if got := counterValue(t, "knell_notifications_failed_total", "missing"); got != failedBefore {
		t.Errorf("failed{missing} = %v after %d sweeps with a full queue, want unchanged %v (the ongoing outage is deferred, not a failed delivery)", got, sweeps, failedBefore)
	}
	if got := counterValue(t, "knell_notifications_dropped_total", "missing"); got != droppedBefore {
		t.Errorf("dropped{missing} = %v after %d sweeps with a full queue, want unchanged %v (the ongoing outage is not lost: it is queued once a slot frees)", got, sweeps, droppedBefore)
	}
	if got, want := counterValue(t, "knell_notifications_failed_total", "history"), historyBefore+sweeps; got != want {
		t.Errorf("failed{history} = %v after %d failed history sends, want %v (one per failed message)", got, sweeps, want)
	}
	if got, want := beatCounterValue(t, "knell_beat_outages_total", id), outagesBefore+1; got != want {
		t.Errorf("beat_outages_total = %v after %d re-detections of one outage, want %v (one increment per outage, even when its notice is not queued yet)", got, sweeps, want)
	}
	// Observable but quiet: one debug line for the affected outage, not one
	// per tick, and no warning (nothing is lost yet).
	if got := rec.CountLevel(slog.LevelDebug, "pending missing queue full"); got != 1 {
		t.Errorf("queue-full debug lines = %d over %d sweeps, want exactly 1 per affected outage: %v", got, sweeps, rec.Messages())
	}
	if got := rec.CountLevel(slog.LevelWarn, "pending missing queue full"); got != 0 {
		t.Errorf("queue-full warnings = %d over %d sweeps, want 0 (back-pressure during a webhook outage is not news): %v", got, sweeps, rec.Messages())
	}

	// The ping that ends that outage is where it actually becomes lost: the
	// queue is still full, so its closed record is discarded and no notice
	// for it will ever arrive. That moves the dropped counter once, still
	// never the failed one, and the outage itself was already counted.
	failedBefore = counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore = counterValue(t, "knell_notifications_dropped_total", "missing")
	outagesBefore = beatCounterValue(t, "knell_beat_outages_total", id)
	closing := capture.Default(t)
	clock.Advance(11 * time.Minute)
	if !w.Beat(id) {
		t.Fatalf("closing Beat(%s) = false", id)
	}
	if got := counterValue(t, "knell_notifications_failed_total", "missing"); got != failedBefore {
		t.Errorf("failed{missing} = %v after the ping whose record was dropped, want unchanged %v (nothing was ever sent for it)", got, failedBefore)
	}
	if got, want := counterValue(t, "knell_notifications_dropped_total", "missing"), droppedBefore+1; got != want {
		t.Errorf("dropped{missing} = %v after the closing ping found no slot, want %v (that notice is lost for good)", got, want)
	}
	if got := beatCounterValue(t, "knell_beat_outages_total", id); got != outagesBefore {
		t.Errorf("beat_outages_total = %v after the ping closing an already-counted outage, want unchanged %v", got, outagesBefore)
	}
	if got := closing.CountLevel(slog.LevelWarn, "pending missing queue full"); got != 1 {
		t.Errorf("queue-full warnings on the closing ping = %d, want exactly 1: %v", got, closing.Messages())
	}

	// A genuinely NEW outage is accounted on its own: the accounting mark is
	// per outage, not a permanent mute.
	droppedBefore = counterValue(t, "knell_notifications_dropped_total", "missing")
	outagesBefore = beatCounterValue(t, "knell_beat_outages_total", id)
	fresh := capture.Default(t)
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if got, want := beatCounterValue(t, "knell_beat_outages_total", id), outagesBefore+1; got != want {
		t.Errorf("beat_outages_total = %v after a new outage, want %v", got, want)
	}
	if got := counterValue(t, "knell_notifications_dropped_total", "missing"); got != droppedBefore {
		t.Errorf("dropped{missing} = %v after a new ONGOING outage overflowed, want unchanged %v (it is deferred, not lost)", got, droppedBefore)
	}
	if got := fresh.CountLevel(slog.LevelDebug, "pending missing queue full"); got != 1 {
		t.Errorf("queue-full debug lines for the new outage = %d, want exactly 1 (a fresh overflow reports itself): %v", got, fresh.Messages())
	}
}

func TestFailedAndDroppedNeverBothMoveForOneEvent(t *testing.T) {
	// Serial (no t.Parallel): asserts deltas on the package-global
	// notification counters. failed and dropped answer two different
	// operator questions -- "wait, it will retry" vs "reconstruct the missed
	// window" -- so a single event must land on exactly one of them, or
	// KnellNotifyFailing reports one incident as two.
	const id = "failed-vs-dropped-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)

	// Event 1: a genuine delivery failure. failed only.
	clock.Advance(11 * time.Minute)
	n.setFail(errors.New("discord down"))
	failedBefore := counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore := counterValue(t, "knell_notifications_dropped_total", "missing")
	w.sweep(context.Background())
	if got, want := counterValue(t, "knell_notifications_failed_total", "missing"), failedBefore+1; got != want {
		t.Errorf("failed{missing} = %v after a failed delivery, want %v", got, want)
	}
	if got := counterValue(t, "knell_notifications_dropped_total", "missing"); got != droppedBefore {
		t.Errorf("dropped{missing} = %v after a failed delivery, want unchanged %v (the record is still queued and retries)", got, droppedBefore)
	}

	// Event 2: a permanent queue-full drop of an ended outage. dropped only.
	// Fill the remaining slots with ended outages first (the failed send
	// above left its record queued and open until this ping seals it).
	for len(w.beats[id].pendingMissing) < missingQueueSize {
		clock.Advance(11 * time.Minute)
		if !w.Beat(id) {
			t.Fatalf("Beat(%s) = false", id)
		}
	}
	failedBefore = counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore = counterValue(t, "knell_notifications_dropped_total", "missing")
	clock.Advance(11 * time.Minute)
	if !w.Beat(id) {
		t.Fatalf("overflow Beat(%s) = false", id)
	}
	if got, want := counterValue(t, "knell_notifications_dropped_total", "missing"), droppedBefore+1; got != want {
		t.Errorf("dropped{missing} = %v after a queue-full drop, want %v", got, want)
	}
	if got := counterValue(t, "knell_notifications_failed_total", "missing"); got != failedBefore {
		t.Errorf("failed{missing} = %v after a queue-full drop, want unchanged %v (nothing was attempted, nothing will retry)", got, failedBefore)
	}

	// Event 3: the sweep-path queue-full event on an ongoing outage. Neither
	// counter moves: the notice is deferred, not failed and not lost.
	n.setFail(nil)
	failedBefore = counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore = counterValue(t, "knell_notifications_dropped_total", "missing")
	outagesBefore := beatCounterValue(t, "knell_beat_outages_total", id)
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if got, want := beatCounterValue(t, "knell_beat_outages_total", id), outagesBefore+1; got != want {
		t.Fatalf("beat_outages_total = %v, want %v (the sweep must have detected the crossing while the queue was full)", got, want)
	}
	if got := counterValue(t, "knell_notifications_failed_total", "missing"); got != failedBefore {
		t.Errorf("failed{missing} = %v after a sweep-path queue-full event, want unchanged %v", got, failedBefore)
	}
	if got := counterValue(t, "knell_notifications_dropped_total", "missing"); got != droppedBefore {
		t.Errorf("dropped{missing} = %v after a sweep-path queue-full event, want unchanged %v", got, droppedBefore)
	}
}

func TestRecoveryQueueDropIsCountedAsDroppedNotFailed(t *testing.T) {
	// Serial (no t.Parallel): asserts deltas on the package-global
	// notification counters and captures the process-global slog default. A
	// dropped recovered notice is best-effort and consumed, so nothing will
	// ever retry it: it belongs on dropped, next to the queue-full missing
	// drop, not on the counter that means "a delivery attempt failed".
	beats := []Beat{
		{ID: "recovery-drop-probe-a", Deadline: 10 * time.Minute},
		{ID: "recovery-drop-probe-b", Deadline: 10 * time.Minute},
	}
	clock := newFakeClock()
	n := &fakeNotifier{}
	w := New(beats, n, clock.Now)
	// New sizes the queue from the beat count, so shrink it by one slot to
	// reach the drop path at all (defensive-only in production).
	w.recoveries = make(chan recoveryEvent, len(beats)-1)

	clock.Advance(11 * time.Minute)
	for range beats {
		w.sweep(context.Background())
	}
	if got := len(n.snapshot()); got != len(beats) {
		t.Fatalf("missing notifications = %d, want %d (both beats must be alerted before their recoveries)", got, len(beats))
	}

	failedBefore := counterValue(t, "knell_notifications_failed_total", "recovered")
	droppedBefore := counterValue(t, "knell_notifications_dropped_total", "recovered")
	rec := capture.Default(t)
	// Ping both without draining: the first recovery queues, the second
	// finds the queue full and its notice is dropped for good.
	for _, b := range beats {
		if !w.Beat(b.ID) {
			t.Fatalf("Beat(%s) = false", b.ID)
		}
	}
	if got, want := counterValue(t, "knell_notifications_dropped_total", "recovered"), droppedBefore+1; got != want {
		t.Errorf("dropped{recovered} = %v after a full recovery queue, want %v (that notice will never be delivered)", got, want)
	}
	if got := counterValue(t, "knell_notifications_failed_total", "recovered"); got != failedBefore {
		t.Errorf("failed{recovered} = %v after a full recovery queue, want unchanged %v (nothing was attempted)", got, failedBefore)
	}
	if got := rec.CountLevel(slog.LevelWarn, "recovery queue full"); got != 1 {
		t.Errorf("recovery-drop warnings = %d, want exactly 1 (a lost notice is news): %v", got, rec.Messages())
	}
}

func TestColdStartExposesFailedAndDroppedSeriesAtZero(t *testing.T) {
	// Serial (no t.Parallel): it deletes and re-mints series on the
	// package-global counters, which every other test reads. Only a serial
	// test has the registry to itself, and this is the one place a cold start
	// is observable at all -- every other test shares whatever the earlier
	// ones counted. increase() needs an earlier sample, so a rule over these
	// counters is blind to the very first failure or drop unless New mints
	// both series at zero.
	kinds := []string{metrics.KindMissing, metrics.KindRecovered, metrics.KindHistory}
	counters := []string{"knell_notifications_failed_total", "knell_notifications_dropped_total"}
	for _, kind := range kinds {
		metrics.NotificationsFailed.Delete(kind)
		metrics.NotificationsDropped.Delete(kind)
	}
	for _, name := range counters {
		for _, kind := range kinds {
			if got, ok := findLabeledValue(name, "kind", kind); ok {
				t.Fatalf("%s{kind=%q} = %s before the cold start, want the series absent (the test's own precondition)", name, kind, got)
			}
		}
	}

	newTestWatcher(Beat{ID: "cold-start-probe", Deadline: 10 * time.Minute})

	for _, name := range counters {
		for _, kind := range kinds {
			if got := labeledValue(t, name, "kind", kind); got != "0" {
				t.Errorf("%s{kind=%q} at boot = %s, want 0 (pre-minted so increase() has a baseline sample)", name, kind, got)
			}
		}
	}
}

func TestHistoryNoticeCountsOncePerMessageWhileOutagesCountEach(t *testing.T) {
	// Serial (no t.Parallel): asserts deltas on the package-global
	// notification counters, which the parallel tests also move.
	const id = "history-counter-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})

	// Both history kinds are pre-minted at boot, alongside missing and
	// recovered: without an earlier zero sample, an increase() alert would
	// miss the very first history failure. labeledValue fails when a series
	// is absent, so these lookups ARE the pre-minting assertion (the values
	// themselves belong to other tests, which share the registry).
	labeledValue(t, "knell_notifications_sent_total", "kind", "history")
	labeledValue(t, "knell_notifications_failed_total", "kind", "history")
	// The per-beat outage counter is pre-minted per configured beat, and
	// this beat id is unique to this test, so its boot value is exactly 0.
	if got := labeledValue(t, "knell_beat_outages_total", "beat", id); got != "0" {
		t.Errorf("beat_outages_total at boot = %s, want 0 (pre-minted so increase() has a baseline)", got)
	}

	// Three outages that all end before any of them can be reported.
	w.Beat(id)
	n.setFail(errors.New("discord down"))
	const outages = 3
	for range outages {
		clock.Advance(11 * time.Minute)
		if !w.Beat(id) {
			t.Fatalf("Beat(%s) = false", id)
		}
	}
	if got, want := beatCounterValue(t, "knell_beat_outages_total", id), float64(outages); got != want {
		t.Errorf("beat_outages_total = %v after %d detected outages, want %v (counted at detection, no delivery involved)", got, outages, want)
	}

	// One collapsed message reports all three: the notification counter
	// tracks MESSAGES, so it moves by one, while the outage counter above
	// already tracked the outages.
	n.setFail(nil)
	sentBefore := counterValue(t, "knell_notifications_sent_total", "history")
	missingBefore := counterValue(t, "knell_notifications_sent_total", "missing")
	recoveredBefore := counterValue(t, "knell_notifications_sent_total", "recovered")
	w.sweep(context.Background())
	if got := n.snapshot(); len(got) != 1 || got[0].outages != outages {
		t.Fatalf("calls = %v, want one history notice covering %d outages", got, outages)
	}
	if got, want := counterValue(t, "knell_notifications_sent_total", "history"), sentBefore+1; got != want {
		t.Errorf("sent{history} = %v after one collapsed notice for %d outages, want %v (one per message)", got, outages, want)
	}
	if got := counterValue(t, "knell_notifications_sent_total", "missing"); got != missingBefore {
		t.Errorf("sent{missing} = %v after a history notice, want unchanged %v (history is its own kind)", got, missingBefore)
	}
	if got := counterValue(t, "knell_notifications_sent_total", "recovered"); got != recoveredBefore {
		t.Errorf("sent{recovered} = %v after a history notice, want unchanged %v (the notice states the outages are over)", got, recoveredBefore)
	}
	if got, want := beatCounterValue(t, "knell_beat_outages_total", id), float64(outages); got != want {
		t.Errorf("beat_outages_total = %v after delivery, want %v (delivery must not move an outage counter)", got, want)
	}
}

func TestCanceledHistoryNotificationIsNotFailedAndKeepsRecords(t *testing.T) {
	// Serial (no t.Parallel): asserts a delta on the package-global failed
	// counter. A shutdown-abandoned send must not page KnellNotifyFailing,
	// and it must not consume the outages it was about to report.
	const id = "history-cancel-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	for range 2 {
		clock.Advance(11 * time.Minute)
		if !w.Beat(id) {
			t.Fatalf("Beat(%s) = false", id)
		}
	}

	failedBefore := counterValue(t, "knell_notifications_failed_total", "history")
	n.setFail(context.Canceled)
	w.sweep(context.Background())
	if got := counterValue(t, "knell_notifications_failed_total", "history"); got != failedBefore {
		t.Errorf("failed{history} = %v after a canceled send, want unchanged %v", got, failedBefore)
	}
	if got := len(w.beats[id].pendingMissing); got != 2 {
		t.Errorf("queued records = %d after a canceled history send, want both retained for the next run", got)
	}

	// Once the notifier heals, the abandoned notice still goes out.
	n.setFail(nil)
	w.sweep(context.Background())
	if got := n.snapshot(); len(got) != 1 || got[0].kind != "history" || got[0].outages != 2 {
		t.Fatalf("calls = %v, want the abandoned history notice delivered after the notifier healed", got)
	}
}
