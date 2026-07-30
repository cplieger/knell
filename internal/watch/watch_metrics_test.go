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
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
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

	if !recordedBeat(w, id) {
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
	//
	// A shutdown is not a delivery outcome, so it must move NEITHER delivery
	// counter on any kind: not failed (which would page KnellNotifyFailing)
	// and not dropped (which would tell the operator to go reconstruct a
	// window by hand). That holds even for recovered, whose non-cancellation
	// failures DO count as dropped.
	const id = "cancel-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	clock.Advance(11 * time.Minute)

	failedBefore := labeledValue(t, "knell_notifications_failed_total", "kind", "missing")
	droppedBefore := labeledValue(t, "knell_notifications_dropped_total", "kind", "missing")
	n.setFail(context.Canceled)
	w.sweep(context.Background())
	if got := labeledValue(t, "knell_notifications_failed_total", "kind", "missing"); got != failedBefore {
		t.Errorf("failed{missing} = %s after canceled send, want unchanged %s (a shutdown must not page KnellNotifyFailing)", got, failedBefore)
	}
	if got := labeledValue(t, "knell_notifications_dropped_total", "kind", "missing"); got != droppedBefore {
		t.Errorf("dropped{missing} = %s after canceled send, want unchanged %s (the record is retained and retried, nothing was lost)", got, droppedBefore)
	}

	// The abandoned send did not mark the beat alerted: once the notifier
	// heals, the outage is still reported.
	n.setFail(nil)
	w.sweep(context.Background())
	got := n.snapshot()
	if len(got) != 1 || got[0].kind != "missing" {
		t.Fatalf("calls = %v, want the missing notice retried after a shutdown-abandoned send", got)
	}

	// Recovered direction: queue a recovery, cancel its delivery. Neither
	// counter may move -- this is the one recovered-send failure that is not
	// a permanent loss to report, because no attempt was made at all.
	w.Beat(id)
	failedBefore = labeledValue(t, "knell_notifications_failed_total", "kind", "recovered")
	droppedBefore = labeledValue(t, "knell_notifications_dropped_total", "kind", "recovered")
	n.setFail(context.Canceled)
	drainRecoveries(w)
	if got := labeledValue(t, "knell_notifications_failed_total", "kind", "recovered"); got != failedBefore {
		t.Errorf("failed{recovered} = %s after canceled send, want unchanged %s", got, failedBefore)
	}
	if got := labeledValue(t, "knell_notifications_dropped_total", "kind", "recovered"); got != droppedBefore {
		t.Errorf("dropped{recovered} = %s after canceled send, want unchanged %s (a shutdown is not a lost notice)", got, droppedBefore)
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
	if !recordedBeat(w, id) {
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
	if recordedBeat(w, unknown) {
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

// labeledCounterValue parses the exposition value of name{label="<value>"} as
// a float. It is the single parse-and-diagnose path behind the two
// label-specific helpers below.
func labeledCounterValue(t *testing.T, name, label, value string) float64 {
	t.Helper()
	v, err := strconv.ParseFloat(labeledValue(t, name, label, value), 64)
	if err != nil {
		t.Fatalf("parsing %s{%s=%q} value: %v", name, label, value, err)
	}
	return v
}

// counterValue parses the exposition value of name{kind="<kind>"} as a float.
func counterValue(t *testing.T, name, kind string) float64 {
	t.Helper()
	return labeledCounterValue(t, name, "kind", kind)
}

// beatCounterValue parses the exposition value of name{beat="<beat>"} as a
// float, for the per-beat counters (outages, received pings).
func beatCounterValue(t *testing.T, name, beat string) float64 {
	t.Helper()
	return labeledCounterValue(t, name, "beat", beat)
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
	// KnellNotifyFailing alert increases() over exactly this series. It must
	// NOT move dropped: unlike a recovered send, the missing record stays
	// queued and the next 15s sweep retries it, so the notice is late rather
	// than lost and the operator has nothing to reconstruct.
	const id = "failed-counter-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	clock.Advance(11 * time.Minute)

	failedBefore := counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore := counterValue(t, "knell_notifications_dropped_total", "missing")
	sentBefore := counterValue(t, "knell_notifications_sent_total", "missing")
	n.setFail(errors.New("discord down"))
	w.sweep(context.Background())
	if got := counterValue(t, "knell_notifications_failed_total", "missing"); got != failedBefore+1 {
		t.Errorf("failed{missing} = %v after failed send, want %v (KnellNotifyFailing increases() over this counter)", got, failedBefore+1)
	}
	if got := counterValue(t, "knell_notifications_dropped_total", "missing"); got != droppedBefore {
		t.Errorf("dropped{missing} = %v after failed send, want unchanged %v (a retryable kind is never a permanent loss)", got, droppedBefore)
	}
	if got := counterValue(t, "knell_notifications_sent_total", "missing"); got != sentBefore {
		t.Errorf("sent{missing} = %v after failed send, want unchanged %v", got, sentBefore)
	}

	// The record survived, which is what makes failed (not dropped) the right
	// counter: the very next sweep delivers the same notice.
	n.setFail(nil)
	w.sweep(context.Background())
	if got := n.snapshot(); len(got) != 1 || got[0].kind != "missing" {
		t.Fatalf("calls = %v, want the missing notice retried on the next sweep", got)
	}
}

func TestQueueFullDropIsLoggedAsAWarning(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, which every other test writes through.
	const id = "missing-overflow-log-probe"
	w, clock, _ := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	fillMissingQueue(t, w, clock, id)

	rec := capture.Default(t)
	clock.Advance(47 * time.Minute)
	if !recordedBeat(w, id) {
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
	// The field that tells a log rule this is a loss and not a late notice:
	// nothing was attempted for this record and nothing survives to attempt.
	if !rec.HasAttr("pending missing queue full", "retryable", "false") {
		t.Errorf("drop warning does not report retryable=false, so a log rule cannot tell it from a send that will retry: %v", rec.Records())
	}
}

// TestQueueFullDropKeepsTimestampAttrsTyped pins the ATTRIBUTE KINDS of the
// queue-full loss log, which the level/message assertions above cannot see: the
// drop warning is the lost outage's only trace, so its two instants must stay
// typed time.Time values in UTC (a pre-rendered RFC3339 string keeps the test
// above green while discarding sub-second precision and the typed shape a JSON
// handler emits).
func TestQueueFullDropKeepsTimestampAttrsTyped(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, which every other test writes through.
	const id = "missing-overflow-timestamp-probe"
	zone := time.FixedZone("probe", -7*60*60)
	clock := &fakeClock{now: time.Date(2026, 7, 23, 12, 0, 0, 123456789, zone)}
	w := New([]Beat{{ID: id, Deadline: 10 * time.Minute}}, &fakeNotifier{}, clock.Now, clock.Now())
	w.Beat(id)
	fillMissingQueue(t, w, clock, id)

	wantSince := clock.Now().UTC()
	clock.Advance(47*time.Minute + 987*time.Nanosecond)
	wantRecovered := clock.Now().UTC()
	rec := capture.Default(t)
	w.Beat(id)

	for key, want := range map[string]time.Time{"since": wantSince, "recovered": wantRecovered} {
		// capture.Recorder.Attr is the typed member of the attr-assertion
		// family: it hands back the slog.Value itself, so the KIND stays
		// assertable. The rendered accessors (AttrValue/HasAttr) cannot express
		// this contract at all — slog.Time("since", t) and
		// slog.String("since", t.String()) render identically.
		value, ok := rec.Attr("pending missing queue full", key)
		if !ok {
			t.Errorf("queue-full warning has no %s attribute: %v", key, rec.Records())
			continue
		}
		if value.Kind() != slog.KindTime {
			t.Errorf("queue-full warning %s kind = %s, want Time (structured log consumers must receive a timestamp, not a pre-rendered string)", key, value.Kind())
			continue
		}
		if timestamp := value.Time(); !timestamp.Equal(want) || timestamp.Location() != time.UTC {
			t.Errorf("queue-full warning %s = %v, want %v with UTC location and nanosecond precision", key, timestamp, want)
		}
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
	fillMissingQueue(t, w, clock, id)

	// The webhook is down, so nothing drains and the beat's current outage
	// cannot be queued: three sweeps re-detect that one ongoing outage,
	// which stays detectable and is queued once a slot opens.
	n.setFail(errors.New("discord down"))
	clock.Advance(11 * time.Minute)
	failedBefore := counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore := counterValue(t, "knell_notifications_dropped_total", "missing")
	historyBefore := counterValue(t, "knell_notifications_failed_total", "history")
	historyDroppedBefore := counterValue(t, "knell_notifications_dropped_total", "history")
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
	// History is retryable like missing: a failed history send keeps its
	// records queued, so it never becomes a permanent loss. Only the
	// fire-once recovered kind moves dropped on a failed send.
	if got := counterValue(t, "knell_notifications_dropped_total", "history"); got != historyDroppedBefore {
		t.Errorf("dropped{history} = %v after %d failed history sends, want unchanged %v (the records stay queued and retry)", got, sweeps, historyDroppedBefore)
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
	// for it will ever arrive. That moves the per-RECORD dropped counter once,
	// never the failed one and never the per-MESSAGE dropped counter, and the
	// outage itself was already counted.
	failedBefore = counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore = counterValue(t, "knell_notifications_dropped_total", "missing")
	recordsDroppedBefore := beatCounterValue(t, "knell_outage_records_dropped_total", id)
	outagesBefore = beatCounterValue(t, "knell_beat_outages_total", id)
	closing := capture.Default(t)
	clock.Advance(11 * time.Minute)
	if !recordedBeat(w, id) {
		t.Fatalf("closing Beat(%s) = false", id)
	}
	if got := counterValue(t, "knell_notifications_failed_total", "missing"); got != failedBefore {
		t.Errorf("failed{missing} = %v after the ping whose record was dropped, want unchanged %v (nothing was ever sent for it)", got, failedBefore)
	}
	if got, want := beatCounterValue(t, "knell_outage_records_dropped_total", id), recordsDroppedBefore+1; got != want {
		t.Errorf("outage_records_dropped_total = %v after the closing ping found no slot, want %v (that record is lost for good)", got, want)
	}
	if got := counterValue(t, "knell_notifications_dropped_total", "missing"); got != droppedBefore {
		t.Errorf("dropped{missing} = %v after the closing ping found no slot, want unchanged %v (a discarded record is not one lost message: a history notice collapses several)", got, droppedBefore)
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
	recordsDroppedBefore = beatCounterValue(t, "knell_outage_records_dropped_total", id)
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
	if got := beatCounterValue(t, "knell_outage_records_dropped_total", id); got != recordsDroppedBefore {
		t.Errorf("outage_records_dropped_total = %v after a new ONGOING outage overflowed, want unchanged %v (it is deferred, not lost)", got, recordsDroppedBefore)
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
	// KnellNotifyFailing reports one incident as two. A lost outage RECORD
	// lands on neither: it is counted per record on
	// knell_outage_records_dropped_total, because one history message covers
	// several records, so N lost records are not N lost messages.
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

	// Event 2: a permanent queue-full drop of an ended outage. The per-RECORD
	// counter only.
	// Fill the remaining slots with ended outages first (the failed send
	// above left its record queued and open until this ping seals it).
	for len(w.beats[id].pendingMissing) < missingQueueSize {
		clock.Advance(11 * time.Minute)
		if !recordedBeat(w, id) {
			t.Fatalf("Beat(%s) = false", id)
		}
	}
	failedBefore = counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore = counterValue(t, "knell_notifications_dropped_total", "missing")
	recordsDroppedBefore := beatCounterValue(t, "knell_outage_records_dropped_total", id)
	clock.Advance(11 * time.Minute)
	if !recordedBeat(w, id) {
		t.Fatalf("overflow Beat(%s) = false", id)
	}
	if got, want := beatCounterValue(t, "knell_outage_records_dropped_total", id), recordsDroppedBefore+1; got != want {
		t.Errorf("outage_records_dropped_total = %v after a queue-full drop, want %v", got, want)
	}
	if got := counterValue(t, "knell_notifications_dropped_total", "missing"); got != droppedBefore {
		t.Errorf("dropped{missing} = %v after a queue-full drop, want unchanged %v (the message counter must not move for a lost RECORD)", got, droppedBefore)
	}
	if got := counterValue(t, "knell_notifications_failed_total", "missing"); got != failedBefore {
		t.Errorf("failed{missing} = %v after a queue-full drop, want unchanged %v (nothing was attempted, nothing will retry)", got, failedBefore)
	}

	// Event 3: the sweep-path queue-full event on an ongoing outage. No
	// counter moves: the notice is deferred, not failed and not lost.
	n.setFail(nil)
	failedBefore = counterValue(t, "knell_notifications_failed_total", "missing")
	droppedBefore = counterValue(t, "knell_notifications_dropped_total", "missing")
	recordsDroppedBefore = beatCounterValue(t, "knell_outage_records_dropped_total", id)
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
	if got := beatCounterValue(t, "knell_outage_records_dropped_total", id); got != recordsDroppedBefore {
		t.Errorf("outage_records_dropped_total = %v after a sweep-path queue-full event, want unchanged %v", got, recordsDroppedBefore)
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
	w := New(beats, n, clock.Now, clock.Now())
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
		if !recordedBeat(w, b.ID) {
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
	if !rec.HasAttr("recovery queue full", "retryable", "false") {
		t.Errorf("recovery-drop warning does not report retryable=false, so a log rule cannot tell it from a send that will retry: %v", rec.Records())
	}
}

func TestFailedRecoveredSendIsCountedAsDroppedNotFailed(t *testing.T) {
	// Serial (no t.Parallel): asserts deltas on the package-global
	// notification counters and captures the process-global slog default.
	//
	// A recovered send is FIRE-ONCE: sendRecovered consumes the queued event
	// and calls finishRecovery unconditionally, so a failed attempt leaves
	// nothing to retry from and that recovery notice will never arrive. That
	// is the dropped counter's meaning, so the failure must land there and
	// NOT on failed, which promises the operator "wait, it retries". The
	// second half proves the premise rather than assuming it: after the
	// notifier heals, nothing re-sends.
	const id = "recovered-send-failure-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)

	// Deliver the missing notice so the beat is alerted and the next ping
	// queues a real recovered transition.
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if got := n.snapshot(); len(got) != 1 || got[0].kind != "missing" {
		t.Fatalf("calls = %v, want the missing notice delivered before the recovery", got)
	}

	n.setFail(errors.New("discord down"))
	if !recordedBeat(w, id) {
		t.Fatalf("Beat(%s) = false", id)
	}
	failedBefore := counterValue(t, "knell_notifications_failed_total", "recovered")
	droppedBefore := counterValue(t, "knell_notifications_dropped_total", "recovered")
	sentBefore := counterValue(t, "knell_notifications_sent_total", "recovered")
	rec := capture.Default(t)
	drainRecoveries(w)

	if got, want := counterValue(t, "knell_notifications_dropped_total", "recovered"), droppedBefore+1; got != want {
		t.Errorf("dropped{recovered} = %v after a failed recovered send, want %v (nothing retries it, so that notice will never arrive)", got, want)
	}
	if got := counterValue(t, "knell_notifications_failed_total", "recovered"); got != failedBefore {
		t.Errorf("failed{recovered} = %v after a failed recovered send, want unchanged %v (failed means the record is still queued and retries; this one is gone)", got, failedBefore)
	}
	if got := counterValue(t, "knell_notifications_sent_total", "recovered"); got != sentBefore {
		t.Errorf("sent{recovered} = %v after a failed recovered send, want unchanged %v", got, sentBefore)
	}
	// The log line is the operator's only per-beat trace of the loss, so it
	// must say the notice is gone rather than merely that a send failed.
	if got := rec.CountLevel(slog.LevelError, "recovered notification failed"); got != 1 {
		t.Errorf("recovered-failure error lines = %d, want exactly 1: %v", got, rec.Messages())
	}
	if !rec.Contains("will ever arrive") {
		t.Errorf("recovered-failure log does not say the notice is lost for good: %v", rec.Messages())
	}
	if !rec.HasAttr("recovered notification failed", "beat", id) {
		t.Errorf("recovered-failure log does not name the beat: %v", rec.Records())
	}
	// Same level as the retryable missing/history failures (something WAS
	// attempted), so the LEVEL cannot carry the distinction: this field does.
	if !rec.HasAttr("recovered notification failed", "retryable", "false") {
		t.Errorf("recovered-failure log does not report retryable=false, which is the only thing separating it from the Error a retried send logs: %v", rec.Records())
	}

	// The premise behind counting it as dropped: nothing is queued or resent
	// once the notifier heals. If a retry existed, failed would be right.
	n.setFail(nil)
	drainRecoveries(w)
	w.sweep(context.Background())
	if got := n.snapshot(); len(got) != 1 || got[0].kind != "missing" {
		t.Fatalf("calls = %v, want only the original missing: a dropped recovery must never be retried", got)
	}
}

// TestUndeliveredNoticeLogsCarryTheirRetryability pins the retryable field on
// the loss/failure sites the capture-based tests around it do not reach: the
// two send failures whose records stay queued, and the three notices abandoned
// by a shutdown. The field exists because the LEVEL cannot carry this
// distinction -- a retried send deliberately stays at Error rather than spamming
// Warn every sweep, and the abandoned recovered notice is a permanent loss
// logged at Info with no counter behind it at all -- so without it a log rule
// cannot tell "the notice is late, wait for it" from "no notice will ever
// arrive, reconstruct the window".
func TestUndeliveredNoticeLogsCarryTheirRetryability(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, which every other test writes through.
	const id = "retryability-log-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)

	// A failed missing send: its record stays queued and the next sweep sends
	// it again, so the failure is retryable.
	clock.Advance(11 * time.Minute)
	n.setFail(errors.New("discord down"))
	missing := capture.Default(t)
	w.sweep(context.Background())
	if got := missing.CountLevel(slog.LevelError, "missing notification failed"); got != 1 {
		t.Fatalf("missing-failure error lines = %d, want exactly 1: %v", got, missing.Messages())
	}
	if !missing.HasAttr("missing notification failed", "retryable", "true") {
		t.Errorf("missing-failure log does not report retryable=true, so a log rule reads a late notice as a lost one: %v", missing.Records())
	}

	// A failed history send: same shape, its whole run stays queued.
	clock.Advance(time.Minute)
	if !recordedBeat(w, id) {
		t.Fatalf("closing Beat(%s) = false", id)
	}
	history := capture.Default(t)
	w.sweep(context.Background())
	if got := history.CountLevel(slog.LevelError, "outage history notification failed"); got != 1 {
		t.Fatalf("history-failure error lines = %d, want exactly 1: %v", got, history.Messages())
	}
	if !history.HasAttr("outage history notification failed", "retryable", "true") {
		t.Errorf("history-failure log does not report retryable=true: %v", history.Records())
	}

	// A recovered notice abandoned by a shutdown: recovered is fire-once and
	// the event was already dequeued, so no notice for that recovery will ever
	// arrive -- and this site moves no counter, which makes the log line its
	// only trace and the field its only loss signal.
	n.setFail(nil)
	w.sweep(context.Background())
	// One more outage, this time delivered, so the beat is alerted and the next
	// ping queues a real recovered transition.
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if !recordedBeat(w, id) {
		t.Fatalf("recovering Beat(%s) = false", id)
	}
	if got := len(w.recoveries); got != 1 {
		t.Fatalf("queued recoveries = %d, want 1 (the test's own precondition)", got)
	}
	n.setFail(context.Canceled)
	abandoned := capture.Default(t)
	drainRecoveries(w)
	if got := abandoned.CountLevel(slog.LevelInfo, "recovered notification abandoned"); got != 1 {
		t.Fatalf("abandoned-recovery info lines = %d, want exactly 1: %v", got, abandoned.Messages())
	}
	if !abandoned.HasAttr("recovered notification abandoned", "retryable", "false") {
		t.Errorf("abandoned-recovery log does not report retryable=false, and no counter moves for this loss at all: %v", abandoned.Records())
	}

	// The two sibling abandonments, each on its own beat so no earlier state in
	// this test decides which branch the sweep takes. Both are shutdowns, so
	// this process sends nothing further for them: retryable=false, even though
	// the identically-shaped FAILURE above is retryable=true.
	const canceledMissingID = "retryability-canceled-missing-probe"
	cm, cmClock, cmNotifier := newTestWatcher(Beat{ID: canceledMissingID, Deadline: 10 * time.Minute})
	cm.Beat(canceledMissingID)
	cmClock.Advance(11 * time.Minute)
	cmNotifier.setFail(context.Canceled)
	canceledMissing := capture.Default(t)
	cm.sweep(context.Background())
	if got := canceledMissing.CountLevel(slog.LevelInfo, "missing notification abandoned"); got != 1 {
		t.Fatalf("abandoned-missing info lines = %d, want exactly 1: %v", got, canceledMissing.Messages())
	}
	if !canceledMissing.HasAttr("missing notification abandoned", "retryable", "false") {
		t.Errorf("abandoned-missing log does not report retryable=false, so a log rule reads a notice this process abandoned as one it will resend: %v", canceledMissing.Records())
	}

	const canceledHistoryID = "retryability-canceled-history-probe"
	ch, chClock, chNotifier := newTestWatcher(Beat{ID: canceledHistoryID, Deadline: 10 * time.Minute})
	ch.Beat(canceledHistoryID)
	chClock.Advance(11 * time.Minute)
	if !recordedBeat(ch, canceledHistoryID) {
		t.Fatalf("closing Beat(%s) = false", canceledHistoryID)
	}
	chNotifier.setFail(context.Canceled)
	canceledHistory := capture.Default(t)
	ch.sweep(context.Background())
	if got := canceledHistory.CountLevel(slog.LevelInfo, "outage history notification abandoned"); got != 1 {
		t.Fatalf("abandoned-history info lines = %d, want exactly 1: %v", got, canceledHistory.Messages())
	}
	if !canceledHistory.HasAttr("outage history notification abandoned", "retryable", "false") {
		t.Errorf("abandoned-history log does not report retryable=false: %v", canceledHistory.Records())
	}
}

func TestHistoryNoticeCountsOncePerMessageWhileOutagesCountEach(t *testing.T) {
	// Serial (no t.Parallel): it asserts deltas on the package-global
	// notification counters, which the parallel tests also move.
	const id = "history-counter-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	// Baseline, not zero: the registry is package-global and survives every
	// test in this binary, so `go test -count=N` re-enters this test with the
	// previous invocation's total. The cold-start zero matrix is pinned once
	// in internal/metrics (TestMintNotificationKindsPremintsEveryCounterAndKind);
	// what is unique here is the +3 delta.
	outagesBefore := beatCounterValue(t, "knell_beat_outages_total", id)

	// Three outages that all end before any of them can be reported.
	n.setFail(errors.New("discord down"))
	const outages = 3
	for range outages {
		clock.Advance(11 * time.Minute)
		if !recordedBeat(w, id) {
			t.Fatalf("Beat(%s) = false", id)
		}
	}
	if got, want := beatCounterValue(t, "knell_beat_outages_total", id), outagesBefore+outages; got != want {
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
	if got, want := beatCounterValue(t, "knell_beat_outages_total", id), outagesBefore+outages; got != want {
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
		if !recordedBeat(w, id) {
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

func TestLogUndeliveredClassifiesOnlyEndedRecordsAsPermanentLoss(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default. The shutdown log is the ONLY trace of a notice this process
	// will never deliver, and it must separate the two queue shapes: a
	// CLOSED record is a permanent loss (its outage already ended, so the
	// record was its last trace), while the OPEN tail loses nothing (the
	// boot-armed clock re-detects an ongoing outage after the restart).
	// Inverting that classification either pages for an outage that will be
	// re-detected or stays quiet about one that is gone for good.
	const id = "shutdown-loss-probe"
	w, clock, _ := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})

	// The first late ping creates an already-ended record. A second silence
	// then creates an ongoing outage behind it without delivering either.
	clock.Advance(11 * time.Minute)
	if !recordedBeat(w, id) {
		t.Fatalf("Beat(%s) = false", id)
	}
	clock.Advance(11 * time.Minute)
	w.collectDue()

	rec := capture.Default(t)
	w.logUndelivered()

	if !rec.HasAttr("watch loop stopped", "undelivered_records", "2") {
		t.Errorf("shutdown summary does not count both queued records: %v", rec.Records())
	}
	if !rec.HasAttr("watch loop stopped", "permanent_loss", "1") {
		t.Errorf("shutdown summary does not classify only the ended record as permanently lost: %v", rec.Records())
	}
	if got := rec.CountLevel(slog.LevelWarn, "shutting down with undelivered ended-outage records"); got != 1 {
		t.Errorf("ended-outage loss warnings = %d, want 1: %v", got, rec.Messages())
	}
	if !rec.HasAttr("shutting down with undelivered ended-outage records", "records", "1") {
		t.Errorf("loss warning does not count the one ended record: %v", rec.Records())
	}
	if !rec.HasAttr("shutting down with undelivered ended-outage records", "still_ongoing", "1") {
		t.Errorf("loss warning does not distinguish the re-detectable ongoing outage: %v", rec.Records())
	}
	if !rec.HasAttr("shutting down with undelivered ended-outage records", "retryable", "false") {
		t.Errorf("loss warning does not report retryable=false for records that die with the process: %v", rec.Records())
	}
}

func TestLogUndeliveredReportsAnOngoingOutageWithoutTheLossWarning(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default. The mixed-queue test cannot see this branch -- its beat has an
	// ended record, so the WARN fires for it either way. Only a queue holding
	// nothing but the open tail pins both halves of the classification:
	// dropping the `p.lost == 0` guard pages the operator that "no notice will
	// ever arrive" for an outage that will in fact be re-detected (the false
	// alarm the split exists to avoid), while dropping the Info line hides the
	// beat whose notice arrives only if the outage outlives the post-restart
	// deadline -- the one shutdown loss that is conditional rather than certain.
	const id = "shutdown-ongoing-only-probe"
	w, clock, _ := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})

	// One silence, never closed by a ping and never delivered: the queue holds
	// exactly the open tail.
	clock.Advance(11 * time.Minute)
	w.collectDue()

	rec := capture.Default(t)
	w.logUndelivered()

	if !rec.HasAttr("watch loop stopped", "undelivered_records", "1") {
		t.Errorf("shutdown summary does not count the queued record: %v", rec.Records())
	}
	if !rec.HasAttr("watch loop stopped", "permanent_loss", "0") {
		t.Errorf("an ongoing outage must not be reported as permanently lost: %v", rec.Records())
	}
	if got := rec.CountLevel(slog.LevelWarn, "shutting down with undelivered ended-outage records"); got != 0 {
		t.Errorf("ended-outage loss warnings = %d, want 0 for an ongoing outage: %v", got, rec.Messages())
	}
	if !rec.HasAttr("watch loop stopped", "ongoing_records", "1") {
		t.Errorf("shutdown summary does not separate the ongoing record from the permanent losses: %v", rec.Records())
	}
	if got := rec.CountLevel(slog.LevelInfo, "shutting down with an ongoing outage"); got != 1 {
		t.Errorf("ongoing-outage notices = %d, want exactly 1 (its notice is conditional, not silent): %v", got, rec.Messages())
	}
	if !rec.HasAttr("shutting down with an ongoing outage", "beat", id) {
		t.Errorf("ongoing-outage notice does not name the beat: %v", rec.Records())
	}
}

func TestShutdownWarnsAboutQueuedRecoveredNotifications(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default. A queued recovered transition dies with the channel at
	// shutdown and no delivery counter can show it (dropped means "discarded
	// by a full queue"), so this WARN is the operator's only trace of it.
	// Both directions are asserted: an empty queue must stay quiet, or the
	// line becomes shutdown noise that trains operators to ignore it.
	const id = "shutdown-queued-recovery-probe"
	w, clock, _ := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})

	quiet := capture.Default(t)
	w.logUndelivered()
	if got := quiet.CountLevel(slog.LevelWarn, "shutting down with queued recovered notifications"); got != 0 {
		t.Fatalf("queued-recovery warnings with an empty queue = %d, want 0: %v", got, quiet.Messages())
	}
	if !quiet.HasAttr("watch loop stopped", "queued_recoveries", "0") {
		t.Errorf("shutdown summary does not report queued_recoveries=0 for an empty queue: %v", quiet.Records())
	}

	// Alert the beat, then ping it: the recovered transition is queued and
	// never drained, exactly as it sits when cancellation arrives.
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if !recordedBeat(w, id) {
		t.Fatalf("Beat(%s) = false", id)
	}
	if got := len(w.recoveries); got != 1 {
		t.Fatalf("queued recoveries = %d, want 1 (the test's own precondition)", got)
	}

	rec := capture.Default(t)
	w.logUndelivered()
	if !rec.HasAttr("watch loop stopped", "queued_recoveries", "1") {
		t.Errorf("shutdown summary does not count the queued recovery: %v", rec.Records())
	}
	if got := rec.CountLevel(slog.LevelWarn, "shutting down with queued recovered notifications"); got != 1 {
		t.Errorf("queued-recovery warnings = %d, want exactly 1 (that notice will never be delivered): %v", got, rec.Messages())
	}
	if !rec.HasAttr("shutting down with queued recovered notifications", "queued", "1") {
		t.Errorf("queued-recovery warning does not report how many notices are lost: %v", rec.Records())
	}
	if !rec.AttrContains("shutting down with queued recovered notifications", "beats", id) {
		t.Errorf("queued-recovery warning does not name the beat whose recovered notice is lost: %v", rec.Records())
	}
	if !rec.HasAttr("shutting down with queued recovered notifications", "retryable", "false") {
		t.Errorf("queued-recovery warning does not report retryable=false for notices that die with the channel: %v", rec.Records())
	}
}

func TestLogUndeliveredCountsAnUnqueuedOngoingOutage(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default. The open-overflow path deliberately keeps an ongoing outage
	// OUTSIDE pendingMissing (overflowAccounted alone), so a shutdown
	// snapshot derived only from the slice reports ongoing_records=0 for a
	// detected outage this process is abandoning -- and reports nothing at
	// all once a history drain has emptied the slice.
	const id = "shutdown-overflow-ongoing-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute})

	// Fill the queue with missingQueueSize already-ended records: each late
	// ping closes an outage and queues its record, none are delivered.
	n.setFail(context.Canceled)
	fillMissingQueue(t, w, clock, id)

	// A further silence is detected by the sweep with no slot free: the
	// outage lives in overflowAccounted only.
	clock.Advance(11 * time.Minute)
	w.collectDue()
	if !w.beats[id].overflowAccounted {
		t.Fatalf("overflowAccounted = false, want true (the test's own precondition)")
	}

	rec := capture.Default(t)
	w.logUndelivered()
	if !rec.HasAttr("watch loop stopped", "undelivered_records", "9") {
		t.Errorf("shutdown summary omits the unqueued ongoing outage: %v", rec.Records())
	}
	if !rec.HasAttr("watch loop stopped", "ongoing_records", "1") {
		t.Errorf("shutdown summary does not count the unqueued ongoing outage: %v", rec.Records())
	}
	if !rec.HasAttr("watch loop stopped", "permanent_loss", "8") {
		t.Errorf("shutdown summary miscounts the permanently lost ended records: %v", rec.Records())
	}
	if !rec.HasAttr("shutting down with undelivered ended-outage records", "still_ongoing", "1") {
		t.Errorf("per-beat loss warning omits the unqueued ongoing outage: %v", rec.Records())
	}

	// Same state after a successful history drain empties the slice: the
	// abandoned outage is then the ONLY undelivered work, and a
	// slice-derived snapshot would skip the beat entirely.
	n.setFail(nil)
	w.sweep(context.Background())
	if got := len(w.beats[id].pendingMissing); got != 0 {
		t.Fatalf("queued records after the history drain = %d, want 0", got)
	}
	if !w.beats[id].overflowAccounted {
		t.Fatalf("overflowAccounted = false after the drain, want the abandoned outage still tracked")
	}

	drained := capture.Default(t)
	w.logUndelivered()
	if !drained.HasAttr("watch loop stopped", "undelivered_records", "1") {
		t.Errorf("drained shutdown summary omits the unqueued ongoing outage: %v", drained.Records())
	}
	if !drained.HasAttr("watch loop stopped", "ongoing_records", "1") {
		t.Errorf("drained shutdown summary does not count the unqueued ongoing outage: %v", drained.Records())
	}
	if !drained.HasAttr("watch loop stopped", "permanent_loss", "0") {
		t.Errorf("an ongoing outage must not be reported as permanently lost: %v", drained.Records())
	}
	if got := drained.CountLevel(slog.LevelInfo, "shutting down with an ongoing outage"); got != 1 {
		t.Errorf("ongoing-outage notices = %d, want exactly 1: %v", got, drained.Messages())
	}
}

func TestBudgetCutIsLoggedOncePerSweepWithTheDeferredCount(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default, which every other test writes through.
	//
	// Cutting a sweep short is ordinary back-pressure while deliveries are
	// slow, not a fault, so it is levelled exactly like the other
	// back-pressure case (recordOngoingOutage's full-queue deferral) rather
	// than as a warning that would page on a delivery outage knell is already
	// reporting. One line per affected SWEEP, naming how many beats the next
	// sweep must pick up: one line per deferred beat would bury real faults
	// under 60 lines every 15s, and a line with no count leaves the operator
	// unable to tell a one-beat overrun from a stalled fleet.
	const (
		total    = 12
		perSend  = 2 * time.Second
		deadline = 10 * time.Minute
	)
	beats := budgetProbeBeats("budget-cut-log-probe", total, deadline)
	clock := newFakeClock()
	n := &fakeNotifier{}
	w := New(beats, n, clock.Now, clock.Now())

	clock.Advance(deadline + time.Minute)
	slowSends(n, clock, perSend)

	rec := capture.Default(t)
	w.sweep(context.Background())

	const msg = "sweep send budget spent"
	if got := rec.CountLevel(slog.LevelDebug, msg); got != 1 {
		t.Fatalf("budget-cut debug lines = %d, want exactly 1 for the sweep (not one per deferred beat): %v", got, rec.Messages())
	}
	for _, level := range []slog.Level{slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
		if got := rec.CountLevel(level, msg); got != 0 {
			t.Errorf("budget-cut lines at %s = %d, want 0 (back-pressure during a delivery outage is not a fault): %v", level, got, rec.Messages())
		}
	}
	// Hard-coded, NOT derived from sweepSendBudget: with the documented
	// five-second budget, three two-second sends start before the elapsed
	// clock is strictly past it, so nine of the twelve due beats are deferred.
	// Deriving this from the production constant would make the constant its
	// own oracle, and an accidental retune (five seconds to ten) would change
	// both sides and stay green while Run services queued recoveries later.
	const wantDeferred = 9
	if !rec.HasAttr(msg, "deferred_beats", strconv.Itoa(wantDeferred)) {
		t.Errorf("budget-cut line does not report the %d beats deferred to the next sweep: %v", wantDeferred, rec.Records())
	}
	if !rec.HasAttr(msg, "budget", sweepSendBudget.String()) {
		t.Errorf("budget-cut line does not name the budget it spent: %v", rec.Records())
	}

	// The line belongs to the cut, not to sweeping: once sends are quick
	// enough to finish the remaining beats, the sweep must stay silent.
	n.onMissing = nil
	quiet := capture.Default(t)
	w.sweep(context.Background())
	if got := quiet.Count(msg); got != 0 {
		t.Errorf("budget-cut lines in a sweep that delivered everything = %d, want 0: %v", got, quiet.Messages())
	}
}

func TestRunReportsUndeliveredWorkWhenTheContextIsCancelled(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default.
	//
	// The shutdown report is wiring, not just a function: every other test of
	// it calls logUndelivered directly, so deleting Run's call to it leaves
	// them all green while the operator loses the ONLY trace of a notice that
	// died with the process (this path has no delivery counter behind it).
	const id = "run-shutdown-report-probe"
	clock := newFakeClock()
	n := &fakeNotifier{}
	w := New([]Beat{{ID: id, Deadline: 10 * time.Minute}}, n, clock.Now, clock.Now())

	// One ended outage whose notice never got out: the webhook is down when
	// the sweep detects the crossing, so the record stays queued, and a late
	// ping seals it. That record is the outage's only trace.
	n.setFail(errors.New("discord down"))
	if !recordedBeat(w, id) {
		t.Fatalf("Beat(%s) = false", id)
	}
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if !recordedBeat(w, id) {
		t.Fatalf("late Beat(%s) = false", id)
	}
	if got := len(w.beats[id].pendingMissing); got != 1 {
		t.Fatalf("queued records before shutdown = %d, want 1 undelivered ended-outage record", got)
	}

	rec := capture.Default(t)
	// A tick far beyond this test's lifetime, so only the cancellation arm of
	// Run's select runs.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx, time.Hour)
	}()
	cancel()
	<-done

	if got := rec.CountLevel(slog.LevelWarn, "shutting down with undelivered ended-outage records"); got != 1 {
		t.Errorf("shutdown loss warnings = %d, want exactly 1: Run must report the notices dying with the process, which no counter records: %v",
			got, rec.Messages())
	}
	if !rec.HasAttr("watch loop stopped", "permanent_loss", "1") {
		t.Errorf("shutdown summary does not report the permanently lost record: %v", rec.Records())
	}
}
