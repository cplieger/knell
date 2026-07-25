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

// labeledValue scrapes the metrics exposition and returns the value token
// of name{label="<value>"}, failing the test when the series is absent.
func labeledValue(t *testing.T, name, label, value string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	metrics.Registry.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	prefix := name + `{` + label + `="` + value + `"} `
	for line := range strings.Lines(rec.Body.String()) {
		if v, ok := strings.CutPrefix(line, prefix); ok {
			return strings.TrimSpace(v)
		}
	}
	t.Fatalf("series %s{%s=%q} not in exposition", name, label, value)
	return ""
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

	if got := rec.CountLevel(slog.LevelWarn, "pending missing queue full"); got != 1 {
		t.Fatalf("queue-full warnings = %d, want exactly 1 (a dropped outage must name itself in the log, not only in a counter): %v", got, rec.Messages())
	}
	if !rec.HasAttr("pending missing queue full", "beat", id) {
		t.Errorf("drop warning does not name the beat: %v", rec.Records())
	}
	if !rec.HasAttr("pending missing queue full", "silence", "47m0s") {
		t.Errorf("drop warning does not report the dropped outage's silence: %v", rec.Records())
	}
}

func TestQueueFullOverflowIsAccountedOncePerAffectedOutage(t *testing.T) {
	// Serial (no t.Parallel): asserts deltas on the package-global failed
	// counter and captures the process-global slog default. An affected
	// outage must be accounted ONCE, not once per 15s sweep: every sweep
	// re-detects the same still-unqueued crossing, and re-counting it would
	// inflate the series KnellNotifyFailing alerts on for a single outage.
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
	historyBefore := counterValue(t, "knell_notifications_failed_total", "history")
	outagesBefore := beatCounterValue(t, "knell_beat_outages_total", id)
	rec := capture.Default(t)
	const sweeps = 3
	for range sweeps {
		w.sweep(context.Background())
	}
	// The queue head is an ended outage, so every sweep's failed send is a
	// history message: one failure per message, on the history kind. The
	// missing kind moves exactly once, for the queue-full overflow.
	if got, want := counterValue(t, "knell_notifications_failed_total", "missing"), failedBefore+1; got != want {
		t.Errorf("failed{missing} = %v after %d sweeps with a full queue, want %v (the overflow, accounted once)", got, sweeps, want)
	}
	if got, want := counterValue(t, "knell_notifications_failed_total", "history"), historyBefore+sweeps; got != want {
		t.Errorf("failed{history} = %v after %d failed history sends, want %v (one per failed message)", got, sweeps, want)
	}
	if got, want := beatCounterValue(t, "knell_beat_outages_total", id), outagesBefore+1; got != want {
		t.Errorf("beat_outages_total = %v after %d re-detections of one outage, want %v (one increment per outage, even when its notice was dropped)", got, sweeps, want)
	}
	if got := rec.CountLevel(slog.LevelWarn, "pending missing queue full"); got != 1 {
		t.Errorf("queue-full warnings = %d over %d sweeps, want exactly 1 per affected outage: %v", got, sweeps, rec.Messages())
	}

	// The ping that ends that outage closes the same record the sweeps
	// already accounted, so it must not count a second time.
	failedBefore = counterValue(t, "knell_notifications_failed_total", "missing")
	outagesBefore = beatCounterValue(t, "knell_beat_outages_total", id)
	clock.Advance(11 * time.Minute)
	if !w.Beat(id) {
		t.Fatalf("closing Beat(%s) = false", id)
	}
	if got := counterValue(t, "knell_notifications_failed_total", "missing"); got != failedBefore {
		t.Errorf("failed{missing} = %v after the ping closing an already-accounted outage, want unchanged %v", got, failedBefore)
	}
	if got := beatCounterValue(t, "knell_beat_outages_total", id); got != outagesBefore {
		t.Errorf("beat_outages_total = %v after the ping closing an already-counted outage, want unchanged %v", got, outagesBefore)
	}

	// A genuinely NEW outage is accounted on its own: the accounting mark is
	// per outage, not a permanent mute.
	failedBefore = counterValue(t, "knell_notifications_failed_total", "missing")
	outagesBefore = beatCounterValue(t, "knell_beat_outages_total", id)
	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if got, want := counterValue(t, "knell_notifications_failed_total", "missing"), failedBefore+1; got != want {
		t.Errorf("failed{missing} = %v after a new affected outage, want %v (a fresh overflow)", got, want)
	}
	if got, want := beatCounterValue(t, "knell_beat_outages_total", id), outagesBefore+1; got != want {
		t.Errorf("beat_outages_total = %v after a new outage, want %v", got, want)
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
