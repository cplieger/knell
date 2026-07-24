package watch

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/knell/internal/config"
	"github.com/cplieger/knell/internal/metrics"
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
	w, clock, _ := newTestWatcher(config.Beat{ID: id, Deadline: 10 * time.Minute})

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
	w, clock, n := newTestWatcher(config.Beat{ID: id, Deadline: 10 * time.Minute})
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
	w, clock, n := newTestWatcher(config.Beat{ID: id, Deadline: 10 * time.Minute})

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

func TestRefreshFreshnessUpdatesGaugeWithoutNotifying(t *testing.T) {
	t.Parallel()

	const id = "refresh-probe"
	w, clock, n := newTestWatcher(config.Beat{ID: id, Deadline: 10 * time.Minute})

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

func TestDeliveredNotificationsIncrementSentCounters(t *testing.T) {
	// Serial (no t.Parallel): asserts deltas on the package-global sent
	// counters, which the parallel tests also increment.
	const id = "sent-counter-probe"
	w, clock, _ := newTestWatcher(config.Beat{ID: id, Deadline: 10 * time.Minute})
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
	w, clock, n := newTestWatcher(config.Beat{ID: id, Deadline: 10 * time.Minute})
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
