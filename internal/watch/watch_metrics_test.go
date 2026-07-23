package watch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/knell/internal/config"
	"github.com/cplieger/knell/internal/metrics"
)

// beatGauge scrapes the metrics exposition and returns the value token of
// name{beat="<beat>"}, failing the test when the series is absent.
func beatGauge(t *testing.T, name, beat string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	metrics.Registry.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	prefix := name + `{beat="` + beat + `"} `
	for line := range strings.Lines(rec.Body.String()) {
		if value, ok := strings.CutPrefix(line, prefix); ok {
			return strings.TrimSpace(value)
		}
	}
	t.Fatalf("series %s{beat=%q} not in exposition", name, beat)
	return ""
}

func TestBeatFreshGaugeTracksOverdueAndRecovery(t *testing.T) {
	t.Parallel()

	// Unique beat id: the metric registry is package-global, so a label
	// value no other test uses keeps this test's series isolated even
	// under t.Parallel.
	const id = "metrics-quorum-probe"
	w, clock, _ := newTestWatcher(config.Beat{ID: id, Deadline: 10 * time.Minute})

	if got := beatGauge(t, "knell_beat_fresh", id); got != "1" {
		t.Fatalf("beat_fresh at boot = %s, want 1", got)
	}
	bootSeen := beatGauge(t, "knell_beat_last_seen_timestamp_seconds", id)

	clock.Advance(11 * time.Minute)
	w.sweep(context.Background())
	if got := beatGauge(t, "knell_beat_fresh", id); got != "0" {
		t.Fatalf("beat_fresh when overdue = %s, want 0", got)
	}

	if !w.Beat(id) {
		t.Fatal("Beat returned false for configured id")
	}
	if got := beatGauge(t, "knell_beat_fresh", id); got != "1" {
		t.Fatalf("beat_fresh after ping = %s, want 1", got)
	}
	if got := beatGauge(t, "knell_beat_last_seen_timestamp_seconds", id); got == bootSeen {
		t.Errorf("beat_last_seen after ping = %s, still the boot baseline", got)
	}
}

// kindCounter scrapes the metrics exposition and returns the value token of
// name{kind="<kind>"}, failing the test when the series is absent.
func kindCounter(t *testing.T, name, kind string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	metrics.Registry.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	prefix := name + `{kind="` + kind + `"} `
	for line := range strings.Lines(rec.Body.String()) {
		if value, ok := strings.CutPrefix(line, prefix); ok {
			return strings.TrimSpace(value)
		}
	}
	t.Fatalf("series %s{kind=%q} not in exposition", name, kind)
	return ""
}

func TestCanceledNotificationsAreNotCountedAsFailed(t *testing.T) {
	// Serial (no t.Parallel): it asserts deltas on the package-global
	// failure counters, which the parallel tests also increment.
	const id = "cancel-probe"
	w, clock, n := newTestWatcher(config.Beat{ID: id, Deadline: 10 * time.Minute})
	w.Beat(id)
	clock.Advance(11 * time.Minute)

	failedBefore := kindCounter(t, "knell_notifications_failed_total", "missing")
	n.setFail(context.Canceled)
	w.sweep(context.Background())
	if got := kindCounter(t, "knell_notifications_failed_total", "missing"); got != failedBefore {
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
	failedBefore = kindCounter(t, "knell_notifications_failed_total", "recovered")
	n.setFail(context.Canceled)
	drainRecoveries(w)
	if got := kindCounter(t, "knell_notifications_failed_total", "recovered"); got != failedBefore {
		t.Errorf("failed{recovered} = %s after canceled send, want unchanged %s", got, failedBefore)
	}
}
