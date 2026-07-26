package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cplieger/knell/internal/metrics"
)

// TestMintNotificationKindsPremintsEveryCounterAndKind pins the whole
// cold-start matrix: three notification counters times three kinds, each born
// at zero. Without a zero sample an increase() alert misses the very first
// failure, drop or delivery, so a counter or kind missing from the minting
// loop is an alert that stays silent through the first event it exists for.
func TestMintNotificationKindsPremintsEveryCounterAndKind(t *testing.T) {
	kinds := []string{metrics.KindMissing, metrics.KindRecovered, metrics.KindHistory}
	for _, kind := range kinds {
		metrics.NotificationsSent.Delete(kind)
		metrics.NotificationsFailed.Delete(kind)
		metrics.NotificationsDropped.Delete(kind)
	}

	metrics.MintNotificationKinds()

	rec := httptest.NewRecorder()
	metrics.Registry.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	exposition := rec.Body.String()
	want := []string{
		`knell_notifications_sent_total{kind="missing"} 0`,
		`knell_notifications_sent_total{kind="recovered"} 0`,
		`knell_notifications_sent_total{kind="history"} 0`,
		`knell_notifications_failed_total{kind="missing"} 0`,
		`knell_notifications_failed_total{kind="recovered"} 0`,
		`knell_notifications_failed_total{kind="history"} 0`,
		`knell_notifications_dropped_total{kind="missing"} 0`,
		`knell_notifications_dropped_total{kind="recovered"} 0`,
		`knell_notifications_dropped_total{kind="history"} 0`,
	}
	for _, line := range want {
		if !strings.Contains(exposition, line+"\n") {
			t.Errorf("/metrics missing cold-start series %q", line)
		}
	}
}
