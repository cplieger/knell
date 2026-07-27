package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestMintNotificationKindsPremintsEveryCounterAndKind pins the whole
// cold-start matrix: three notification counters times three kinds, each born
// at zero. Without a zero sample an increase() alert misses the very first
// failure, drop or delivery, so a counter or kind missing from the minting
// loop is an alert that stays silent through the first event it exists for.
func TestMintNotificationKindsPremintsEveryCounterAndKind(t *testing.T) {
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

	// Asserted BEFORE anything in this test touches the counters: this is the
	// real cold start, so it pins the init() call itself. Deleting
	// mintNotificationKinds() from init() leaves every series absent here,
	// while the explicit re-mint below would still pass.
	assertNotificationSeries(t, want, "at cold start")

	kinds := []string{KindMissing, KindRecovered, KindHistory}
	for _, kind := range kinds {
		NotificationsSent.Delete(kind)
		NotificationsFailed.Delete(kind)
		NotificationsDropped.Delete(kind)
	}

	mintNotificationKinds()

	assertNotificationSeries(t, want, "after an explicit re-mint")
}

// assertNotificationSeries scrapes the registry and requires every wanted
// exposition line to be present.
func assertNotificationSeries(t *testing.T, want []string, when string) {
	t.Helper()
	rec := httptest.NewRecorder()
	Registry.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	exposition := rec.Body.String()
	for _, line := range want {
		if !strings.Contains(exposition, line+"\n") {
			t.Errorf("/metrics %s is missing the cold-start series %q", when, line)
		}
	}
}
