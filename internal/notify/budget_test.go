package notify

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/knell/internal/watch"
)

// TestEveryNoticeStaysInsideDiscordsContentLimit makes the derivation behind
// MaxNodeNameBytes structural instead of a prose comment: it
// renders every shape this package produces at its worst case — the largest
// accepted node name, the longest beat id config's grammar admits, a silence
// long enough to render a maximal duration, and the LONGEST late clause of each
// branch — and asserts the result fits Discord's `content` limit. A wording
// change that eats the budget now fails in the package that made the edit,
// rather than silently invalidating the cap internal/config enforces (an
// over-limit notice is answered 400, so knell would arm, detect outages and
// never ring).
func TestEveryNoticeStaysInsideDiscordsContentLimit(t *testing.T) {
	// Discord's hard limit on a webhook message's `content` field. Owned by
	// this test, the only place that measures against it: an over-limit
	// content is answered 400 and the notice is never delivered.
	const discordContentLimit = 2000

	rec := newWebhookRecorder(http.StatusNoContent)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()

	// Every interpolated field at its maximum: the node name at the cap, a
	// 64-byte beat id (config's beatIDPattern ceiling) and a 200-year silence,
	// long enough to exercise a wide multi-year duration. The assertion below
	// measures the final rendered message directly rather than duplicating its
	// template budget in prose.
	d := New(srv.URL, strings.Repeat("n", MaxNodeNameBytes))
	defer d.Close()
	id := strings.Repeat("b", 64)
	started := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	observed := started.Add(200 * 365 * 24 * time.Hour)
	live := watch.Transition{Started: started, Observed: observed}

	// The single-outage notice carries the longest of the two lateClause
	// branches; the batch carries the MIXED batchLateClause, the longest of the
	// four late clauses, at watch's missingQueueSize bound of 8 records.
	single := []watch.Outage{{Started: started, Recovered: observed, LateReason: watch.LateEndedBeforeDetection}}
	mixed := make([]watch.Outage, 0, 8)
	for i := range 8 {
		reason := watch.LateUndelivered
		if i%2 == 0 {
			reason = watch.LateEndedBeforeDetection
		}
		mixed = append(mixed, watch.Outage{Started: started, Recovered: observed, LateReason: reason})
	}

	cases := map[string]func() error{
		"missing":         func() error { return d.BeatMissing(t.Context(), id, live) },
		"recovered":       func() error { return d.BeatRecovered(t.Context(), id, live) },
		"history one":     func() error { return d.BeatOutageHistory(t.Context(), id, single) },
		"history several": func() error { return d.BeatOutageHistory(t.Context(), id, mixed) },
	}
	for name, send := range cases {
		t.Run(name, func(t *testing.T) {
			if err := send(); err != nil {
				t.Fatalf("sending the %s notice: %v", name, err)
			}
			content := <-rec.contents
			if runes := len([]rune(content)); runes >= discordContentLimit {
				t.Errorf("the %s notice renders %d characters at the worst case, want under Discord's %d-character content limit: either shorten the template or lower MaxNodeNameBytes, because Discord answers 400 for an over-limit content and the notice is never delivered",
					name, runes, discordContentLimit)
			}
		})
	}
}
