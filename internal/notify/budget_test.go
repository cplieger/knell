package notify

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/knell/internal/config"
	"github.com/cplieger/knell/internal/watch"
)

// TestEveryNoticeStaysInsideDiscordsContentLimit renders every notice shape at
// its worst case (max node name, max beat id, a multi-year silence, and each
// branch's longest wording) and asserts the result fits Discord's content
// limit, so a wording change that busts the budget fails here rather than
// silently invalidating MaxNodeNameBytes (an over-limit notice is answered 400
// and never delivered).
func TestEveryNoticeStaysInsideDiscordsContentLimit(t *testing.T) {
	// Discord's hard limit on a webhook message's content field.
	const discordContentLimit = 2000

	rec := newWebhookRecorder(http.StatusNoContent)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()

	// "*" fillers so markdown escaping (which doubles each character) is
	// inside the measured worst case.
	d := New(srv.URL, strings.Repeat("*", MaxNodeNameBytes))
	defer d.Close()
	id := "b" + strings.Repeat("_", config.MaxBeatIDLen-1)
	started := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	observed := started.Add(200 * 365 * 24 * time.Hour)
	live := watch.Transition{Started: started, Observed: observed}

	// Both fixtures use the unattempted branch: it is the longer wording in
	// both lateClause and batchLateClause (measured).
	single := []watch.Outage{{Started: started, Recovered: observed}}
	unattempted := make([]watch.Outage, 0, watch.MaxHistoryBatch)
	for range watch.MaxHistoryBatch {
		unattempted = append(unattempted, watch.Outage{Started: started, Recovered: observed})
	}
	// One refused among the rest unattempted: the only shape that renders
	// both counts, so both hit their max digit width for this bound.
	mixed := make([]watch.Outage, 0, watch.MaxHistoryBatch)
	for i := range watch.MaxHistoryBatch {
		mixed = append(mixed, watch.Outage{
			Started:     started,
			Recovered:   observed,
			Undelivered: i == 0,
		})
	}

	cases := map[string]func() error{
		"missing":               func() error { return d.BeatMissing(t.Context(), id, live) },
		"recovered":             func() error { return d.BeatRecovered(t.Context(), id, live) },
		"history one":           func() error { return d.BeatOutageHistory(t.Context(), id, single) },
		"history several":       func() error { return d.BeatOutageHistory(t.Context(), id, unattempted) },
		"history several mixed": func() error { return d.BeatOutageHistory(t.Context(), id, mixed) },
	}
	for name, send := range cases {
		t.Run(name, func(t *testing.T) {
			if err := send(); err != nil {
				t.Fatalf("sending the %s notice: %v", name, err)
			}
			content := <-rec.contents
			if runes := len([]rune(content)); runes > discordContentLimit {
				t.Errorf("the %s notice renders %d characters at the worst case, want at most Discord's %d-character content limit: either shorten the template or lower MaxNodeNameBytes, because Discord answers 400 for an over-limit content and the notice is never delivered",
					name, runes, discordContentLimit)
			}
		})
	}
}
