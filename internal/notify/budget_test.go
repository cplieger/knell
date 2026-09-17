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

// TestEveryNoticeStaysInsideDiscordsPayloadLimits renders every notice shape at
// its worst case (max node name, max beat id, a multi-year silence, a full
// history batch, and each branch's longest wording) and measures the result
// against every limit Discord documents, so a wording change that busts one
// fails here rather than silently invalidating MaxNodeNameBytes (an over-limit
// payload is answered 400 and never delivered).
func TestEveryNoticeStaysInsideDiscordsPayloadLimits(t *testing.T) {
	rec := newWebhookRecorder(http.StatusNoContent)
	srv := httptest.NewServer(rec.handler(t))
	defer srv.Close()

	// "*" fillers so markdown escaping (which doubles each character) is
	// inside the measured worst case.
	d := newTestNotifier(t, srv.URL, strings.Repeat("*", MaxNodeNameBytes))
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
			payload := <-rec.payloads

			if got := len(payload.Embeds); got > limitEmbeds {
				t.Errorf("the %s notice carries %d embeds at the worst case, want at most Discord's %d: either shorten the template or lower MaxNodeNameBytes, because Discord answers 400 for an over-limit payload and the notice is never delivered",
					name, got, limitEmbeds)
			}
			for i, e := range payload.Embeds {
				if got := len(e.Fields); got > limitFields {
					t.Errorf("the %s notice renders %d fields in embeds.%d at the worst case, want at most Discord's %d: either shorten the template or lower MaxNodeNameBytes, because Discord answers 400 for an over-limit payload and the notice is never delivered",
						name, got, i, limitFields)
				}
			}
			var combined int
			for _, slot := range measureSlots(payload) {
				if slot.counted {
					combined += slot.runes
				}
				if slot.runes > slot.limit {
					t.Errorf("the %s notice renders %d characters in %s at the worst case, want at most Discord's %d: either shorten the template or lower MaxNodeNameBytes, because Discord answers 400 for an over-limit payload and the notice is never delivered",
						name, slot.runes, slot.slot, slot.limit)
				}
			}
			if combined > limitCombined {
				t.Errorf("the %s notice renders %d characters across every embed slot at the worst case, want at most Discord's %d-character combined embed budget: either shorten the template or lower MaxNodeNameBytes, because Discord answers 400 for an over-limit payload and the notice is never delivered",
					name, combined, limitCombined)
			}
		})
	}
}
