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

	// Every interpolated field at its maximum, with MARKUP-heavy fillers so the
	// escapeMarkdown expansion is inside the measured worst case: escaping
	// doubles every markup character, so the widest rendered node is the
	// 256-byte cap of "*" (512 runes) and the widest id config's grammar admits
	// is derived from config.MaxBeatIDLen. Plain letters would measure a notice
	// hundreds of characters shorter than the one the cap must cover. The
	// silence is a 200-year span, long enough to exercise a wide multi-year
	// duration. The assertion below measures the final rendered message directly
	// rather than duplicating its template budget in prose.
	d := New(srv.URL, strings.Repeat("*", MaxNodeNameBytes))
	defer d.Close()
	id := "b" + strings.Repeat("_", config.MaxBeatIDLen-1)
	started := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	observed := started.Add(200 * 365 * 24 * time.Hour)
	live := watch.Transition{Started: started, Observed: observed}

	// Both fixtures carry the LONGEST clause of their shape, measured rather than
	// presumed: lateClause's nothing-attempted branch is 105 characters against 69
	// for the refused-delivery branch, and batchLateClause's none-attempted
	// whole-batch sentence is 86 against 72 for the mixed count at watch's
	// MaxHistoryBatch bound and 58 for all-delayed. So the single record and every
	// record of the widest batch are unattempted. The mixed batch is kept as a
	// second case because it is the only shape that renders COUNTS, whose digit
	// width grows if MaxHistoryBatch ever passes 9 — a whole-batch sentence never
	// carries a count of itself.
	single := []watch.Outage{{Started: started, Recovered: observed}}
	unattempted := make([]watch.Outage, 0, watch.MaxHistoryBatch)
	for range watch.MaxHistoryBatch {
		unattempted = append(unattempted, watch.Outage{Started: started, Recovered: observed})
	}
	// The mixed batch is the widest count sentence: one refused and the rest not,
	// so both counts render at their maximum digit width for this bound.
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
			if runes := len([]rune(content)); runes >= discordContentLimit {
				t.Errorf("the %s notice renders %d characters at the worst case, want under Discord's %d-character content limit: either shorten the template or lower MaxNodeNameBytes, because Discord answers 400 for an over-limit content and the notice is never delivered",
					name, runes, discordContentLimit)
			}
		})
	}
}
