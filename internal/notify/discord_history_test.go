package notify

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/knell/internal/watch"
)

func TestBeatOutageHistoryRejectsInvalidRecoveryPoints(t *testing.T) {
	t.Parallel()

	started := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	cases := map[string]watch.Outage{
		"missing recovery point": {Started: started},
		"recovery before start":  {Started: started, Recovered: started.Add(-time.Second)},
	}
	for name, outage := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			rec := newWebhookRecorder(http.StatusNoContent)
			srv := httptest.NewServer(rec.handler(t))
			t.Cleanup(srv.Close)

			d := New(srv.URL, "node-1")
			t.Cleanup(d.Close)

			err := d.BeatOutageHistory(context.Background(), "api", []watch.Outage{outage})
			if err == nil {
				t.Fatal("BeatOutageHistory with an invalid recovery point = nil, want error")
			}
			if !strings.Contains(err.Error(), "outage 1 of 1 has no recovery point at or after its start") {
				t.Errorf("BeatOutageHistory error = %q, want the invalid recovery point identified", err)
			}
			if got := rec.hits.Load(); got != 0 {
				t.Errorf("webhook hits = %d, want 0 (an unresolved or negative outage must not be announced as recovered)", got)
			}
		})
	}
}

// TestBeatOutageHistoryRefusesRecordsItCannotReportTruthfully covers the two
// remaining clauses of BeatOutageHistory's contract: a record with no start,
// whose silence cannot be measured, and a batch whose recovery points do not
// ascend, where historyMessage reads "last recovered at" off the final entry
// and would publish a stale instant. Both guards keep a notice from stating
// something false about an incident that is already over, which is the whole
// reason the history path exists, and neither is reachable from watch today -
// so nothing but this test stands between a producer change and a published
// lie.
func TestBeatOutageHistoryRefusesRecordsItCannotReportTruthfully(t *testing.T) {
	t.Parallel()

	started := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	cases := map[string]struct {
		outages []watch.Outage
		want    string
	}{
		// Ended() holds (the recovery point is set and not before the zero
		// Started), so this record passes the recovery-point guard and only
		// the start guard stops it. Rendered, it reads "was missing for
		// 17752008h0m0s".
		"no start to measure the silence from": {
			outages: []watch.Outage{{Recovered: started, LateReason: watch.LateUndelivered}},
			want:    "outage 1 of 1 has no start",
		},
		// Both records are individually well-formed; only their ORDER is
		// wrong. Rendered, the batch would report the earlier recovery as the
		// most recent one, sending an operator to the wrong window.
		"recovery points that do not ascend": {
			outages: []watch.Outage{
				{Started: started, Recovered: started.Add(time.Hour)},
				{Started: started.Add(10 * time.Minute), Recovered: started.Add(30 * time.Minute)},
			},
			want: "outage 2 of 2 recovered before outage 1",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			rec := newWebhookRecorder(http.StatusNoContent)
			srv := httptest.NewServer(rec.handler(t))
			t.Cleanup(srv.Close)

			d := New(srv.URL, "node-1")
			t.Cleanup(d.Close)

			err := d.BeatOutageHistory(context.Background(), "api", tc.outages)
			if err == nil {
				t.Fatalf("BeatOutageHistory with %s = nil, want error", name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("BeatOutageHistory error = %q, want it to identify %q", err, tc.want)
			}
			if got := rec.hits.Load(); got != 0 {
				t.Errorf("webhook hits = %d, want 0 (a record the notice cannot describe truthfully must not be published)", got)
			}
		})
	}
}
