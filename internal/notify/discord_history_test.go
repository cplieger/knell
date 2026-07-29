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
