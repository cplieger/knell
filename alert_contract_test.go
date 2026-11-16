package main

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cplieger/go/slogx"
	"github.com/cplieger/knell/internal/obs"
)

// logqlFilters returns the backticked line filters of one rule in
// alerts/logql.yaml, the text a shipped Loki rule matches in knell's log.
func logqlFilters(t *testing.T, alert string) []string {
	t.Helper()
	raw, err := os.ReadFile("alerts/logql.yaml")
	if err != nil {
		t.Fatalf("reading the shipped LogQL rules: %v", err)
	}
	_, rule, found := strings.Cut(string(raw), "- alert: "+alert+"\n")
	if !found {
		t.Fatalf("alerts/logql.yaml defines no %s rule", alert)
	}
	_, expr, found := strings.Cut(rule, "expr: |\n")
	if !found {
		t.Fatalf("%s has no block expr", alert)
	}
	expr, _, _ = strings.Cut(expr, "for:")
	var filters []string
	for _, m := range regexp.MustCompile("`([^`]+)`").FindAllStringSubmatch(expr, -1) {
		filters = append(filters, m[1])
	}
	if len(filters) == 0 {
		t.Fatalf("%s has no line filter to bind", alert)
	}
	return filters
}

// lineWith returns the first line of out containing marker.
func lineWith(out, marker string) (string, bool) {
	for line := range strings.Lines(out) {
		if strings.Contains(line, marker) {
			return line, true
		}
	}
	return "", false
}

// TestKnellExitedWithErrorMatchesEveryExitLine binds KnellExitedWithError to
// the line knell writes on each way it exits with an error: the rule has no
// series to fall back on, so a reworded line silences it.
func TestKnellExitedWithErrorMatchesEveryExitLine(t *testing.T) {
	filters := logqlFilters(t, "KnellExitedWithError")
	cases := map[string]struct {
		env  []string
		args []string
	}{
		"rejected configuration": {env: []string{
			"BEATS=api:20m",
			"DISCORD_WEBHOOK_URL=" + testPlainHTTPWebhookURL,
			"DISCORD_WEBHOOK_URL_FILE=",
		}},
		"unknown command": {args: []string{"serve"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, out := runMain(t, tc.env, tc.args...)
			line, ok := lineWith(out, "knell exited with error")
			if !ok {
				t.Fatalf("no exit line in the output:\n%s", out)
			}
			for _, f := range filters {
				if !strings.Contains(line, f) {
					t.Errorf("exit line %q lacks the rule's filter %q", line, f)
				}
			}
		})
	}
}

// TestKnellNoticeLostForGoodMatchesALostNoticeLine binds KnellNoticeLostForGood
// to a notice-loss line as knell renders it: the loss happens while the process
// stops, so no scrape ever sees a counter it moves.
func TestKnellNoticeLostForGoodMatchesALostNoticeLine(t *testing.T) {
	// Serial (no t.Parallel): swaps the process-global slog default.
	filters := logqlFilters(t, "KnellNoticeLostForGood")
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	var buf bytes.Buffer
	slogx.Setup(slogx.Options{Output: &buf})

	expired, cancel := context.WithCancel(t.Context())
	cancel()
	if awaitWatchLoop(expired, make(chan struct{})) {
		t.Fatal("awaitWatchLoop reported a never-finishing loop as stopped")
	}

	line, ok := lineWith(buf.String(), "watch loop still running")
	if !ok {
		t.Fatalf("no abandoned-loop line in the log:\n%s", buf.String())
	}
	for _, f := range filters {
		if !strings.Contains(line, f) {
			t.Errorf("notice-loss line %q lacks the rule's filter %q", line, f)
		}
	}
}

// TestQuorumFixtureIsTheCommentedRule keeps testdata/promql_quorum.yaml, which
// alerts/promql_test.yaml tests, equal to the commented KnellBeatQuorumLow a
// user uncomments in alerts/promql.yaml.
func TestQuorumFixtureIsTheCommentedRule(t *testing.T) {
	shipped, err := os.ReadFile("alerts/promql.yaml")
	if err != nil {
		t.Fatalf("reading the shipped PromQL rules: %v", err)
	}
	fixture, err := os.ReadFile("testdata/promql_quorum.yaml")
	if err != nil {
		t.Fatalf("reading the quorum fixture: %v", err)
	}
	const start = "# - alert: KnellBeatQuorumLow"
	var commented []string
	in := false
	for line := range strings.Lines(string(shipped)) {
		trimmed := strings.TrimSpace(line)
		in = in || strings.HasPrefix(trimmed, start)
		if !in {
			continue
		}
		if !strings.HasPrefix(trimmed, "#") {
			break
		}
		commented = append(commented, strings.Replace(strings.TrimRight(line, "\n"), "# ", "", 1))
	}
	if len(commented) == 0 {
		t.Fatal("alerts/promql.yaml has no commented KnellBeatQuorumLow rule")
	}
	_, rule, found := strings.Cut(string(fixture), "      - alert: KnellBeatQuorumLow")
	if !found {
		t.Fatal("testdata/promql_quorum.yaml has no KnellBeatQuorumLow rule")
	}
	want := strings.TrimRight("      - alert: KnellBeatQuorumLow"+rule, "\n")
	if got := strings.Join(commented, "\n"); got != want {
		t.Errorf("uncommented rule in alerts/promql.yaml:\n%s\nwant the tested fixture:\n%s", got, want)
	}
}

// TestPromQLRulesReadServedSeries binds every shipped PromQL rule to what
// knell serves: each knell_* family a rule reads is on /metrics, and each
// reason KnellAuthFailures names is a reason knell_auth_failures_total
// carries. A renamed family or label value would otherwise leave a rule that
// can never fire with every test green.
func TestPromQLRulesReadServedSeries(t *testing.T) {
	raw, err := os.ReadFile("alerts/promql.yaml")
	if err != nil {
		t.Fatalf("reading the shipped PromQL rules: %v", err)
	}
	// Per-beat and build families show once a beat and a build exist.
	obs.InitBeat("alert-contract", time.Minute, time.Unix(0, 0))
	obs.SetBeatFresh("alert-contract", true)
	obs.SetBuildInfo("alert-contract")
	rec := httptest.NewRecorder()
	obs.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	page := rec.Body.String()
	for _, family := range regexp.MustCompile(`\bknell_[a-z_]+`).FindAllString(string(raw), -1) {
		if !strings.Contains(page, "# TYPE "+family+" ") {
			t.Errorf("alerts/promql.yaml reads %s, which /metrics does not serve", family)
		}
	}
	for _, m := range regexp.MustCompile(`knell_auth_failures_total\{reason=~?"([^"]+)"\}`).FindAllStringSubmatch(string(raw), -1) {
		for reason := range strings.SplitSeq(m[1], "|") {
			if !strings.Contains(page, `knell_auth_failures_total{reason="`+reason+`"}`) {
				t.Errorf("a rule reads reason %q, which knell_auth_failures_total does not carry", reason)
			}
		}
	}
}
