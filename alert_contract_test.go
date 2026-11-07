package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/cplieger/go/slogx"
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
