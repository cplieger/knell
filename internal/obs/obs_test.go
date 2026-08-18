package obs

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
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
	assertExpositionLines(t, want, "at cold start")

	// Three counters x every legal kind: keyed off notificationKinds so a new
	// kind fails the guard below until its want lines are added, rather than
	// silently dropping out of this test.
	if len(want) != 3*len(notificationKinds) {
		t.Fatalf("want has %d lines for %d kinds x 3 counters: add the new kind's cold-start lines", len(want), len(notificationKinds))
	}
	for _, kind := range notificationKinds {
		notificationsSent.Delete(string(kind))
		notificationsFailed.Delete(string(kind))
		notificationsDropped.Delete(string(kind))
	}

	mintNotificationKinds()

	assertExpositionLines(t, want, "after an explicit re-mint")
}

// TestMintRefusalReasonsPremintsEveryReason is the pre-route refusal counter's
// half of the same cold-start contract, and it is the assertion the counter
// exists for: its predecessor was a path label on the request counter, minted on
// its FIRST refusal, so increase() could never see the first event of the class
// and the separation bought nothing. A reason missing from the minting loop
// reproduces exactly that, and every other test in the tree asserts DELTAS, so
// nothing else would notice.
func TestMintRefusalReasonsPremintsEveryReason(t *testing.T) {
	want := []string{
		`knell_pre_route_refusals_total{reason="non_canonical_beat_path"} 0`,
		`knell_pre_route_refusals_total{reason="host_not_allowed"} 0`,
		`knell_pre_route_refusals_total{reason="auth_throttled"} 0`,
	}

	// Before this test touches the counter: the real cold start, so it pins
	// init()'s own mintRefusalReasons() call and not just the function.
	assertExpositionLines(t, want, "at cold start")

	// Keyed off refusalReasons so a new reason fails here until its cold-start
	// line is added, rather than silently dropping out of the contract.
	if len(want) != len(refusalReasons) {
		t.Fatalf("want has %d lines for %d refusal reasons: add the new reason's cold-start line", len(want), len(refusalReasons))
	}
	for _, reason := range refusalReasons {
		preRouteRefusals.Delete(string(reason))
	}

	mintRefusalReasons()

	assertExpositionLines(t, want, "after an explicit re-mint")
}

// TestRecordPreRouteRefusalMovesOnlyItsOwnReason pins the label wiring: each
// reason is its own series, so an operator can tell a sender pinging a malformed
// URL from a forgotten hostname from a guessing run. A helper that dropped the
// argument, or passed a constant, would leave every knell test green while the
// three causes collapsed into one number that answers none of the three
// questions.
func TestRecordPreRouteRefusalMovesOnlyItsOwnReason(t *testing.T) {
	// The registry is a package-level singleton that outlives one iteration, so
	// restore the cold-start zeros: without this, `go test -count=2` fails in
	// TestMintRefusalReasonsPremintsEveryReason above instead of pinning
	// production behavior.
	t.Cleanup(func() {
		for _, reason := range refusalReasons {
			preRouteRefusals.Delete(string(reason))
		}
		mintRefusalReasons()
	})
	for _, recorded := range refusalReasons {
		before := refusalReasonValues(t)

		RecordPreRouteRefusal(recorded)

		after := refusalReasonValues(t)
		for _, reason := range refusalReasons {
			want := before[reason]
			if reason == recorded {
				want++
			}
			if after[reason] != want {
				t.Errorf("recording %q: reason %q = %v, want %v", recorded, reason, after[reason], want)
			}
		}
	}
}

// refusalReasonValues reads every refusal reason's current sample, so a test can
// assert that one reason moved and the others did not.
func refusalReasonValues(t *testing.T) map[Refusal]float64 {
	t.Helper()
	values := make(map[Refusal]float64, len(refusalReasons))
	for _, reason := range refusalReasons {
		rendered, ok := rawSeriesValue(t, `knell_pre_route_refusals_total{reason="`+string(reason)+`"} `)
		if !ok {
			t.Fatalf("knell_pre_route_refusals_total{reason=%q} is absent: the pre-minting contract is broken, so increase() misses the first refusal of that reason", reason)
		}
		value, err := strconv.ParseFloat(rendered, 64)
		if err != nil {
			t.Fatalf("parsing the %q sample %q: %v", reason, rendered, err)
		}
		values[reason] = value
	}
	return values
}

// assertExpositionLines checks that every want line is present in the rendered
// exposition verbatim. Shared by the two cold-start tests, so both read the
// series the same way an alert's selector does: off the exposition text.
func assertExpositionLines(t *testing.T, want []string, when string) {
	t.Helper()
	body := exposition(t)
	for _, line := range want {
		if !strings.Contains(body, line+"\n") {
			t.Errorf("/metrics %s is missing the cold-start series %q", when, line)
		}
	}
}

// exposition scrapes /metrics once and returns the rendered body. Every
// assertion in this file reads the real handler output rather than a collector,
// because what the alerts consume is the exposition text.
func exposition(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

// TestInitBeatMintsEveryColdStartSeriesForAConfiguredBeat pins the per-beat
// half of the cold-start guarantee: declaring a beat must publish all six
// per-beat series, with the three counters born at zero, the last-seen gauge
// carrying the boot baseline and the deadline gauge carrying the configured
// deadline. Five of the six come from InitBeat; knell_beat_fresh comes from
// SetBeatFresh, the single door for the freshness verdict, because the
// boundary belongs to the watch state machine (watch.New publishes the boot
// verdict right after InitBeat, and internal/watch's own tests pin that it is
// 1 at boot). A counter whose first exposed sample is already nonzero has no
// earlier sample for increase() to diff against, so a series dropped
// from the declaration path is an alert that stays silent through the first
// event of the beat's life; a missing last-seen baseline leaves the operator
// no window to reconstruct after a dropped notice.
func TestInitBeatMintsEveryColdStartSeriesForAConfiguredBeat(t *testing.T) {
	// The registry is a package-level singleton shared by every test in this
	// binary, so the probe id must be unique to this test for the values below
	// to be the cold-start ones.
	const id = "init-beat-cold-start-probe"
	series := []string{
		"knell_beat_fresh",
		"knell_beat_last_seen_timestamp_seconds",
		"knell_beat_deadline_seconds",
		"knell_beats_received_total",
		"knell_beat_outages_total",
		"knell_outage_records_dropped_total",
	}
	// The registry outlives one test iteration, so leaving the probe series
	// behind makes `go test -count=2` fail on the precondition below instead
	// of pinning production behavior. Registered BEFORE the preconditions so
	// even a failing iteration cleans up for the next one.
	t.Cleanup(func() {
		beatFresh.Delete(id)
		beatLastSeen.Delete(id)
		beatDeadline.Delete(id)
		beatsReceived.Delete(id)
		beatOutages.Delete(id)
		outageRecordsDropped.Delete(id)
	})
	for _, name := range series {
		if got, ok := beatSeriesValue(t, name, id); ok {
			t.Fatalf("%s{beat=%q} = %s before InitBeat: the probe id is not unique to this test, so it cannot pin cold-start values", name, id, got)
		}
	}

	InitBeat(id, 20*time.Minute, time.Unix(1700000000, 0))
	// The boot freshness verdict is the caller's to publish, exactly as
	// watch.New does it.
	SetBeatFresh(id, true)

	want := map[string]string{
		"knell_beat_fresh":                       "1",
		"knell_beat_last_seen_timestamp_seconds": "1700000000",
		"knell_beat_deadline_seconds":            "1200",
		"knell_beats_received_total":             "0",
		"knell_beat_outages_total":               "0",
		"knell_outage_records_dropped_total":     "0",
	}
	for _, name := range series {
		got, ok := beatSeriesValue(t, name, id)
		if !ok {
			t.Errorf("%s{beat=%q} is absent after the beat is declared: an increase() alert on it has no cold-start sample and misses the first event of the beat's life", name, id)
			continue
		}
		if got != want[name] {
			t.Errorf("%s{beat=%q} = %s, want %s", name, id, got, want[name])
		}
	}
}

// beatSeriesValue returns the rendered value of name{beat=id}. It is
// rawSeriesValue with the beat-label prefix spelled for it, so the two cannot
// drift apart on how a series is located in the exposition.
func beatSeriesValue(t *testing.T, name, id string) (string, bool) {
	t.Helper()
	return rawSeriesValue(t, name+`{beat="`+id+`"} `)
}

// TestNotificationCountersAdvertiseTheKindListInTheirHelpText pins the third
// leg of the exposition contract: the kind vocabulary is published as
// exposition METADATA, and an operator writing a KnellNotifyFailing selector
// reads the HELP text to learn which kind values exist. Rendered literally
// rather than via joinLabelValues, so a changed separator or a dropped kind
// fails here instead of silently reshaping the advertised set.
func TestNotificationCountersAdvertiseTheKindListInTheirHelpText(t *testing.T) {
	const kindList = "(missing, recovered, history)"
	want := []string{
		"knell_notifications_sent_total",
		"knell_notifications_failed_total",
		"knell_notifications_dropped_total",
	}

	for _, name := range want {
		help, _ := helpText(t, name)
		if help == "" {
			t.Errorf("%s has no HELP line: the exposition lost the metadata an operator reads to learn the metric's meaning", name)
			continue
		}
		if !strings.Contains(help, kindList) {
			t.Errorf("%s HELP = %q, want it to advertise %s: the rendered kind list is what tells an operator which kind label values a selector may match", name, strings.TrimSpace(help), kindList)
		}
	}
}

// TestPreRouteRefusalsAdvertiseTheReasonListInTheirHelpText is the refusal
// counter's half of the same metadata contract, and the reason joinLabelValues
// is shared at all: an operator investigating a missing beat reads this HELP
// text to learn which reason values a selector may match. Nothing else observes
// it - the cold-start test reads SAMPLES, so pinning the vocabulary in the
// sample lines leaves the advertised list free to go stale, naming reasons that
// no longer exist or omitting one that does.
func TestPreRouteRefusalsAdvertiseTheReasonListInTheirHelpText(t *testing.T) {
	const reasonList = "(non_canonical_beat_path, host_not_allowed, auth_throttled)"
	const name = "knell_pre_route_refusals_total"

	help, ok := helpText(t, name)
	if !ok {
		t.Fatalf("%s has no HELP line: the exposition lost the metadata an operator reads to learn the metric's meaning", name)
	}
	if !strings.Contains(help, reasonList) {
		t.Errorf("%s HELP = %q, want it to advertise %s: the rendered reason list is what tells an operator which reason label values a selector may match", name, strings.TrimSpace(help), reasonList)
	}
}

// helpText returns the HELP text of one metric out of the rendered exposition,
// and whether the metric had a HELP line at all. Shared by the two metadata
// tests so both read the advertised vocabulary the way an operator does.
func helpText(t *testing.T, name string) (string, bool) {
	t.Helper()
	prefix := "# HELP " + name + " "
	for line := range strings.Lines(exposition(t)) {
		if v, ok := strings.CutPrefix(line, prefix); ok {
			return v, true
		}
	}
	return "", false
}

// TestRecordHTTPRecordsBothTheCounterAndTheDuration pins that RecordHTTP wires
// BOTH of its collectors. internal/webapi's routed tests pin the counter and
// its label derivation, but nothing observes the histogram: with httpDuration
// dropped from the call, knell_http_request_duration_seconds stays at its
// registered zero forever and the whole test suite still passes, so the
// latency view an operator uses to tell a slow webhook from a slow scrape can
// be silently unwired. Asserted from inside the package because the histogram
// is unlabelled — there is no series a routed test could attribute to its own
// request.
func TestRecordHTTPRecordsBothTheCounterAndTheDuration(t *testing.T) {
	const (
		method = "POST"
		path   = "/record-http-probe/{id}"
		status = 418
	)
	counter := `knell_http_requests_total{method="` + method + `",path="` + path + `",status="418"} `
	// Same singleton-registry hygiene as the cold-start test: without this the
	// probe series survives into the next iteration and `go test -count=2`
	// fails on the uniqueness precondition below.
	t.Cleanup(func() {
		httpRequests.Delete(method, path, strconv.Itoa(status))
	})

	countBefore, _ := rawSeriesValue(t, "knell_http_request_duration_seconds_count")
	sumBefore, _ := rawSeriesValue(t, "knell_http_request_duration_seconds_sum")
	if _, ok := rawSeriesValue(t, counter); ok {
		t.Fatalf("%s is already present: the probe route is not unique to this test", counter)
	}

	RecordHTTP(method, path, status, 250*time.Millisecond)

	if got, ok := rawSeriesValue(t, counter); !ok || got != "1" {
		t.Errorf("%s = %q (present=%v), want 1: the counter is the only view of a refused ping", counter, got, ok)
	}
	countAfter, ok := rawSeriesValue(t, "knell_http_request_duration_seconds_count")
	if !ok || countAfter == countBefore {
		t.Errorf("knell_http_request_duration_seconds_count = %q, was %q: RecordHTTP observed no duration, so the latency view is unwired", countAfter, countBefore)
	}
	sumAfter, _ := rawSeriesValue(t, "knell_http_request_duration_seconds_sum")
	// The _sum is a UNIT-BEARING sample, so "it moved" is not the contract: the
	// series is named _seconds, and a duration observed in any other unit leaves
	// every latency panel, quantile and slow-request threshold wrong by three
	// orders of magnitude with nothing failing.
	before, err := strconv.ParseFloat(sumBefore, 64)
	if err != nil {
		t.Fatalf("parsing the pre-call sum %q: %v (the histogram is registered at init, so it must always be published)", sumBefore, err)
	}
	after, err := strconv.ParseFloat(sumAfter, 64)
	if err != nil {
		t.Fatalf("parsing the post-call sum %q: %v", sumAfter, err)
	}
	if delta := after - before; math.Abs(delta-0.25) > 1e-6 {
		t.Errorf("knell_http_request_duration_seconds_sum moved by %v for a 250ms request, want 0.25: the observation is not in seconds, so every latency query against a series named _seconds is wrong", delta)
	}
}

// TestRecordBeatPublishesLastSeenInWholeUnixSeconds pins the UNIT of the
// per-beat last-seen sample. The series is named _timestamp_seconds and two
// documented operator computations read it as Unix seconds: adding
// beat_deadline_seconds to it to learn when an overdue beat fires, and
// reconstructing the window of an outage whose record a full queue discarded.
// A RecordBeat that published milliseconds, nanoseconds or a fractional second
// leaves both of those silently wrong while every existing assertion stays
// green: internal/watch's ping test only checks the sample MOVED off the boot
// baseline, internal/webapi's asserts a refused ping left it UNCHANGED, and the
// exact-value assertions in this file cover InitBeat's baseline rather than
// RecordBeat's ping.
func TestRecordBeatPublishesLastSeenInWholeUnixSeconds(t *testing.T) {
	const id = "record-beat-unit-probe"
	// The registry is a package-level singleton that outlives one iteration, so
	// the probe series must not survive into the next `go test -count=2` run.
	t.Cleanup(func() {
		beatLastSeen.Delete(id)
		beatsReceived.Delete(id)
	})
	if got, ok := beatSeriesValue(t, "knell_beat_last_seen_timestamp_seconds", id); ok {
		t.Fatalf("knell_beat_last_seen_timestamp_seconds{beat=%q} = %s before RecordBeat: the probe id is not unique to this test, so it cannot pin the published sample", id, got)
	}

	// A ping with a sub-second component, so the assertion also fails for a
	// fractional-seconds rendering, not only for a wrong-magnitude unit.
	RecordBeat(id, time.Unix(1700000123, 456000000))

	got, ok := beatSeriesValue(t, "knell_beat_last_seen_timestamp_seconds", id)
	if !ok {
		t.Fatal("knell_beat_last_seen_timestamp_seconds is absent after an accepted ping: the operator has no window to reconstruct after a dropped notice, and no way to compute when the beat fires")
	}
	if got != "1700000123" {
		t.Errorf("knell_beat_last_seen_timestamp_seconds{beat=%q} = %s, want 1700000123: the series is named _seconds and the operator adds beat_deadline_seconds to it to learn when an overdue beat fires, so a sample in any other unit leaves that computation and the dropped-record reconstruction wrong with nothing failing", id, got)
	}
}

// rawSeriesValue returns the rendered value of the first exposition line
// starting with prefix, and whether one was present. It is the general form:
// beatSeriesValue is this function with a beat-label prefix, and the unlabelled
// http series (keyed on method/path/status) can only be reached through here.
func rawSeriesValue(t *testing.T, prefix string) (string, bool) {
	t.Helper()
	for line := range strings.Lines(exposition(t)) {
		if v, ok := strings.CutPrefix(line, prefix); ok {
			return strings.TrimSpace(v), true
		}
	}
	return "", false
}
