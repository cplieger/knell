// Package metrics defines knell's Prometheus metrics and the registry that
// serves them. Metrics are package-level singletons registered once at init
// (registration only panics on programmer error: a duplicate or invalid
// name); the exposition prefix is "knell_".
//
// The registry and the collectors are unexported: the package's edge is
// Handler plus the knell-semantic recording functions below (InitBeat,
// RecordBeat, SetBeatFresh, RecordOutage, RecordHTTP and the three
// RecordNotification* functions). Callers therefore cannot register, rename or
// delete a series, nor write a raw label position, which is what keeps the
// metric names, label sets and cold-start zero samples the quorum and delivery
// alerts read in one place.
//
// # Label-cardinality contract (read this before adding a caller)
//
// Label CARDINALITY is not structurally contained, and cannot be: Prometheus
// creates the labelled child on first use, so InitBeat, RecordBeat,
// SetBeatFresh and RecordOutage mint a per-beat series for whatever id they
// are handed and this package has nothing to reject it with. A series, once
// minted, is permanent for the process lifetime — in knell and in every
// observer scraping it.
//
// Keeping the beat label bounded is therefore the CALLER's contract, enforced
// by exactly three things upstream of here:
//
//   - internal/config validates every id against
//     [A-Za-z0-9][A-Za-z0-9_-]{0,63} and caps a fleet at 64 beats;
//   - internal/watch passes only ids from that configured set (New's beat map
//     is the whole domain of every call it makes);
//   - internal/webapi answers 404 for an unknown id, so an arbitrary request
//     path never reaches watch.Beat at all.
//
// The only PRODUCTION caller of the four id-taking functions is
// internal/watch; test code in this module may also call them, with fixed
// literal ids only. If you are adding any other production call, you have
// just inherited that contract: pass only an id internal/config validated and
// capped, or route through internal/watch, or validate the id yourself before
// the call.
// A raw id from a request path, a filename, a log line or a remote payload
// makes Mimir's label set grow without bound, and the growth is not
// recoverable by fixing the caller afterwards.
//
// RecordHTTP is the one recording function reached from internal/webapi in
// PRODUCTION, and it is deliberately not id-taking: it carries no beat label,
// so it is outside the contract above and adding it did not widen the set of
// packages allowed to mint a per-beat series. It has its own bounded-label
// contract instead, and the same reasoning drives it — its method and path
// labels must be bounded by the ROUTE TABLE and a closed method set, never by
// the request line, because /beat/{id} is served to unauthenticated callers
// (webhttp.Logging is the outermost middleware, so its metric hook fires before
// the BEAT_TOKEN gate runs). The derivation lives in webhttp
// (RouteMetricLabels): internal/webapi.New passes this function straight to
// webhttp.WithRecordRouteMetric, so the library computes the pair and knell has
// none of its own to get wrong.
//
// Runtime enforcement inside this package is deliberately NOT the answer: it
// would add state plus an init-order dependency on config to defend against a
// caller that does not exist. This comment is the guard instead — a reviewer's
// (or an agent's) tripwire, not the compiler's.
package metrics

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	metricslib "github.com/cplieger/metrics/v3"
)

// beatLabel names the watched beat on per-beat metrics; kindLabel names the
// notification kind (missing, recovered, history) on the delivery counters.
// The remaining three label the served-request counter.
const (
	beatLabel   = "beat"
	kindLabel   = "kind"
	methodLabel = "method"
	pathLabel   = "path"
	statusLabel = "status"
)

// Kind distinguishes notification label values from runtime strings. String
// variables require an explicit conversion before reaching the recording
// functions. It is not a closed set: untyped literals remain assignable, so
// callers must use the constants below and keep notificationKinds in sync.
type Kind string

// The notification kinds are the legal values of kindLabel on the sent,
// failed and dropped notification counters; dashboards and the
// KnellNotifyFailing alert key on them. They count MESSAGES, so KindHistory
// moves by one for a notice covering any number of ended outages;
// beatOutages counts outages.
const (
	KindMissing   Kind = "missing"
	KindRecovered Kind = "recovered"
	KindHistory   Kind = "history"
)

// notificationKinds drives cold-start pre-minting and the HELP kind list. Add
// every new Kind constant here so those two exposition views stay aligned.
var notificationKinds = []Kind{KindMissing, KindRecovered, KindHistory}

var notificationKindsText = joinKinds(notificationKinds)

func joinKinds(kinds []Kind) string {
	parts := make([]string, len(kinds))
	for i, k := range kinds {
		parts[i] = string(k)
	}
	return strings.Join(parts, ", ")
}

// mintNotificationKinds pre-mints every notification counter series at zero
// for every kind, so an increase() alert sees the very first failure or drop:
// a counter series born at a nonzero value has no earlier sample to diff
// against. Dropped is minted for all three kinds like
// its siblings, so the alert's selector covers the whole set from a cold start
// (only missing and recovered have a drop path today; a failed history send
// keeps its records and retries). Called from init() below, after the
// registrations, so the guarantee cannot be lost by a path that serves
// /metrics without building a Watcher.
func mintNotificationKinds() {
	for _, kind := range notificationKinds {
		notificationsSent.Add(0, string(kind))
		notificationsFailed.Add(0, string(kind))
		notificationsDropped.Add(0, string(kind))
	}
}

// registry serves every registered metric plus process metrics on /metrics;
// Handler is its only exported view.
var registry = metricslib.NewRegistry("knell")

// beatFresh reports per beat whether the last ping is within its deadline
// (1) or the beat is overdue (0). This is the aggregation input for
// multi-observer quorum rules.
var beatFresh = metricslib.NewLabeledGauge(
	"beat_fresh",
	"Whether the beat's last ping is within its deadline (1 = fresh, 0 = overdue).",
	[]string{beatLabel},
)

// beatLastSeen is the Unix timestamp of each beat's last accepted ping.
// Until a first ping arrives it carries the process start time (the boot
// baseline the deadline counts from).
var beatLastSeen = metricslib.NewLabeledGauge(
	"beat_last_seen_timestamp_seconds",
	"Unix timestamp of the beat's last accepted ping (process start until the first ping).",
	[]string{beatLabel},
)

// beatDeadline is each configured beat's silence deadline in seconds. It is
// configuration, not state, so it never changes for the process lifetime:
// it exists because the deadline is the value that decides when a beat is
// declared missing, and without it the exposition cannot answer two
// operator questions at all — how long until this overdue beat fires, and
// do the observers aggregated by one quorum rule agree on the deadline
// (a BEATS skew between nodes is otherwise invisible until one node
// alerts and the others stay quiet). The label is the same bounded beat
// label as its siblings, so this adds no cardinality the package doc's
// contract does not already cover.
var beatDeadline = metricslib.NewLabeledGauge(
	"beat_deadline_seconds",
	"Configured silence deadline per beat, in seconds.",
	[]string{beatLabel},
)

// beatsReceived counts accepted pings per beat. Unknown beat ids are
// rejected with 404 and deliberately not counted (the id is a label; counting
// arbitrary request paths would let callers mint unbounded series).
var beatsReceived = metricslib.NewLabeledCounter(
	"beats_received_total",
	"Accepted pings per beat (unknown ids are rejected and not counted).",
	[]string{beatLabel},
)

// beatOutages counts detected outages per beat: exactly one increment for
// every deadline crossing the state machine detects, at detection time and
// independent of notification delivery. An outage whose notice was never
// delivered, or was dropped by a full queue, or was collapsed with others
// into one history message is counted here all the same. This is the only
// series that counts OUTAGES: the notification counters count MESSAGES, and
// one history message can cover several outages.
var beatOutages = metricslib.NewLabeledCounter(
	"beat_outages_total",
	"Detected outages per beat: one increment per deadline crossing detected, independent of notification delivery.",
	[]string{beatLabel},
)

// notificationsSent counts webhook notifications delivered, by kind
// (missing, recovered, history). One increment per delivered MESSAGE: a
// history message covering several ended outages counts once, so pair this
// with beatOutages to reason about outages rather than messages.
var notificationsSent = metricslib.NewLabeledCounter(
	"notifications_sent_total",
	"Webhook notifications delivered, by kind ("+notificationKindsText+"); one per delivered message.",
	[]string{kindLabel},
)

// notificationsFailed counts webhook delivery attempts that failed after
// retries AND still have a record to retry from, by kind. In practice that is
// missing and history: their records stay queued, so the next sweep re-sends
// them. It is deliberately NOT every failed attempt — a failed RECOVERED send
// is counted on notificationsDropped instead, because the event is dequeued
// before the send and finishRecovery runs unconditionally, so nothing survives
// to retry (watch.sendRecovered spells this out); failed{kind="recovered"}
// therefore stays at its pre-minted zero. One increment per failed MESSAGE, so
// a failed history message counts once whatever number of ended outages it
// covered. A notification that was never attempted because its record was
// discarded by a full queue is NOT counted here either: see
// notificationsDropped.
var notificationsFailed = metricslib.NewLabeledCounter(
	"notifications_failed_total",
	"Webhook delivery attempts that failed after retries, by kind ("+notificationKindsText+"); one per failed message.",
	[]string{kindLabel},
)

// notificationsDropped counts notifications that will never be delivered, by
// kind. Two causes reach it: a record discarded when the per-beat queue was
// full, and a recovered send that FAILED — recovered is dequeued before the
// send and finishRecovery runs unconditionally, so nothing holds a record to
// retry from. That is the line against notificationsFailed, which is drawn by
// what SURVIVES rather than by whether a send was attempted: a notification
// counted failed still has its record and retries, while a dropped one has
// nothing left to retry from, so no notice for that transition will ever
// arrive. Reconstruct the missed window from beatLastSeen; beatOutages
// already counted the outage itself at detection. A queue-full event that
// loses nothing — an ongoing outage that stays detected and is queued once a
// slot frees — is not counted here either, because nothing was dropped.
var notificationsDropped = metricslib.NewLabeledCounter(
	"notifications_dropped_total",
	"Notifications that will never be delivered, by kind ("+notificationKindsText+"); a record discarded when the per-beat queue was full, or a recovered send that failed with nothing left to retry from; distinct from a delivery that failed and will retry.",
	[]string{kindLabel},
)

// httpRequests counts every served HTTP request by matched route template,
// method and status. It is the ONLY view of a REFUSED ping: an unauthorized
// (401), unknown-id (404), shutting-down (503) or disallowed-method (405)
// beat request never reaches beatsReceived, so without this series a sender
// whose token was rotated or whose beat id is misspelled stays invisible
// until the beat goes missing a full deadline later — on an app whose whole
// job is being alertable. Labels are bounded by the CALLER, which passes the
// pair webhttp derives from the matched route and a closed method set rather
// than anything off the request line (see the package doc's RecordHTTP
// contract).
var httpRequests = metricslib.NewLabeledCounter(
	"http_requests_total",
	"Served HTTP requests by matched route template, method and status.",
	[]string{methodLabel, pathLabel, statusLabel},
)

// httpDuration observes served-request latency across the whole surface. It
// is deliberately unlabelled: a labelled histogram would multiply bucket
// series per route for no operator question on a three-route surface, and
// the aggregate still surfaces the one signal that matters here (a slow
// request — e.g. a body drain riding out the 30s read timeout — lands past
// the 1.0s top default bucket). httpRequests carries the per-route
// request/status split; per-route LATENCY is deliberately not derivable from
// this pair, and a route label should be added here if that question ever
// becomes real.
var httpDuration = metricslib.NewHistogram(
	"http_request_duration_seconds",
	"Served HTTP request duration in seconds.",
)

func init() {
	registry.RegisterLabeledGauge(beatFresh)
	registry.RegisterLabeledGauge(beatLastSeen)
	registry.RegisterLabeledGauge(beatDeadline)
	registry.RegisterLabeledCounter(beatsReceived)
	registry.RegisterLabeledCounter(beatOutages)
	registry.RegisterLabeledCounter(notificationsSent)
	registry.RegisterLabeledCounter(notificationsFailed)
	registry.RegisterLabeledCounter(notificationsDropped)
	registry.RegisterLabeledCounter(httpRequests)
	registry.RegisterHistogram(httpDuration)
	mintNotificationKinds()
}

// Handler serves the Prometheus exposition: every registered metric plus
// process metrics. It is the only exported view of the registry, so a
// consumer can scrape the metrics but cannot mint, delete or re-register a
// series.
func Handler() http.Handler {
	return registry.Handler()
}

// InitBeat declares a configured beat at process start: the beat begins fresh
// with start as its last-seen baseline (the boot-armed clock every first
// deadline counts from), and its per-beat counters are pre-minted at zero for
// the same reason mintNotificationKinds pre-mints the notification counters —
// increase() needs an earlier sample to diff against. deadline is published as
// beat_deadline_seconds, the one per-beat series that is configuration rather
// than state: this is the only place it is written, and it is what lets an
// operator compute when a beat goes overdue (last-seen plus the deadline) and
// see whether the observers one quorum rule aggregates agree on it. Only
// configured ids reach here, which is what keeps the beat label bounded (see
// the package doc's label-cardinality contract).
func InitBeat(id string, deadline time.Duration, start time.Time) {
	beatDeadline.Set(deadline.Seconds(), id)
	beatFresh.Set(1, id)
	beatLastSeen.Set(float64(start.Unix()), id)
	beatsReceived.Add(0, id)
	beatOutages.Add(0, id)
}

// RecordBeat records an accepted ping for id observed at now: the ping is
// counted and the beat is published fresh with its new last-seen timestamp.
// The caller publishes under its own lock so concurrent pings cannot write
// the gauges out of state order. id is a label: see the package doc's
// label-cardinality contract.
func RecordBeat(id string, now time.Time) {
	beatsReceived.Inc(id)
	beatFresh.Set(1, id)
	beatLastSeen.Set(float64(now.Unix()), id)
}

// SetBeatFresh publishes the quorum gauge for id: fresh when the last ping is
// within the beat's deadline, overdue otherwise. The freshness boundary
// itself belongs to the watch state machine; this is only the exposition of
// its verdict. id is a label: see the package doc's label-cardinality
// contract.
func SetBeatFresh(id string, fresh bool) {
	if fresh {
		beatFresh.Set(1, id)
		return
	}
	beatFresh.Set(0, id)
}

// RecordOutage counts one detected outage for id, at detection time and
// independent of whether its notice is ever delivered. It is the only
// counter that counts OUTAGES rather than messages. id is a label: see the
// package doc's label-cardinality contract.
func RecordOutage(id string) {
	beatOutages.Inc(id)
}

// RecordHTTP records one served HTTP request: its latency, and one increment
// on the method/path/status combination. method and path must be bounded by the
// route table and a closed method vocabulary, not taken off the request line —
// see the package doc's RecordHTTP contract. Its only production caller is
// webhttp's WithRecordRouteMetric hook, wired in internal/webapi.New: the
// library derives the pair (webhttp.RouteMetricLabels) and calls this function
// with it. Unlike the beat counters this series is not pre-minted:
// an HTTP counter has no known-in-advance status set, so alerts on it should
// sum over the label set rather than select a single status.
func RecordHTTP(method, path string, status int, d time.Duration) {
	metricslib.RecordHTTP(httpRequests, httpDuration, d, method, path, strconv.Itoa(status))
}

// RecordNotificationSent counts one delivered notification message of kind.
func RecordNotificationSent(kind Kind) {
	notificationsSent.Inc(string(kind))
}

// RecordNotificationFailed counts one notification message of kind whose
// delivery was attempted and failed after retries. Its record survives, so
// the send retries: this is never the counter for something that was dropped
// unsent.
func RecordNotificationFailed(kind Kind) {
	notificationsFailed.Inc(string(kind))
}

// RecordNotificationDropped counts one notification message of kind that will
// never be delivered, for either cause: the record it would have been built
// from was discarded by a full queue, or it was a recovered send that failed
// and left no record behind. Nothing retries it.
func RecordNotificationDropped(kind Kind) {
	notificationsDropped.Inc(string(kind))
}
