// Package obs defines knell's Prometheus metrics and the registry that
// serves them: package-level singletons registered once at init, exposition
// prefix "knell_". The registry and the collectors are unexported, so a caller
// cannot register, rename or delete a series, nor write a raw label position.
//
// # Label-cardinality contract
//
// Cardinality is not structurally contained here: keeping the beat label
// bounded is the CALLER's contract. internal/config validates every id
// against [A-Za-z0-9][A-Za-z0-9_-]{0,63} and caps a fleet at 64 beats, and
// internal/watch is the only production caller of the id-taking functions.
package obs

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cplieger/metrics/v4"
)

// beatLabel names the watched beat on per-beat metrics; kindLabel names the
// notification kind on the delivery counters; reasonLabel names the cause on
// the pre-route refusal counter.
const (
	beatLabel   = "beat"
	kindLabel   = "kind"
	reasonLabel = "reason"
	methodLabel = "method"
	pathLabel   = "path"
	statusLabel = "status"
)

// Kind distinguishes notification label values from runtime strings; use the
// constants below and keep notificationKinds in sync.
type Kind string

// The notification kinds are the legal values of kindLabel on the sent, failed
// and dropped counters. They count MESSAGES: KindHistory moves by one for a
// notice covering any number of ended outages.
const (
	KindMissing   Kind = "missing"
	KindRecovered Kind = "recovered"
	KindHistory   Kind = "history"
)

// notificationKinds drives cold-start pre-minting and the HELP kind list. Add
// every new Kind constant here so those two views stay aligned.
var notificationKinds = []Kind{KindMissing, KindRecovered, KindHistory}

var notificationKindsText = joinLabelValues(notificationKinds)

// Refusal distinguishes pre-route refusal reason values from runtime strings;
// use the constants and keep refusalReasons in sync.
type Refusal string

// The refusal reasons are the legal values of reasonLabel, naming the CAUSE
// rather than the status code (already visible on http_requests_total).
const (
	// RefusalNonCanonicalBeatPath is a /beat spelling net/http would rewrite
	// before routing, refused 404 by internal/webapi.
	RefusalNonCanonicalBeatPath Refusal = "non_canonical_beat_path"
	// RefusalHostNotAllowed is a request whose Host is not in ALLOWED_HOSTS.
	RefusalHostNotAllowed Refusal = "host_not_allowed"
	// RefusalAuthThrottled is a beat request refused because the shared
	// failed-auth bucket was empty.
	RefusalAuthThrottled Refusal = "auth_throttled"
)

// refusalReasons drives cold-start pre-minting and the HELP reason list. Add
// every new Refusal constant here so those two views stay aligned.
var refusalReasons = []Refusal{RefusalNonCanonicalBeatPath, RefusalHostNotAllowed, RefusalAuthThrottled}

var refusalReasonsText = joinLabelValues(refusalReasons)

// joinLabelValues renders a closed label vocabulary for a HELP string.
// Generic over the string-kinded label types so both vocabularies are
// advertised alike.
func joinLabelValues[T ~string](values []T) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = string(v)
	}
	return strings.Join(parts, ", ")
}

// mintNotificationKinds pre-mints every notification counter series at zero
// for every kind, so an increase() alert sees the very first failure or drop.
// Called from init() so the guarantee cannot be lost by a path that serves
// /metrics without building a Watcher.
func mintNotificationKinds() {
	for _, kind := range notificationKinds {
		notificationsSent.Add(0, string(kind))
		notificationsFailed.Add(0, string(kind))
		notificationsDropped.Add(0, string(kind))
	}
}

// mintRefusalReasons pre-mints the pre-route refusal counter at zero for
// every reason, for the same reason mintNotificationKinds exists.
func mintRefusalReasons() {
	for _, reason := range refusalReasons {
		preRouteRefusals.Add(0, string(reason))
	}
}

// registry serves every registered metric plus process metrics on /metrics;
// Handler is its only exported view.
var registry = metrics.NewRegistry("knell")

// beatFresh reports per beat whether its observed silence is within its
// deadline. Silence runs from process start until the first ping, so a beat
// nothing has pinged reads 1 for its first deadline.
var beatFresh = metrics.NewLabeledGauge(
	"beat_fresh",
	"Whether the beat's observed silence is within its deadline (1 = fresh, 0 = overdue; silence runs from process start until the first ping).",
	[]string{beatLabel},
)

// beatLastSeen is the Unix timestamp of each beat's last accepted ping. Until
// a first ping arrives it carries the process start time.
var beatLastSeen = metrics.NewLabeledGauge(
	"beat_last_seen_timestamp_seconds",
	"Unix timestamp of the beat's last accepted ping (process start until the first ping).",
	[]string{beatLabel},
)

// beatDeadline is each configured beat's silence deadline in seconds. It is
// configuration, not state: it lets an operator see how long until an overdue
// beat fires and whether multiple observers agree on the deadline.
var beatDeadline = metrics.NewLabeledGauge(
	"beat_deadline_seconds",
	"Configured silence deadline per beat, in seconds.",
	[]string{beatLabel},
)

// beatsReceived counts accepted pings per beat. Unknown ids are rejected and
// deliberately not counted.
var beatsReceived = metrics.NewLabeledCounter(
	"beats_received_total",
	"Accepted pings per beat (unknown ids are rejected and not counted).",
	[]string{beatLabel},
)

// beatOutages counts detected outages per beat: one increment per deadline
// crossing at detection time, independent of delivery.
var beatOutages = metrics.NewLabeledCounter(
	"beat_outages_total",
	"Detected outages per beat: one increment per deadline crossing detected, independent of notification delivery.",
	[]string{beatLabel},
)

// outageRecordsDropped counts outage records discarded for good per beat: one
// increment per record whose outage had already ended when a full per-beat
// queue refused it, so no notice for it will ever arrive. The still-ongoing
// overflow case does not reach here: it is re-recorded by a later sweep.
var outageRecordsDropped = metrics.NewLabeledCounter(
	"outage_records_dropped_total",
	"Ended-outage records discarded per beat because the per-beat queue was full; no notice for them will ever arrive.",
	[]string{beatLabel},
)

// notificationsSent counts webhook notifications delivered, by kind: one
// increment per delivered message.
var notificationsSent = metrics.NewLabeledCounter(
	"notifications_sent_total",
	"Webhook notifications delivered, by kind ("+notificationKindsText+"); one per delivered message.",
	[]string{kindLabel},
)

// notificationsFailed counts delivery attempts that failed after retries and
// still have a record to retry from (missing and history in practice).
// failed{kind="recovered"} stays at its pre-minted zero: recovered is
// fire-once, so a failed one is counted on notificationsDropped instead.
var notificationsFailed = metrics.NewLabeledCounter(
	"notifications_failed_total",
	"Webhook delivery attempts that failed after retries, by kind ("+notificationKindsText+"); one per failed message. kind=recovered never moves here: recovered is fire-once with nothing left to retry, so a failed recovered send is counted on notifications_dropped_total instead.",
	[]string{kindLabel},
)

// notificationsDropped counts notification messages that will never be
// delivered. Distinct from notificationsFailed by what survives: a failed
// notification still has its record and retries; a dropped one does not.
// Today only the fire-once recovered kind can land here.
var notificationsDropped = metrics.NewLabeledCounter(
	"notifications_dropped_total",
	"Notification messages that will never be delivered, by kind ("+notificationKindsText+"); a fire-once recovered notice whose send failed with nothing left to retry from; distinct from a delivery that failed and will retry.",
	[]string{kindLabel},
)

// preRouteRefusals counts requests knell refuses before the mux routes, by
// reason: none of these is nameable on httpRequests, which buckets them all
// as "unmatched" beside scanner traffic. A diagnostic, not an alert source.
var preRouteRefusals = metrics.NewLabeledCounter(
	"pre_route_refusals_total",
	"Requests refused before the mux routes, by reason ("+refusalReasonsText+"); a diagnostic for a missing beat, not an alert source.",
	[]string{reasonLabel},
)

// httpRequests counts every served HTTP request by matched route template,
// method and status. It is the only view of a refused ping -- 401, 404, 405
// and 503 never reach beatsReceived.
var httpRequests = metrics.NewLabeledCounter(
	"http_requests_total",
	"Served HTTP requests by matched route template, method and status. Series are not pre-minted, so a status series is born with its first request and increase() cannot see that first event; alert on the absolute value (status=401 > 0), which latches until restart.",
	[]string{methodLabel, pathLabel, statusLabel},
)

// httpDuration observes served-request latency across the whole surface. It
// is deliberately unlabelled: a labelled histogram would multiply bucket
// series per route for no operator question on a three-route surface.
var httpDuration = metrics.NewHistogram(
	"http_request_duration_seconds",
	"Served HTTP request duration in seconds.",
)

func init() {
	// MustRegister: init-time registration has no caller to hand an error to.
	// metrics v4 captures a construction error into the metric value rather
	// than panicking, so registration is where it surfaces, at process start.
	registry.MustRegister(
		beatFresh,
		beatLastSeen,
		beatDeadline,
		beatsReceived,
		beatOutages,
		outageRecordsDropped,
		notificationsSent,
		notificationsFailed,
		notificationsDropped,
		preRouteRefusals,
		httpRequests,
		httpDuration,
	)
	mintNotificationKinds()
	mintRefusalReasons()
}

// Handler serves the Prometheus exposition: every registered metric plus
// process metrics. It is the only exported view of the registry, so a consumer
// can scrape the metrics but cannot mint, delete or re-register a series.
func Handler() http.Handler {
	return registry.Handler()
}

// InitBeat declares a configured beat at process start with start as its
// last-seen baseline, and pre-mints its per-beat counters at zero. This is
// the only place beat_deadline_seconds is written; SetBeatFresh is the
// gauge's own single door.
func InitBeat(id string, deadline time.Duration, start time.Time) {
	beatDeadline.Set(deadline.Seconds(), id)
	beatLastSeen.Set(float64(start.Unix()), id)
	beatsReceived.Add(0, id)
	beatOutages.Add(0, id)
	outageRecordsDropped.Add(0, id)
}

// RecordBeat records an accepted ping for id observed at now. The caller
// publishes under its own lock so concurrent pings cannot write the gauges
// out of state order.
func RecordBeat(id string, now time.Time) {
	beatsReceived.Inc(id)
	beatLastSeen.Set(float64(now.Unix()), id)
}

// SetBeatFresh publishes the quorum gauge for id. The freshness boundary
// belongs to the watch state machine; this is only its exposition.
func SetBeatFresh(id string, fresh bool) {
	if fresh {
		beatFresh.Set(1, id)
		return
	}
	beatFresh.Set(0, id)
}

// RecordOutage counts one detected outage for id, at detection time and
// independent of whether its notice is ever delivered.
func RecordOutage(id string) {
	beatOutages.Inc(id)
}

// RecordOutageRecordDropped counts one ended-outage record for id discarded
// for good because the per-beat queue was full. The still-ongoing overflow
// case, which loses nothing, must not come here.
func RecordOutageRecordDropped(id string) {
	outageRecordsDropped.Inc(id)
}

// RecordHTTP records one served HTTP request: its latency, and one increment
// on the method/path/status combination. method and path must be bounded by
// the route table and a closed method vocabulary. This series is not
// pre-minted, so alert on the absolute value rather than increase().
func RecordHTTP(method, path string, status int, d time.Duration) {
	metrics.RecordHTTP(httpRequests, httpDuration, d, method, path, strconv.Itoa(status))
}

// RecordPreRouteRefusal counts one request refused before the mux routed it,
// under the reason knell refused it.
func RecordPreRouteRefusal(reason Refusal) {
	preRouteRefusals.Inc(string(reason))
}

// RecordNotificationSent counts one delivered notification message of kind.
func RecordNotificationSent(kind Kind) {
	notificationsSent.Inc(string(kind))
}

// RecordNotificationFailed counts one notification message of kind whose
// delivery was attempted and failed after retries.
func RecordNotificationFailed(kind Kind) {
	notificationsFailed.Inc(string(kind))
}

// RecordNotificationDropped counts one notification message of kind that
// will never be delivered: a fire-once recovered notice whose send failed
// with nothing left to retry from.
func RecordNotificationDropped(kind Kind) {
	notificationsDropped.Inc(string(kind))
}
