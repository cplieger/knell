// Package metrics defines knell's Prometheus metrics and the registry that
// serves them: package-level singletons registered once at init, exposition
// prefix "knell_". The registry and the collectors are unexported, so a caller
// cannot register, rename or delete a series, nor write a raw label position.
//
// # Label-cardinality contract (read this before adding a caller)
//
// Cardinality is NOT structurally contained: Prometheus mints the labelled
// child on first use, and a minted series is permanent for the process
// lifetime -- here and in every observer scraping it. Keeping the beat label
// bounded is therefore the CALLER's contract: internal/config validates every
// id against [A-Za-z0-9][A-Za-z0-9_-]{0,63} and caps a fleet at 64 beats, and
// internal/watch is the only production caller of the id-taking functions. Any
// other production call inherits that contract, and runtime enforcement here
// would only add an init-order dependency on config in its place.
package metrics

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	metricslib "github.com/cplieger/metrics/v3"
)

// beatLabel names the watched beat on per-beat metrics; kindLabel names the
// notification kind on the delivery counters; reasonLabel names the cause on
// the pre-route refusal counter. The remaining three label served requests.
const (
	beatLabel   = "beat"
	kindLabel   = "kind"
	reasonLabel = "reason"
	methodLabel = "method"
	pathLabel   = "path"
	statusLabel = "status"
)

// Kind distinguishes notification label values from runtime strings. It is not
// a closed set: untyped literals remain assignable, so callers must use the
// constants below and keep notificationKinds in sync.
type Kind string

// The notification kinds are the legal values of kindLabel on the sent, failed
// and dropped counters; dashboards and the KnellNotifyFailing alert key on
// them. They count MESSAGES, so KindHistory moves by one for a notice covering
// any number of ended outages; beatOutages counts outages.
const (
	KindMissing   Kind = "missing"
	KindRecovered Kind = "recovered"
	KindHistory   Kind = "history"
)

// notificationKinds drives cold-start pre-minting and the HELP kind list. Add
// every new Kind constant here so those two views stay aligned.
var notificationKinds = []Kind{KindMissing, KindRecovered, KindHistory}

var notificationKindsText = joinLabelValues(notificationKinds)

// Refusal distinguishes pre-route refusal reason values from runtime strings,
// as Kind does for the notification counters, and is not closed to the
// compiler either: use the constants and keep refusalReasons in sync.
type Refusal string

// The refusal reasons are the legal values of reasonLabel. Each names the
// CAUSE rather than the status code, because the status is already visible on
// http_requests_total and the cause is what this counter adds.
const (
	// RefusalNonCanonicalBeatPath is a /beat spelling net/http would rewrite
	// before routing, refused 404 by internal/webapi. It means a sender is
	// pinging a malformed URL.
	RefusalNonCanonicalBeatPath Refusal = "non_canonical_beat_path"
	// RefusalHostNotAllowed is a request whose Host is not in ALLOWED_HOSTS:
	// either a DNS-rebinding attempt or a hostname the deployment forgot.
	RefusalHostNotAllowed Refusal = "host_not_allowed"
	// RefusalAuthThrottled is a beat request refused because the shared
	// failed-auth bucket was empty: a rotated token on a whole sender fleet,
	// or a guessing run.
	RefusalAuthThrottled Refusal = "auth_throttled"
)

// refusalReasons drives cold-start pre-minting and the HELP reason list. Add
// every new Refusal constant here so those two views stay aligned.
var refusalReasons = []Refusal{RefusalNonCanonicalBeatPath, RefusalHostNotAllowed, RefusalAuthThrottled}

var refusalReasonsText = joinLabelValues(refusalReasons)

// joinLabelValues renders a closed label vocabulary for a HELP string, so an
// operator writing a selector can read which values exist. Generic over the
// string-kinded label types so the two vocabularies are advertised alike.
func joinLabelValues[T ~string](values []T) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = string(v)
	}
	return strings.Join(parts, ", ")
}

// mintNotificationKinds pre-mints every notification counter series at zero
// for every kind, so an increase() alert sees the very first failure or drop:
// a series born at a nonzero value has no earlier sample to diff against.
// Called from init() after the registrations, so the guarantee cannot be lost
// by a path that serves /metrics without building a Watcher.
func mintNotificationKinds() {
	for _, kind := range notificationKinds {
		notificationsSent.Add(0, string(kind))
		notificationsFailed.Add(0, string(kind))
		notificationsDropped.Add(0, string(kind))
	}
}

// mintRefusalReasons pre-mints the pre-route refusal counter at zero for every
// reason, for the reason mintNotificationKinds exists: without an earlier
// sample the first refusal of a reason is invisible to a windowed query.
func mintRefusalReasons() {
	for _, reason := range refusalReasons {
		preRouteRefusals.Add(0, string(reason))
	}
}

// registry serves every registered metric plus process metrics on /metrics;
// Handler is its only exported view.
var registry = metricslib.NewRegistry("knell")

// beatFresh reports per beat whether its observed silence is within its
// deadline; until a first ping arrives that silence runs from process start
// (the boot-armed clock), so a beat nothing has pinged reads 1 for its first
// deadline. This is the aggregation input for multi-observer quorum rules.
var beatFresh = metricslib.NewLabeledGauge(
	"beat_fresh",
	"Whether the beat's observed silence is within its deadline (1 = fresh, 0 = overdue; silence runs from process start until the first ping).",
	[]string{beatLabel},
)

// beatLastSeen is the Unix timestamp of each beat's last accepted ping. Until
// a first ping arrives it carries the process start time.
var beatLastSeen = metricslib.NewLabeledGauge(
	"beat_last_seen_timestamp_seconds",
	"Unix timestamp of the beat's last accepted ping (process start until the first ping).",
	[]string{beatLabel},
)

// beatDeadline is each configured beat's silence deadline in seconds. It is
// configuration, not state, and exists because without it the exposition
// cannot answer two operator questions: how long until this overdue beat
// fires, and whether the observers one quorum rule aggregates agree on the
// deadline (a BEATS skew is otherwise invisible until one node alerts alone).
var beatDeadline = metricslib.NewLabeledGauge(
	"beat_deadline_seconds",
	"Configured silence deadline per beat, in seconds.",
	[]string{beatLabel},
)

// beatsReceived counts accepted pings per beat. Unknown ids are rejected and
// deliberately not counted: the id is a label.
var beatsReceived = metricslib.NewLabeledCounter(
	"beats_received_total",
	"Accepted pings per beat (unknown ids are rejected and not counted).",
	[]string{beatLabel},
)

// beatOutages counts detected outages per beat: one increment per deadline
// crossing, at detection time and independent of delivery, so an outage whose
// notice was never delivered, dropped, or collapsed into a history message is
// counted all the same. One of the two series counting OUTAGES rather than
// messages (outageRecordsDropped is the other).
var beatOutages = metricslib.NewLabeledCounter(
	"beat_outages_total",
	"Detected outages per beat: one increment per deadline crossing detected, independent of notification delivery.",
	[]string{beatLabel},
)

// outageRecordsDropped counts outage RECORDS discarded for good per beat: one
// increment per record whose outage had already ENDED when a full per-beat
// queue refused it, so no notice for it will ever arrive. The unit is the
// RECORD because the operator's remedy is per-OUTAGE (reconstruct the window
// from beatLastSeen), and a history message collapses several records, so N
// discarded records are not N lost messages. The still-ONGOING overflow case
// does not reach here: it is re-recorded by a later sweep, so nothing is lost.
var outageRecordsDropped = metricslib.NewLabeledCounter(
	"outage_records_dropped_total",
	"Ended-outage records discarded per beat because the per-beat queue was full; no notice for them will ever arrive.",
	[]string{beatLabel},
)

// notificationsSent counts webhook notifications delivered, by kind: one
// increment per delivered MESSAGE, so pair it with beatOutages to reason about
// outages rather than messages.
var notificationsSent = metricslib.NewLabeledCounter(
	"notifications_sent_total",
	"Webhook notifications delivered, by kind ("+notificationKindsText+"); one per delivered message.",
	[]string{kindLabel},
)

// notificationsFailed counts delivery attempts that failed after retries AND
// still have a record to retry from, which in practice is missing and history.
// failed{kind="recovered"} stays at its pre-minted zero: recovered is
// fire-once, so a failed one is counted on notificationsDropped. A
// notification never attempted because its record was discarded is a lost
// RECORD, counted on outageRecordsDropped.
var notificationsFailed = metricslib.NewLabeledCounter(
	"notifications_failed_total",
	"Webhook delivery attempts that failed after retries, by kind ("+notificationKindsText+"); one per failed message. kind=recovered never moves here: recovered is fire-once with nothing left to retry, so a failed recovered send is counted on notifications_dropped_total instead.",
	[]string{kindLabel},
)

// notificationsDropped counts notification MESSAGES that will never be
// delivered. The line against notificationsFailed is drawn by what SURVIVES: a
// failed notification still has its record and retries, a dropped one has
// nothing left to retry from. Today only recovered can land here, being the
// one fire-once kind. A discarded outage RECORD is not a message and goes to
// outageRecordsDropped.
var notificationsDropped = metricslib.NewLabeledCounter(
	"notifications_dropped_total",
	"Notification messages that will never be delivered, by kind ("+notificationKindsText+"); a fire-once recovered notice whose send failed with nothing left to retry from; distinct from a delivery that failed and will retry.",
	[]string{kindLabel},
)

// preRouteRefusals counts requests knell refuses BEFORE the mux routes, by
// reason: none is nameable on httpRequests, which buckets the pre-route 403
// and 404 as "unmatched" beside scanner traffic and never sees the 429 at all.
// It is a DIAGNOSTIC for a missing beat, not an alert source: a sender refused
// here is not feeding its beat, so the missing notice fires on its own.
var preRouteRefusals = metricslib.NewLabeledCounter(
	"pre_route_refusals_total",
	"Requests refused before the mux routes, by reason ("+refusalReasonsText+"); a diagnostic for a missing beat, not an alert source.",
	[]string{reasonLabel},
)

// httpRequests counts every served HTTP request by matched route template,
// method and status. It is the ONLY view of a REFUSED ping -- 401, 404, 405
// and 503 never reach beatsReceived -- so without it a sender whose token was
// rotated or whose id is misspelled stays invisible until the beat goes
// missing a full deadline later. Labels are bounded by the CALLER.
var httpRequests = metricslib.NewLabeledCounter(
	"http_requests_total",
	"Served HTTP requests by matched route template, method and status. Series are not pre-minted, so a status series is born with its first request and increase() cannot see that first event; alert on the absolute value (status=401 > 0), which latches until restart.",
	[]string{methodLabel, pathLabel, statusLabel},
)

// httpDuration observes served-request latency across the whole surface. It is
// deliberately unlabelled: a labelled histogram would multiply bucket series
// per route for no operator question on a three-route surface, and the
// aggregate still surfaces the one signal that matters (a slow request lands
// past the 1.0s top default bucket). Per-route LATENCY is deliberately not
// derivable; add a route label here if that question becomes real.
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
	registry.RegisterLabeledCounter(outageRecordsDropped)
	registry.RegisterLabeledCounter(notificationsSent)
	registry.RegisterLabeledCounter(notificationsFailed)
	registry.RegisterLabeledCounter(notificationsDropped)
	registry.RegisterLabeledCounter(preRouteRefusals)
	registry.RegisterLabeledCounter(httpRequests)
	registry.RegisterHistogram(httpDuration)
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
// last-seen baseline (the boot-armed clock every first deadline counts from),
// and pre-mints its per-beat counters at zero so increase() has an earlier
// sample to diff against. This is the only place beat_deadline_seconds is
// written. The freshness verdict is NOT published here: SetBeatFresh is that
// gauge's single door.
func InitBeat(id string, deadline time.Duration, start time.Time) {
	beatDeadline.Set(deadline.Seconds(), id)
	beatLastSeen.Set(float64(start.Unix()), id)
	beatsReceived.Add(0, id)
	beatOutages.Add(0, id)
	outageRecordsDropped.Add(0, id)
}

// RecordBeat records an accepted ping for id observed at now. The resulting
// freshness verdict goes through SetBeatFresh, so this never decides freshness
// itself. The caller publishes under its own lock so concurrent pings cannot
// write the gauges out of state order.
func RecordBeat(id string, now time.Time) {
	beatsReceived.Inc(id)
	beatLastSeen.Set(float64(now.Unix()), id)
}

// SetBeatFresh publishes the quorum gauge for id. The freshness boundary
// belongs to the watch state machine; this is only the exposition of its
// verdict.
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

// RecordOutageRecordDropped counts one ended-outage RECORD for id discarded
// for good because the per-beat queue was full. It counts RECORDS, not
// messages. The ongoing-outage overflow, which loses nothing, must not come
// here.
func RecordOutageRecordDropped(id string) {
	outageRecordsDropped.Inc(id)
}

// RecordHTTP records one served HTTP request: its latency, and one increment
// on the method/path/status combination. method and path must be bounded by
// the route table and a closed method vocabulary, not taken off the request
// line. Unlike the beat counters this series is not pre-minted, so a refusal
// series is BORN with the first refused ping and increase() cannot see that
// birth: alert on the ABSOLUTE value, which latches until restart.
func RecordHTTP(method, path string, status int, d time.Duration) {
	metricslib.RecordHTTP(httpRequests, httpDuration, d, method, path, strconv.Itoa(status))
}

// RecordPreRouteRefusal counts one request refused before the mux routed it,
// under the reason knell refused it. Read it while investigating a missing
// beat; it is deliberately not an alert source (see preRouteRefusals).
func RecordPreRouteRefusal(reason Refusal) {
	preRouteRefusals.Inc(string(reason))
}

// RecordNotificationSent counts one delivered notification message of kind.
func RecordNotificationSent(kind Kind) {
	notificationsSent.Inc(string(kind))
}

// RecordNotificationFailed counts one notification message of kind whose
// delivery was attempted and failed after retries. Its record survives, so the
// send retries: this is never the counter for something dropped unsent.
func RecordNotificationFailed(kind Kind) {
	notificationsFailed.Inc(string(kind))
}

// RecordNotificationDropped counts one notification message of kind that will
// never be delivered: a fire-once recovered notice whose send failed and left
// no record behind. A discarded outage RECORD is not a message and goes to
// RecordOutageRecordDropped instead.
func RecordNotificationDropped(kind Kind) {
	notificationsDropped.Inc(string(kind))
}
