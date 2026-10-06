# Monitoring and alerts

This page lists what knell logs and the metrics it publishes, and how to load the alert rules that watch knell itself. It is for operators who run Prometheus, the Mimir ruler or Loki.

## Logs

knell writes logfmt to standard error, so `docker logs knell` and any Docker log collector read it with no configuration. Go renders the level in capitals, `level=ERROR`. At startup it logs one `watching beat` line per beat, one `configuration loaded` line with the settings it runs with and then `listening`.

Every line that reports a lost or delayed notice carries a `retryable` field. `retryable=true` means the next sweep tries again, so the notice is late. `retryable=false` means nothing will retry it, so rebuild the missed window yourself.

## Metrics

`GET /metrics` serves these, plus the standard `go_*` and `process_*` runtime metrics.

| Metric | Type | Notes |
| --- | --- | --- |
| `knell_beat_fresh{beat}` | gauge | `1` while the beat is within its deadline, `0` when overdue. Silence counts from process start, so an unpinged beat reads `1` for one deadline |
| `knell_beat_last_seen_timestamp_seconds{beat}` | gauge | Unix time of the last accepted ping, or of process start until the first ping |
| `knell_beat_deadline_seconds{beat}` | gauge | The configured deadline. Add it to the last-seen gauge to get when a beat fires. Compare it across instances to catch a `BEATS` mismatch |
| `knell_beats_received_total{beat}` | counter | Accepted pings. Unknown ids are refused and not counted |
| `knell_beat_outages_total{beat}` | counter | Outages detected, counted when the deadline passes and independent of delivery. Count outages with this one |
| `knell_outage_records_dropped_total{beat}` | counter | Ended-outage records discarded because the beat's queue was full, one per record. No notice for that outage will ever arrive |
| `knell_notifications_sent_total{kind}` | counter | Delivered notices by kind, `missing`, `recovered` or `history`, one per message. A history notice covering several outages counts once |
| `knell_notifications_failed_total{kind}` | counter | Sends that failed after their retries, one per message, with the record still queued. In practice `missing` and `history`, which the next sweep retries |
| `knell_notifications_dropped_total{kind}` | counter | Notices that will never be delivered, one per message. In practice `recovered`, the one kind sent once. Nothing retries a drop |
| `knell_pre_route_refusals_total{reason}` | counter | Requests refused before routing: `non_canonical_beat_path`, `host_not_allowed` or `auth_throttled`. A diagnostic, see below |
| `knell_http_requests_total{method,path,status}` | counter | Served requests by route template, never the raw path. Host and malformed-path refusals have `path="unmatched"`. A throttled `429` shows only in `knell_pre_route_refusals_total` |
| `knell_http_request_duration_seconds` | histogram | Request latency across all endpoints, with no labels |

Every `kind`, `reason` and configured `beat` series starts at zero, so `increase()` sees the first event after a cold start. `knell_http_requests_total` is the exception, because a status series appears with its first request.

A lost outage record counts on `knell_outage_records_dropped_total`, not on the notification counters, because a record is not a message. `failed` means wait, the notice is retried. Either `dropped` counter means nothing will arrive.

### Why a ping stopped landing

`knell_pre_route_refusals_total` has no alert rule of its own on purpose. A sender whose pings are refused is not feeding its beat, so that beat passes its deadline and `KnellBeatOverdue` fires anyway. Read the counter when a beat has gone missing and you need to know why. Its reasons are a malformed URL, a `Host` missing from `ALLOWED_HOSTS`, or a rotated token throttling every sender.

One refusal is missing from it by design. A request whose headers exceed the 8704-byte limit is answered `431` by Go's HTTP server before knell sees it, so it appears in no knell metric and no log line. A missing beat with no refusal recorded and no ping in `knell_http_requests_total` points to that case.

## Alerting

knell is the alert path for the things it watches, so rules about knell itself have to come from a second vantage point. That is your metrics stack scraping `/metrics` and your log stack reading its container log.

The five PromQL rules in [`alerts/promql.yaml`](../alerts/promql.yaml) go to Prometheus or the Mimir ruler, and the three LogQL rules in [`alerts/logql.yaml`](../alerts/logql.yaml) go to Loki's ruler. [Loading metric alert rules](https://github.com/cplieger/docs/blob/main/docs/monitoring.md#loading-metric-alert-rules) and [Loading an app's alert rules](https://github.com/cplieger/docs/blob/main/docs/monitoring.md#loading-an-apps-alert-rules) show how. The three log rules exist because their conditions leave no series to read at all. A knell that refuses its configuration exits before it opens its port, so it publishes no metrics and a crash-looping container is never scraped.

| Alert | Fires when | Severity |
| --- | --- | --- |
| `KnellTargetDown` | `up{job="knell"} == 0` for 15m, so knell is not being scraped and every beat it watches is unmonitored | critical |
| `KnellTargetAbsent` | `absent(up{job="knell"})` for 15m, so knell is not a scrape target at all | critical |
| `KnellBeatOverdue` | `knell_beat_fresh == 0` for 5m, so a beat is overdue and its missing notice may not have reached you | warning |
| `KnellNotifyFailing` | a send failed after retries, or a notice or an outage record was dropped for good | warning |
| `KnellRestartChurn` | knell restarted more than once inside a beat's deadline window | warning |
| `KnellExitedWithError` | knell logged its own exit: a refused configuration, a port it could not bind, an unknown command, or a stop that outlived its grace | critical |
| `KnellNoticeLostForGood` | a log line reports a notice nothing will retry, including the queued records a stop discards, which move no counter | warning |
| `KnellAcceptFailing` | the listener logged an accept failure, so pings stop landing while the process stays up | critical |

`KnellTargetDown` and `KnellTargetAbsent` are the floor under every other rule, because all the others read a knell metric and go quiet together when knell stops being scraped. They are critical because a dead-man switch that is itself dead reports nothing.

They are two rules because neither covers the other. `up == 0` catches a target that is configured and failing, and keeps its labels so the alert names the node. `absent(up{job="knell"})` catches a target that stopped existing, for example a dropped scrape target, a removed scrape config, or a deleted Kubernetes pod or ServiceMonitor. There `up` has no series and `up == 0` cannot match.

Use an exact `job` matcher, never a regex. A regex `absent()` asks whether any matching target is up. With several observers, one survivor would then hide every dead one, and its result carries no `job` label to route on. With several observers, alert per node on the `instance` label of the `up == 0` rule. Evaluate both rules somewhere other than the box knell runs on.

`KnellExitedWithError` fires before knell binds its port. Startup fails fast on purpose, because a dead-man switch with the wrong configuration is worse than none. A `BEATS` entry that is not `id:deadline`, a webhook that is not https, a missing or unreadable `BEAT_TOKEN` file, a taken port and a mistyped container command all end here. No metric can carry this, because the process publishes no `/metrics` and a crash-looping container is never scraped. The target rules report the silence 15 minutes later, without a cause. The log line has the cause on its `error` attribute.

`KnellAcceptFailing` reads the accept errors Go's HTTP server logs, which knell routes into its log at `ERROR`. Nothing else can see them, because a refused connection produces no request and moves no request series. The usual cause is an exhausted file-descriptor budget. Every beat then crosses its deadline, and a sender seeing a refused connection has no way to report it. The target rules do not cover this. The server retries the accept, so scrapes can keep succeeding and `up{job="knell"}` may never hold 0 for 15 minutes.

`KnellNotifyFailing` has three `or` legs on purpose. The `failed` leg means the notice is late and the 15-second sweep retries it, so you wait. Either `dropped` leg means nothing will arrive, and you rebuild the window from `knell_beat_last_seen_timestamp_seconds`. Drop the third leg and a permanently lost outage record pages nobody.

`KnellNoticeLostForGood` covers what the counters cannot reach. It keys on the `retryable=false` field every notice-loss line carries, which includes the losses a stop causes. Queued records and pending recovered notices die with the process and move no counter. A send cancelled mid-flight is in no counter either. The rule also matches a recovered notice whose send failed, and a teardown that outlived its grace while still holding notices. It overlaps `KnellNotifyFailing` on purpose, because the log line names the beat and the counters do not.

Keep `LOG_LEVEL` at its `info` default for `KnellNoticeLostForGood` to mean that. The loss lines sit at `info`, `warn` and `error`. At `LOG_LEVEL=warn` the notices abandoned mid-send are hidden. At `LOG_LEVEL=error` the shutdown losses are hidden too, and the rule covers only the one loss a counter already reports. `KnellExitedWithError` and `KnellAcceptFailing` key on `ERROR` and work at every level. Go renders the level in capitals, so the matchers read `level=ERROR`, never `level=error`.

Every restart re-arms each beat's full deadline, so an instance restarting more often than a beat's deadline never fires that beat's alert. `KnellRestartChurn` covers this. The shipped window is 26h, so set it to your longest beat deadline.

With several instances, point each sender at all of them and alert on `sum by (beat) (knell_beat_fresh)`. That way one instance being down lowers the count instead of paging falsely, and you can alert only when the count falls below the number of instances you require.

Thresholds and windows are starting points. Set the churn window to your longest beat deadline, match the `job` selector to your scrape config, and change the `container` selector to the label your log collector sets. Grafana Alloy's Docker log discovery sets `container` with no configuration. Promtail and other collectors may use `job` or `service`. Route by whatever labels your Alertmanager uses.
