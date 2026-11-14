# Monitoring and alerts

This page lists what knell logs and the metrics it publishes, and how to load the alert rules that watch knell itself. [The Grafana dashboard](dashboard.md) has its own page. It is for anyone who runs knell. The Discord notices need nothing else, and the rules below need a metric store or Loki. [Watching your alert pipeline](alert-pipeline.md) shows how to make knell watch your Alertmanager.

## What you get

| You run | You get | You set up |
| --- | --- | --- |
| Prometheus, Mimir or VictoriaMetrics, with Alertmanager | Every Discord notice, the six PromQL alerts about knell itself, `up` and the [dashboard](dashboard.md) | A scrape job named `knell` for port `9190`, and [`alerts/promql.yaml`](../alerts/promql.yaml) on a ruler that does not run beside knell |
| Loki and Alertmanager, no metric store | Every Discord notice, and the two LogQL alerts: knell exited with an error, or a notice was lost for good | [`alerts/logql.yaml`](../alerts/logql.yaml) on Loki's ruler, with `LOG_LEVEL` at `info` |
| Grafana-managed alerting only | The same rules, imported into Grafana | A Prometheus or Loki data source, and the rules |
| Docker or Unraid only | Every Discord notice, and Docker health | An uptime prober on `/healthz`, because nothing else tells you that knell itself stopped |

## Metrics

`GET /metrics` is always on, on the same port as the pings, `9190` by default. It has no login, so keep the port on a network you trust. When `ALLOWED_HOSTS` is set, add the name your scraper uses, or knell refuses the scrape with `403` and the down alerts fire. The `beat` label carries the ids you configured in `BEATS`.

| Metric | Type | Notes |
| --- | --- | --- |
| `knell_beat_fresh{beat}` | gauge | `1` while the beat is within its deadline, `0` when overdue. Silence counts from process start, so an unpinged beat reads `1` for one deadline |
| `knell_beat_last_seen_timestamp_seconds{beat}` | gauge | Unix time of the last accepted ping, or of process start until the first ping |
| `knell_beat_deadline_seconds{beat}` | gauge | The configured deadline. Add it to the last-seen gauge to get when a beat fires. Compare it across instances to catch a `BEATS` mismatch |
| `knell_beats_received_total{beat}` | counter | Accepted pings. Unknown ids are refused and not counted |
| `knell_beat_outages_total{beat}` | counter | Outages detected, counted when the deadline passes and independent of delivery. Count outages with this one |
| `knell_outage_records_dropped_total{beat}` | counter | Ended-outage records discarded because the beat's queue was full, one per record. No notice for that outage will ever arrive |
| `knell_notifications_sent_total{kind}` | counter | Delivered notices by kind, `missing`, `recovered` or `history`, one per message, a repeat counting as `missing`. A history notice covering several outages counts once |
| `knell_notifications_failed_total{kind}` | counter | Sends that failed after their retries, one per message, with the record still queued. In practice `missing` and `history`, which the next sweep retries |
| `knell_notifications_dropped_total{kind}` | counter | Notices that will never be delivered, one per message. In practice `recovered`, the one kind sent once. Nothing retries a drop |
| `knell_listener_accept_errors_total` | counter | Failed accepts on the listener, usually an exhausted file-descriptor budget. The server retries each one. A shutdown is not counted |
| `knell_pre_route_refusals_total{reason}` | counter | Requests refused before routing: `non_canonical_beat_path`, `host_not_allowed` or `auth_throttled`. A diagnostic, see below |
| `knell_http_requests_total{method,path,status}` | counter | Served requests by route template, never the raw path. Host and malformed-path refusals have `path="unmatched"`. A throttled `429` shows only in `knell_pre_route_refusals_total` |
| `knell_http_request_duration_seconds` | histogram | Request latency across all endpoints, with no labels |
| `knell_build_info{version}` | gauge | Always `1`. `version` is the running release. A rule joins on it to read `process_*` for knell alone, whatever the scrape job is named |

The page also carries the Go runtime and process families (`go_*`, `process_*`) under the names Go's official Prometheus client uses, so stock Go dashboards work.

Every `kind`, `reason` and configured `beat` series, and the accept-error counter, starts at zero, so `increase()` sees the first event after a cold start. `knell_http_requests_total` is the exception, because a status series appears with its first request.

A lost outage record counts on `knell_outage_records_dropped_total`, not on the notification counters, because a record is not a message. Rebuild its window from `knell_beat_last_seen_timestamp_seconds`. `failed` means wait, the notice is retried. Either `dropped` counter means nothing will arrive.

### Why a ping stopped landing

`knell_pre_route_refusals_total` has no alert rule of its own on purpose. A sender whose pings are refused is not feeding its beat, so that beat passes its deadline and `KnellBeatOverdue` fires anyway. Read the counter when a beat has gone missing and you need to know why. Its reasons are a malformed URL, a `Host` missing from `ALLOWED_HOSTS`, or a rotated token throttling every sender.

One refusal is missing from it by design. A request whose headers exceed the 8704-byte limit is answered `431` by Go's HTTP server before knell sees it, so it appears in no knell metric and no log line. A missing beat with no refusal recorded and no ping in `knell_http_requests_total` points to that case.

## Logs

knell writes logfmt to standard error, so `docker logs knell` and any Docker log collector read it with no configuration. Go renders the level in capitals, `level=ERROR`. At startup it logs one `watching beat` line per beat, one `configuration loaded` line with the settings it runs with and then `listening`.

Every line that reports a lost or delayed notice carries a `retryable` field. `retryable=true` means the next sweep tries again, so the notice is late. `retryable=false` means nothing will retry it, so rebuild the missed window yourself.

The LogQL rules read two things in the log. One is the `knell exited with error` line, which knell writes once when it exits with an error. The other is the `retryable=false` field. Keep `LOG_LEVEL` at `info` for those rules.

## Alerting

knell is the alert path for the things it watches, so rules about knell itself have to come from a second vantage point. That is your metrics stack scraping `/metrics` and your log stack reading its container log.

The six PromQL rules in [`alerts/promql.yaml`](../alerts/promql.yaml) go to Prometheus, Mimir or any ruler that reads Prometheus rules, and the two LogQL rules in [`alerts/logql.yaml`](../alerts/logql.yaml) go to Loki's ruler. Load both files. They are complementary, and no alert appears in both. [Loading metric alert rules](https://github.com/cplieger/docs/blob/main/docs/monitoring.md#loading-metric-alert-rules) and [Loading an app's alert rules](https://github.com/cplieger/docs/blob/main/docs/monitoring.md#loading-an-apps-alert-rules) show how. The two log rules exist because their conditions leave no series. Either knell exits before it opens its port, or a stop discards notices.

| Alert | Fires when | Severity |
| --- | --- | --- |
| `KnellTargetDown` | `up{job="knell"} == 0` for 15m, so knell is not being scraped and every beat it watches is unmonitored | critical |
| `KnellTargetAbsent` | `absent(up{job="knell"})` for 15m, so knell is not a scrape target at all | critical |
| `KnellBeatOverdue` | `knell_beat_fresh == 0` for 5m, so a beat is overdue and its missing notice may not have reached you | warning |
| `KnellAcceptFailing` | the listener failed to accept connections in the last 15 minutes (`knell_listener_accept_errors_total`), so pings stop landing while the process stays up | critical |
| `KnellNotifyFailing` | a send failed after retries, or a notice or an outage record was dropped for good | warning |
| `KnellRestartChurn` | knell restarted more than once inside a beat's deadline window | warning |
| `KnellExitedWithError` | knell logged its own exit: a refused configuration, a port it could not bind, an unknown command, or a stop that outlived its grace | critical |
| `KnellNoticeLostForGood` | a log line reports a notice nothing will retry, including the queued records a stop discards, which move no counter | warning |

`KnellTargetDown` and `KnellTargetAbsent` are the floor under every other rule, because all the others read a knell metric and go quiet together when knell stops being scraped. They are critical because a dead-man switch that is itself dead reports nothing.

They are two rules because neither covers the other. `up == 0` catches a target that is configured and failing, and keeps its labels so the alert names the node. `absent(up{job="knell"})` catches a target that stopped existing, for example a dropped scrape target, a removed scrape config, or a deleted Kubernetes pod or ServiceMonitor. There `up` has no series and `up == 0` cannot match.

Use an exact `job` matcher, never a regex. A regex `absent()` asks whether any matching target is up. With several observers, one survivor would then hide every dead one, and its result carries no `job` label to route on. With several observers, alert per node on the `instance` label of the `up == 0` rule. Evaluate both rules somewhere other than the box knell runs on.

In the healthy case `KnellBeatOverdue` repeats knell's own Discord notice. It exists for a wedged sender or a Discord outage, so route it to a different receiver or drop it.

`KnellAcceptFailing` reads `knell_listener_accept_errors_total`. A refused connection produces no request and moves no request series, and the usual cause is an exhausted file-descriptor budget. Every beat then crosses its deadline, and a sender seeing a refused connection has no way to report it. The counter sees what `up` misses. The server retries each accept, so scrapes can keep succeeding between failures and `up{job="knell"}` may never hold 0 for 15 minutes.

`KnellExitedWithError` fires before knell binds its port. Startup fails fast on purpose, because a dead-man switch with the wrong configuration is worse than none. A `BEATS` entry that is not `id:deadline`, a webhook that is not https, a missing or unreadable `BEAT_TOKEN` file, a taken port and a mistyped container command all end here. No metric can carry this, because the process publishes no `/metrics` and a crash-looping container is never scraped. The target rules report the silence 15 minutes later, without a cause. The log line has the cause on its `error` attribute.

`KnellNotifyFailing` has three `or` legs on purpose. The `failed` leg means the notice is late and the 15-second sweep retries it, so you wait. Either `dropped` leg means nothing will arrive, and you rebuild the window from `knell_beat_last_seen_timestamp_seconds`. Drop the third leg and a permanently lost outage record pages nobody.

`KnellNoticeLostForGood` covers what the counters cannot reach. It keys on the `retryable=false` field every notice-loss line carries, which includes the losses a stop causes. Queued records and pending recovered notices die with the process and move no counter. A send cancelled mid-flight is in no counter either. The rule also matches a recovered notice whose send failed, and a teardown that outlived its grace while still holding notices. It overlaps `KnellNotifyFailing` on purpose, because the log line names the beat and the counters do not.

Keep `LOG_LEVEL` at its `info` default for `KnellNoticeLostForGood` to mean that. The loss lines sit at `info`, `warn` and `error`. At `LOG_LEVEL=warn` the notices abandoned mid-send are hidden. At `LOG_LEVEL=error` the shutdown losses are hidden too, and the rule covers only the one loss a counter already reports. `KnellExitedWithError` keys on `ERROR` and works at every level. Go renders the level in capitals, so the matchers read `level=ERROR`, never `level=error`.

Every restart re-arms each beat's full deadline, so an instance restarting more often than a beat's deadline never fires that beat's alert. `KnellRestartChurn` covers this. The shipped window is 26h, so set it to your longest beat deadline.

With several instances, point each sender at all of them, then uncomment `KnellBeatQuorumLow` in `alerts/promql.yaml` and use it in place of `KnellBeatOverdue`. It alerts when `sum by (beat) (knell_beat_fresh)`, the number of instances that still see a beat on time, falls below its count. Set the count to the number of instances you require. One instance can be down or overdue without an alert only when you run more instances than that count.

Thresholds and windows are starting points. Set the churn window to your longest beat deadline, match the `job` selector to your scrape config, and change the `container` selector to the label your log collector sets. Grafana Alloy's Docker log discovery sets `container` with no configuration. Promtail and other collectors may use `job` or `service`.

## Knowing the app is alive

| Route | Method | Port | Login | Answers |
| --- | --- | --- | --- | --- |
| `/healthz` | `GET`, `HEAD` | `LISTEN_ADDR`, `9190` by default | none. `ALLOWED_HOSTS` applies, except to knell's own `knell health` check, unless `LISTEN_ADDR` names a host other than `localhost` | `200 {"status":"ok"}`, or `503 {"status":"unhealthy","reasons":[...]}` |
| `/metrics` | `GET` | the same port | none. Keep the port on your network | Prometheus text format |

**What healthy means.** knell answers `/healthz` itself, so a `200` proves the listener accepts and serves requests. Nothing answers before the listener is up. From the moment knell begins to shut down, it answers `503` until the listener closes. knell keeps this verdict in memory, so a read-only root filesystem never makes it unhealthy. Discord being unreachable never makes knell unhealthy. It shows on `knell_notifications_failed_total` and in the log.

**Docker health.** The image checks itself every 30 seconds with `knell health`, which asks `/healthz` on the container's loopback, or on the address `LISTEN_ADDR` names. A check fails when no answer comes within 4 seconds. Docker marks the container unhealthy after 3 failures, and allows 15 seconds after a start. Docker's health status notifies nobody on its own, and Docker does not restart an unhealthy container.

**Who is told.**

| You run | A dead knell | A wedged knell |
| --- | --- | --- |
| Prometheus with these rules | `KnellTargetDown`, or `KnellTargetAbsent` once the target is gone | `KnellAcceptFailing` for a listener that cannot accept |
| Loki only | `KnellExitedWithError` when it exits with an error. Nothing reports a kill, so add an uptime prober on `/healthz` | an uptime prober on `/healthz` |
| Docker or Unraid only | an uptime prober on `/healthz`, or something that watches Docker health | the same |

One wedge reaches the down alerts instead. At `LOG_LEVEL=debug`, a log destination that stops reading blocks the loop that judges beats, and it also blocks every scrape, because each scrape's access line waits for the same output. The scrape then fails, so `KnellTargetDown` fires. At `info` neither writes a line per request or per check.

**Probing from outside.** Check `GET http://<host>:9190/healthz` and pass on status 200 only. Do not match the body. Use a 5-second timeout, check every 60 seconds, alert after 3 failures, and wait 15 seconds after a first start. Address knell by a name in `ALLOWED_HOSTS`, or knell refuses the request with `403`. Run the prober somewhere other than the host knell runs on, so it can report that host going down. A Kubernetes liveness probe that must ignore readiness uses a `tcpSocket` probe on the port.
