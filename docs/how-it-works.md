# How knell works

This page explains how knell decides a beat is missing, which notices it sends and when, and how its HTTP endpoints answer. It is for operators who want to know exactly what a notice means or why a ping was refused.

## Deadlines start at boot

Every beat's deadline counts from the moment knell starts. A beat that never pings at all is reported missing one deadline after start, so a restart can never quietly disarm the switch.

The other side of this is that every restart re-arms each beat's full deadline. A knell instance that restarts more often than a beat's deadline never reports that beat. The `KnellRestartChurn` alert rule in [Monitoring and alerts](monitoring.md) covers this case.

knell checks every beat every 15 seconds. That check is called a sweep below.

## The notices

A live outage and one that is already over are reported differently. No notice announces a resolved outage as a beat that is down right now. Each notice is a one-line Discord message plus a card with the figures. The card shows how long the beat was silent, when the silence began and the `NODE_NAME` of the instance that saw it.

### Missing

knell sends a missing notice once per live outage, when a beat first passes its deadline. If delivery fails, because Discord or the network is down, the next sweep tries again, every 15 seconds until one send succeeds. The beat is marked notified only after a delivered send.

### Recovered

knell sends a recovered notice on the first accepted ping after a missing notice. Delivery makes up to three attempts, waiting a little longer and adding a small random delay between attempts. It also honors Discord's `Retry-After` value on a rate limit. A recovered notice is sent once. If those attempts fail, nothing retries it and it never arrives. It then counts on `knell_notifications_dropped_total{kind="recovered"}`, not as a failure you can wait out.

### Outage history

An outage that starts while an earlier missing notice is still waiting gets its own record, so it is never merged into the earlier one and lost. A record whose outage ended before it could be delivered is reported once, in the past tense. The notice says why it is late:

- "delivery was delayed" means sends failed, so check the webhook.
- "no delivery was ever attempted" means no sweep saw the outage before a ping ended it, or a sweep deferred it, so the webhook is not the place to look.

The second wording is used only while nothing about that outage has failed to send. A history notice that fails and is retried carries the webhook wording when it arrives. It never vouches for a webhook that just refused it.

Several ended outages for one beat become one summary card. It shows the number of outages, the longest one, the last recovery time and how many were delayed or never attempted. It is delivered in a single sweep, so a live outage queued behind a full backlog waits one sweep, not one per record. Because the notice says these outages are over, no recovered notice follows for them.

### Queued records

Each beat queues up to 8 outage records and reports them oldest first. When a beat's queue is full, the newest record is not queued, and the result depends on the outage:

- An outage a ping has already ended is dropped for good. Its record was the last trace of it, so no notice for it will ever arrive. `knell_outage_records_dropped_total{beat}` goes up by one and knell logs one warning for that outage. Rebuild the missed window from `knell_beat_last_seen_timestamp_seconds`.
- An outage still in progress loses nothing. `knell_beat_outages_total{beat}` already counted it, and it is queued and delivered once a slot opens. The full queue only delays the notice until a slot opens, so knell logs it at `debug` and moves no delivery counter.

A stop discards whatever is still queued. Each lost notice is logged with `retryable=false`, which the `KnellNoticeLostForGood` rule reads.

### The webhook URL stays secret

The webhook URL is treated as a credential. It never appears in a log line or an error message, including the errors knell logs when Discord rejects a notice.

## The endpoints

| Endpoint | Purpose |
| --- | --- |
| `POST /beat/{id}` | Records a ping when it carries `Authorization: Bearer <BEAT_TOKEN>`, and answers `{"ok":true}` |
| `GET /healthz` | Liveness, answers `{"status":"OK"}` |
| `GET /metrics` | Prometheus metrics |

### Answers a ping can get

| Status | Meaning |
| --- | --- |
| `200` | The ping was recorded |
| `401` | The token is missing or wrong |
| `403` | The `Host` is not in `ALLOWED_HOSTS` |
| `404` | The id is not in `BEATS`, or the URL has an empty, extra or repeated path segment |
| `405` | Any method other than `POST` |
| `429` | Too many pings with a wrong token, with a `Retry-After` hint |
| `431` | The request headers are larger than 8704 bytes |
| `503` | knell is shutting down |

Only `POST` records. `GET` and `HEAD` are answered `405` and never feed the switch. Nothing that merely fetches a URL can keep a beat looking alive, such as a link preview, a crawler, an uptime prober or an image on a web page.

Pings without a valid token share one throttle budget and get `429` once it is spent. That limits both guessing and the log lines a bad sender writes. A ping with the right token is never throttled, however many senders you run.

### Request bodies and headers

knell ignores a ping's body, so a webhook-shaped sender such as an Alertmanager `webhook_configs` target or a CI notification hook can point at it unchanged. A body over 1 MiB still records the ping and answers `{"ok":true}`. knell logs one `warn` line and closes that connection after the answer.

Headers are limited the other way round, because knell must read them before it knows anything about the caller. It reads at most 8704 bytes of request headers and answers `431` to a larger block. A proxy that adds many `X-Forwarded-*`, tracing and cookie headers can produce a block that large. If pings start failing that way, trim the headers the proxy adds.

### Probe logging

`/healthz` and `/metrics` are logged as machine probes. A successful probe or scrape is logged at `debug`, so set `LOG_LEVEL=debug` to confirm it reaches knell. An answered `4xx` is logged at `warn`, and an answered `5xx` at `error`. A scrape that never reaches knell produces no request log.

## Several instances

You can run knell on several hosts and point each sender at all of them. Each instance sends its own notices and publishes its own metrics. In your metrics stack, `sum by (beat) (knell_beat_fresh)` counts how many instances see each beat as on time. One instance going down then lowers the count instead of raising a false alarm.
