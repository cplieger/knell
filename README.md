# knell

[![Image Size](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/knell/badges/size.json)](https://github.com/cplieger/knell/pkgs/container/knell)
![Platforms](https://img.shields.io/badge/platforms-amd64%20%7C%20arm64-blue)
![base: scratch](https://img.shields.io/badge/base-scratch-000000)
[![Test coverage](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/knell/badges/coverage.json)](https://github.com/cplieger/knell/actions/workflows/coverage.yml)
[![Mutation](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/knell/badges/mutation.json)](https://github.com/cplieger/knell/issues?q=label%3Agremlins-tracker)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/13764/badge)](https://www.bestpractices.dev/projects/13764)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/cplieger/knell/badge)](https://scorecard.dev/viewer/?uri=github.com/cplieger/knell)
[![SBOM](https://img.shields.io/badge/SBOM-SPDX-1D4ED8)](https://github.com/cplieger/knell/releases)

A dead man's switch in a single tiny container: things ping it while they're alive, and when the pings stop, it rings a Discord webhook.

## What it does

Monitoring tells you when something visibly breaks. It stays quiet when the thing that was supposed to run simply never ran: the cron job that silently stopped, the alerting pipeline that died along with its own ability to alert. knell watches for that silence.

You configure named beats, each with a deadline. Anything that can send an HTTP request pings its beat (`POST /beat/<id>`); if a beat stays silent past its deadline, knell posts a missing notice to your Discord webhook, and a recovered notice when the pings return. Per-beat freshness is also exposed as Prometheus metrics, so a metrics stack can aggregate several knell instances into quorum views.

- One binary on a `scratch` base: no shell, no libc, no dependencies to patch
- Deadline clock starts at boot: a beat that never pings at all still alerts one deadline after start, so a restart never silently disarms the switch (the flip side: each restart re-arms full deadlines; see the restart-churn rule under Alerting)
- One missing notice per live outage (delivery is retried every sweep until it succeeds), one recovered notice when the beat returns
- Outages that were already over before anything could be sent are reported once, in the past tense, with the reason the notice is late, instead of arriving as apparent new failures
- Unknown beat ids are rejected with 404 and never create metric series

## Quick start

Images are published to GHCR (`ghcr.io/cplieger/knell`) and Docker Hub
(`cplieger/knell`).

```yaml
# compose.yaml
services:
  knell:
    image: ghcr.io/cplieger/knell:latest
    container_name: knell
    restart: unless-stopped
    environment:
      BEATS: "cron-backup:26h,pipeline-watchdog:20m"
      DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/..."
      NODE_NAME: "server-1"
    ports:
      - "9190:9190"
```

Then ping a beat from the thing being watched:

```sh
# at the end of the daily backup script
curl -fsS -X POST http://knell:9190/beat/cron-backup
```

Silence past the deadline rings the bell:

> 🚨 [knell server-1] beat **cron-backup** MISSING: silent for 26h0m1s. The sender is down, or nothing on its path can reach this observer.

## Configuration reference

| Variable | Description | Default | Required |
| --- | --- | --- | --- |
| `BEATS` | comma-separated `id:deadline` list, e.g. `api:20m,backup:26h`. Ids match `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`; deadlines are Go durations, minimum `30s`, maximum 64 beats | _none_ | Yes |
| `DISCORD_WEBHOOK_URL` | the webhook notifications post to, `https` only: the URL's own path carries the credential, so a plain-http webhook would put it on the wire in cleartext. `DISCORD_WEBHOOK_URL_FILE` points at a mounted secret file instead | _none_ | Yes |
| `NODE_NAME` | names this observer instance in every notification | container hostname | No |
| `BEAT_TOKEN` | when set, `/beat/{id}` requires `Authorization: Bearer <token>` from senders. Empty leaves the endpoint open. `BEAT_TOKEN_FILE` points at a mounted secret file instead | _(unset)_ | No |
| `LISTEN_ADDR` | TCP listen address (`host:port`) | `:9190` | No |
| `LOG_LEVEL` | `debug`/`info`/`warn`/`error`; unknown falls back to `info` | `info` | No |

knell serves plain HTTP, so `BEAT_TOKEN` crosses the network in cleartext. Put a TLS reverse proxy in front, or keep pings on a trusted network.

A malformed `BEATS` or `DISCORD_WEBHOOK_URL` fails startup rather than falling back, and so does a webhook URL on any scheme other than `https`. When a `_FILE` variable is set but its file cannot be read, because it is missing, unreadable, or empty, startup fails instead of falling back to the plain variable; only an unset `_FILE` variable falls back. A dead-man switch running with the wrong config is worse than one that refuses to start.

## Endpoints

| Endpoint | Purpose |
| -------- | ------- |
| `POST /beat/{id}` | record a ping (`GET` works too, for ad-hoc senders); `{"ok":true}` on success, 404 for unknown ids |
| `GET /healthz` | liveness (`{"status":"OK"}`) |
| `GET /metrics` | Prometheus exposition |

Request bodies on `/beat/{id}` are ignored, so webhook-shaped senders (an Alertmanager `webhook_configs` target, a CI notification hook) can point at it unchanged.

Because `GET /beat/{id}` records a ping exactly like `POST`, keep beat URLs away from anything that fetches links automatically: a chat client's URL preview, a crawler, an uptime prober. An automated fetch feeds the switch and can mask a dead sender. `HEAD` requests are rejected with 405 and never record a ping, so a HEAD-only prober cannot feed the switch.

`/healthz` and `/metrics` are logged as machine probes: a successful probe or scrape lands at `debug` (out of the log at the default level, visible under `LOG_LEVEL=debug` when the question is whether the prober arrives at all), while one answering 4xx or 5xx lands at `warn`/`error`. So a scrape that stopped landing shows up in the log without raising the level.

## Notification semantics

A live incident and one that is already over are reported differently: nothing an operator reads should announce a resolved outage as a beat that is down right now.

- **Missing**: sent once per live outage, when a beat first passes its deadline. A failed delivery (Discord outage, network) is retried on every 15s sweep until one succeeds; the beat is only marked notified after a delivered send.
- **Recovered**: sent on the first accepted ping after a missing notice, best-effort. Delivery uses bounded retries with jittered backoff and honors `Retry-After` on rate limits. It is fire-once: the queued transition is consumed before the send, so a delivery that still fails has nothing left to retry from and that recovery notice will never arrive. It therefore counts as `knell_notifications_dropped_total{kind="recovered"}`, not as a failure you can wait out.
- **Ended outages**: an outage that starts while an earlier missing notice is still undelivered gets its own queued record instead of being collapsed into that earlier one and lost. Records whose outage has already ended by the time they can be delivered are reported once in the past tense, and the notice says why it is late, because the two reasons ask for different things. An outage whose alert was still undelivered when the beat came back points at the webhook:

  > 🕓 [knell server-1] beat **cron-backup** was missing for 12m0s, recovered at 2026-07-23 14:07 UTC. This notice is late: its alert was still undelivered when the beat returned - check the webhook.

  An outage that ended before a sweep detected it never had an alert to deliver, so there is nothing to check:

  > 🕓 [knell server-1] beat **cron-backup** was missing for 12m0s, recovered at 2026-07-23 14:07 UTC. This notice is late only because the outage ended before a sweep detected it - nothing was wrong with delivery.

  Several become one summary. When the batch mixes the two reasons, it reports both counts rather than blaming one for all of them:

  > 🕓 [knell server-1] beat **cron-backup** had 3 outages: longest 47m0s, last recovered at 2026-07-23 14:07 UTC. 2 had an undelivered alert (check the webhook), 1 ended before a sweep detected it.

  The whole run of ended outages goes out in a single sweep, so a genuinely live outage queued behind it waits one sweep rather than one sweep per stale record. Because the notice states the outages are over, no recovered notice follows for them.
- **Queued outages**: each beat queues up to 8 records and reports them oldest first. When a beat's queue is full, the newest record is not queued, and the two cases that can arise differ in consequence:
  - an outage a ping has already ended is **dropped for good**: its record was the last trace of it, so no notice for it will ever arrive. `knell_notifications_dropped_total{kind="missing"}` increments and a warning is logged, once for that outage. Reconstruct the missed window from `knell_beat_last_seen_timestamp_seconds`.
  - an outage still in progress **loses nothing**: it stays detected (`knell_beat_outages_total{beat}` already counted it), and it is queued and delivered once a slot opens. That is ordinary back-pressure while notifications are failing, so it is logged at debug level and moves no delivery counter.
- The webhook URL is treated as a secret: it is never logged and never appears in error messages.

## Metrics

| Metric | Type | Notes |
| ----- | ------- | ----- |
| `knell_beat_fresh{beat}` | gauge | 1 = last ping within deadline, 0 = overdue. The aggregation input for multi-observer quorum rules |
| `knell_beat_last_seen_timestamp_seconds{beat}` | gauge | Unix time of the last accepted ping (process start until the first ping) |
| `knell_beats_received_total{beat}` | counter | accepted pings; unknown ids are rejected, not counted |
| `knell_beat_outages_total{beat}` | counter | outages detected per beat, counted when the deadline is crossed and independent of any delivery. Count outages with this one, not with the notification counters |
| `knell_notifications_sent_total{kind}` | counter | delivered webhook notifications (`missing`, `recovered`, `history`), one per delivered message: a `history` message covering several ended outages counts once |
| `knell_notifications_failed_total{kind}` | counter | delivery attempts that failed after retries and will be retried, one per failed message. This counter means exactly that: something was sent, did not get through, and its record is still queued. In practice that is `missing` and `history`, which the next sweep tries again |
| `knell_notifications_dropped_total{kind}` | counter | notifications that will never be delivered, either because a fire-once `recovered` send failed or because a record was discarded when the per-beat queue was full. Nothing retries a drop: reconstruct the missed window from `knell_beat_last_seen_timestamp_seconds` |

Plus standard `go_*` / `process_*` runtime metrics.

## Alerting

knell is itself the alert path for the things it watches, so alert rules about knell should come from a second vantage point (your metrics stack scraping `/metrics`). Two rules cover it:

```yaml
# A beat is overdue but the missing notification may not have reached you
# (Discord outage): the metric is the ground truth.
- alert: KnellBeatOverdue
  expr: knell_beat_fresh == 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "beat {{ $labels.beat }} is overdue on {{ $labels.instance }}"

# knell could not get a notification through, for one of two reasons the
# counters keep apart:
#   failed  = a delivery attempt failed after retries (the webhook is
#             unreachable) and its record is still queued, so it is retried
#             every 15s sweep. The notice is late, not lost: wait for it.
#   dropped = the notice will never arrive, either because a fire-once
#             recovered send failed or because a record was discarded when
#             its queue was full. Reconstruct the missed window from
#             knell_beat_last_seen_timestamp_seconds.
- alert: KnellNotifyFailing
  expr: >
    increase(knell_notifications_failed_total[15m]) > 0
    or increase(knell_notifications_dropped_total[15m]) > 0
  for: 0m
  labels:
    severity: warning
  annotations:
    summary: "knell on {{ $labels.instance }} failed to deliver or dropped a notification"
```

One caveat comes with the boot-armed clock: every restart re-arms each beat's full deadline, so an observer restarting more often than a beat's deadline never fires that beat's alert. The runtime metrics already expose this; alert on restart churn within your longest deadline window:

```yaml
# knell restarting faster than a beat's deadline (26h here) keeps re-arming
# that beat's clock; the alert for an ongoing outage is deferred each time.
- alert: KnellRestartChurn
  expr: changes(process_start_time_seconds{job="knell"}[26h]) > 1
  for: 0m
  labels:
    severity: warning
  annotations:
    summary: "knell on {{ $labels.instance }} keeps restarting within a beat deadline window; boot-armed clocks are re-armed before they can fire"
```

Running several instances? Point each sender at all of them and aggregate: `sum by (beat) (knell_beat_fresh)` gives an N-of-M quorum view where one observer being down degrades the count instead of paging falsely.

Thresholds and windows are starting points: set the churn window to your longest beat deadline, match the `job` selector to your scrape config, and route by whatever labels your Alertmanager uses.

## Healthcheck

The image bakes a shell-less healthcheck: `knell health` checks a marker file the server touches once its listener is bound and removes on shutdown. Nothing to configure; `docker ps` shows `healthy` once knell is serving.

## Security

The image runs as a non-root numeric user (65534) on `scratch` and writes only its `/tmp` health marker. A hardened deployment profile:

```yaml
    read_only: true
    cap_drop: [ALL]
    security_opt:
      - no-new-privileges:true
    tmpfs:
      - /tmp:rw,noexec,nosuid,nodev,size=16m,mode=1777
```

## Building from source

```sh
go build -trimpath -ldflags="-s -w" -o knell .
# or
docker build -t knell .
```

## Disclaimer

This project is built with care and follows security best practices, but it is intended for personal / self-hosted use. No guarantees of fitness for production environments. Use at your own risk.

This project was built with AI-assisted tooling using [Claude](https://claude.com), [GPT](https://openai.com), and [Kiro](https://kiro.dev). The human maintainer defines architecture, supervises implementation, and makes all final decisions.

## License

GPL-3.0. See [LICENSE](LICENSE).
