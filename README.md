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
      BEAT_TOKEN: "CHANGEME"  # invalid placeholder; required, min 16 bytes: openssl rand -hex 16
      NODE_NAME: "server-1"
    ports:
      - "9190:9190"
```

Then ping a beat from the thing being watched, presenting the token:

```sh
# at the end of the daily backup script
curl -fsS -X POST -H "Authorization: Bearer $BEAT_TOKEN" http://knell:9190/beat/cron-backup
```

Silence past the deadline rings the bell:

> 🚨 [knell server-1] beat **cron-backup** MISSING: silent for 26h0m1s. Nothing has pinged it in time: check the sender, its path to this observer, and that anything is pinging this beat id at all.

## Configuration reference

| Variable | Description | Default | Required |
| --- | --- | --- | --- |
| `BEATS` | comma-separated `id:deadline` list, e.g. `api:20m,backup:26h`; whitespace around an entry and around its colon is ignored, so `api:20m, backup:26h` is the same list. Ids match `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`; deadlines are Go durations of at least `30s`; at most 64 beats | _none_ | Yes |
| `DISCORD_WEBHOOK_URL` | the webhook notifications post to. `https` only, and it must carry the credential path Discord issues (`/api/webhooks/{id}/{token}`). `DISCORD_WEBHOOK_URL_FILE` reads it from a mounted secret file instead | _none_ | Yes |
| `NODE_NAME` | names this observer in every notification; maximum 256 bytes, since it prefixes every notice and Discord caps a message at 2000 characters | container hostname | No |
| `BEAT_TOKEN` | the bearer token every sender presents as `Authorization: Bearer <token>` on `POST /beat/{id}`, and the endpoint's only gate. Required, 16 to 512 bytes, and verified exactly as configured: generate one with `openssl rand -hex 16`. `BEAT_TOKEN_FILE` reads it from a mounted secret file instead | _none_ | Yes |
| `LISTEN_ADDR` | TCP listen address (`host:port`) | `:9190` | No |
| `ALLOWED_HOSTS` | comma-separated exact-match `Host` allowlist (bare hostnames or IPs with an optional port, no scheme or path), e.g. `knell.internal,10.0.0.5`. Unset accepts every `Host`; set, any other `Host` is refused 403 `host_not_allowed` on every endpoint, which is what blocks DNS rebinding from a browser inside your network. It covers `/healthz` and `/metrics` too, so list every name your probes and Prometheus scraper use, not just the browser-facing one. The baked `knell health` check reads a marker file and sends no request, so no allowlist can break it | _(unset)_ | No |
| `TRUSTED_PROXIES` | comma-separated CIDRs or bare IPs of the reverse proxies in front of knell, e.g. `10.0.0.0/24,192.168.1.5`. Their `X-Forwarded-For` is believed, so the access line's `client_ip` names the real sender; unset honors no forwarded header. Set it when you put a TLS proxy in front, or every access line — including the 401s a token-guessing run writes — names the proxy instead of an address you can block. List exactly those hops: a range wider than your proxies lets anything inside it choose its own `client_ip`. Malformed entries are logged and dropped rather than failing startup | _(unset)_ | No |
| `LOG_LEVEL` | `debug`/`info`/`warn`/`error`; unknown falls back to `info` | `info` | No |

knell serves plain HTTP, so `BEAT_TOKEN` crosses the network in cleartext on every ping. It is the only thing standing between a stranger who can reach the port and a forged heartbeat, and anything that can read one ping can replay it forever: put a TLS reverse proxy in front, or keep pings on a network you trust to that same standard.

`BEAT_TOKEN` gates `/beat/{id}` only. `/healthz` and `/metrics` stay open on the same port so probes and scrapes keep working, and `/metrics` publishes every configured beat id with its last-seen timestamp and freshness, so anyone who can reach the port can enumerate the beats and see which one is about to fire, token or no token. Publish the port to a trusted network only (the compose example maps it on every host interface), or put an authenticating proxy in front of `/metrics`. A trusted network is not enough against a browser: a page an operator opens can make their browser reach knell under a hostname an attacker controls, which is what `ALLOWED_HOSTS` refuses. Check that it armed in the `configuration loaded` line knell logs at startup, where `allowed_hosts=allowlist(2)` names how many hosts the gate holds and `allowed_hosts=any` means every `Host` is accepted — which is also what a misspelled variable name looks like.

Invalid configuration fails startup rather than falling back to a default, because a dead-man switch running with the wrong config is worse than one that refuses to start. That covers a `BEATS` entry that is not `id:deadline` with a valid id and a Go duration (blank segments between commas are skipped, so a trailing comma is fine), a webhook URL that is not an `https` Discord webhook path, a `NODE_NAME` over 256 bytes, an `ALLOWED_HOSTS` entry no `Host` could ever match, and a `BEAT_TOKEN` no sender could present as written: unset, empty, outside the 16-to-512-byte bounds, carrying surrounding spaces or tabs, or carrying a control character HTTP forbids in a header value. Whitespace is refused rather than trimmed, since rewriting the token would arm the gate for a value you did not configure, reject every ping while reporting itself gated, then declare every beat missing one deadline later. A `_FILE` variable that is set but missing, unreadable, or empty fails startup too; only an unset one falls back to the plain variable.

## Endpoints

| Endpoint | Purpose |
| -------- | ------- |
| `POST /beat/{id}` | record a ping, with `Authorization: Bearer <BEAT_TOKEN>`; `{"ok":true}` on success, 401 without the token, 404 for unknown ids, 405 for any other method |
| `GET /healthz` | liveness (`{"status":"OK"}`) |
| `GET /metrics` | Prometheus exposition |

Request bodies on `/beat/{id}` are ignored, so webhook-shaped senders (an Alertmanager `webhook_configs` target, a CI notification hook) can point at it unchanged. A payload over 1 MiB still records the ping and answers `{"ok":true}`; it logs one `warn` line and loses keep-alive on that connection, never its ping. A ping is never refused for its payload: the body is not what the switch is listening for.

Headers are bounded the other way round, because they are the part knell has to parse before it knows anything about the caller: at most 8704 bytes of request headers are read, and a request whose header block is larger is answered 431. That figure is the 512-byte `BEAT_TOKEN` maximum plus 8 KiB for the request line, `Host`, the `Bearer` prefix, and whatever sits in front of knell adds. The 8 KiB matches the default per-header-LINE ceiling of nginx and Apache; their whole-block defaults are higher, so a proxy that piles on `X-Forwarded-*`, tracing and cookie headers can still deliver a block knell answers 431. If pings start failing that way, trim the headers the proxy adds.

Only `POST` records. `GET` and `HEAD` are answered with 405 and never feed the switch, so nothing that merely fetches a URL — a chat client's link preview, a crawler, an uptime prober, an `<img>` on a page an operator opens — can keep a beat looking alive. Pings that present no valid token share one throttle budget and are answered 429 with a `Retry-After` hint once it is spent, which caps both guessing and the log flood a bad sender would write; a ping with the right token is never throttled, however many senders you run.

`/healthz` and `/metrics` are logged as machine probes: a successful probe or scrape lands at `debug` (visible under `LOG_LEVEL=debug` when the question is whether the prober arrives at all), while one answering 4xx or 5xx lands at `warn`/`error`. So a scrape that stopped landing shows up in the log without raising the level.

## Notification semantics

A live incident and one that is already over are reported differently: nothing an operator reads should announce a resolved outage as a beat that is down right now.

- **Missing**: sent once per live outage, when a beat first passes its deadline. A failed delivery (Discord outage, network) is retried on every 15s sweep until one succeeds; the beat is only marked notified after a delivered send.
- **Recovered**: sent on the first accepted ping after a missing notice, best-effort. Delivery uses bounded retries with jittered backoff and honors `Retry-After` on rate limits. It is fire-once: the queued transition is consumed before the send, so a delivery that still fails has nothing left to retry from and that notice will never arrive. It therefore counts as `knell_notifications_dropped_total{kind="recovered"}`, not as a failure you can wait out.
- **Ended outages**: an outage that starts while an earlier missing notice is still undelivered gets its own queued record instead of being collapsed into that earlier one and lost. Records whose outage has already ended by the time they can be delivered are reported once in the past tense, and the notice says why it is late, because the two reasons ask for different things:

  > 🕓 [knell server-1] beat **cron-backup** was missing for 12m0s, recovered at 2026-07-23 14:07 UTC. This notice is late because delivery was delayed - check the webhook.

  An outage nothing was ever attempted for — no sweep saw it before a ping ended it, or a sweep saw it and deferred it — says so instead and points at nothing to check. That wording is only used while nothing about the outage has failed to send: a past-tense notice that itself fails and is retried carries the webhook wording when it finally arrives, so it never vouches for a webhook that just refused it. Several ended outages become one summary message stating both counts ("had 3 outages: longest 47m0s ... Delivery was delayed for 2 (check the webhook); 1 had nothing attempted"), delivered in a single sweep, so a genuinely live outage queued behind a full backlog waits one sweep rather than one per stale record. Because the notice states the outages are over, no recovered notice follows for them.
- **Queued outages**: each beat queues up to 8 records and reports them oldest first. When a beat's queue is full, the newest record is not queued, and the two cases differ in consequence:
  - an outage a ping has already ended is **dropped for good**: its record was the last trace of it, so no notice for it will ever arrive. `knell_outage_records_dropped_total{beat}` increments and a warning is logged, once for that outage. Reconstruct the missed window from `knell_beat_last_seen_timestamp_seconds`.
  - an outage still in progress **loses nothing**: it stays detected (`knell_beat_outages_total{beat}` already counted it), and it is queued and delivered once a slot opens. That is ordinary back-pressure while notifications are failing, so it is logged at debug level and moves no delivery counter.
- The webhook URL is treated as a secret: it is never logged and never appears in error messages.

## Metrics

| Metric | Type | Notes |
| ----- | ------- | ----- |
| `knell_beat_fresh{beat}` | gauge | 1 = observed silence within deadline, 0 = overdue; silence runs from process start until the first ping, so a beat nothing has pinged reads 1 for its first deadline. The aggregation input for multi-observer quorum rules |
| `knell_beat_last_seen_timestamp_seconds{beat}` | gauge | Unix time of the last accepted ping (process start until the first ping) |
| `knell_beat_deadline_seconds{beat}` | gauge | the beat's configured silence deadline. Add it to the last-seen gauge to get when an overdue beat fires, and compare it across observers to catch a `BEATS` skew before one node alerts alone |
| `knell_beats_received_total{beat}` | counter | accepted pings; unknown ids are rejected, not counted |
| `knell_beat_outages_total{beat}` | counter | outages detected per beat, counted when the deadline is crossed and independent of any delivery. Count outages with this one, not with the notification counters |
| `knell_outage_records_dropped_total{beat}` | counter | ended-outage records discarded per beat because the beat's queue was full, one per RECORD: no notice for that outage will ever arrive (see Notification semantics) |
| `knell_notifications_sent_total{kind}` | counter | delivered webhook notifications (`missing`, `recovered`, `history`), one per delivered message: a `history` message covering several ended outages counts once |
| `knell_notifications_failed_total{kind}` | counter | delivery attempts that failed after retries, one per failed message, with the record still queued: in practice `missing` and `history`, which the next sweep tries again |
| `knell_notifications_dropped_total{kind}` | counter | notification messages that will never be delivered, one per lost message: in practice `recovered`, the one fire-once kind. Nothing retries a drop. A lost outage RECORD is counted on `knell_outage_records_dropped_total` instead, because a record is not a message |
| `knell_pre_route_refusals_total{reason}` | counter | requests knell's own code refuses before any route ran, by cause: `non_canonical_beat_path` (a malformed `/beat` URL), `host_not_allowed` (a `Host` missing from `ALLOWED_HOSTS`, or a DNS-rebinding attempt), `auth_throttled` (failed authentication over the throttle's budget). A diagnostic, not an alert source — see below |
| `knell_http_requests_total{method,path,status}` | counter | served requests, labelled by the matched route template (never the raw path) and a closed method set. The only view of a REFUSED ping: a 401, 404, 405 or 503 never reaches `knell_beats_received_total`. A refusal answered before routing has no template, so it lands under `path="unmatched"` |
| `knell_http_request_duration_seconds` | histogram | served-request latency across the whole surface, deliberately unlabelled |

Plus standard `go_*` / `process_*` runtime metrics.

`knell_pre_route_refusals_total` deliberately has no alert rule of its own, and should not get one: a sender whose pings are refused is not feeding its beat, so that beat crosses its deadline and `KnellBeatOverdue` (below) fires anyway. Read it when a beat has gone missing and you need to know why the pings stopped landing — a malformed URL, a `Host` the deployment forgot to allow, or a rotated token throttling every sender. It needs its own counter because all three refusals happen before any route matches, where `knell_http_requests_total` cannot tell them apart from port scans in the `path="unmatched"` bucket. One refusal is missing from it by construction: a request whose header block exceeds the 8704-byte ceiling above is answered 431 by Go's HTTP server before knell sees the request at all, so it appears in no knell metric and no log line — a beat going missing with no refusal recorded, and no ping in `knell_http_requests_total`, is the signature of that case. Every reason is exposed at zero from startup, so `increase()` over it works from a cold start.

## Alerting

knell is itself the alert path for the things it watches, so alert rules about knell should come from a second vantage point (your metrics stack scraping `/metrics`). Two rules cover its state, plus one for restart churn below:

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

# knell could not get a notification through. The three legs are distinct
# consequences: failed = a delivery attempt failed after retries and its
# record is still queued, so the notice is late and retried every 15s sweep;
# either dropped = nothing will arrive and you reconstruct the window
# yourself (see Notification semantics). Keep all three: when a full queue
# discards an ended outage, only the third leg moves.
- alert: KnellNotifyFailing
  expr: >
    increase(knell_notifications_failed_total[15m]) > 0
    or increase(knell_notifications_dropped_total[15m]) > 0
    or increase(knell_outage_records_dropped_total[15m]) > 0
  for: 0m
  labels:
    severity: warning
  annotations:
    summary: "knell on {{ $labels.instance }} failed to deliver a notification, or dropped a notification or an outage record"
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

GPL-3.0-or-later. See [LICENSE](LICENSE).
