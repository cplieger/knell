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
| `BEATS` | comma-separated `id:deadline` list, e.g. `api:20m,backup:26h`. Ids match `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`; deadlines are Go durations, minimum `30s`, maximum 64 beats | _none_ | Yes |
| `DISCORD_WEBHOOK_URL` | the webhook notifications post to, `https` only: the URL's own path carries the credential, so a plain-http webhook would put it on the wire in cleartext. It must carry that path (a host-only, root-only or query-only URL is refused) and contain no space or other invisible character. `DISCORD_WEBHOOK_URL_FILE` points at a mounted secret file instead | _none_ | Yes |
| `NODE_NAME` | names this observer instance in every notification; maximum 256 bytes, since the name prefixes every notice and a longer one would push the message past Discord's 2000-character limit | container hostname | No |
| `BEAT_TOKEN` | the bearer token every sender must present as `Authorization: Bearer <token>` on `POST /beat/{id}`. It is the endpoint's only gate, so it is required and must be at least 16 bytes: generate one with `openssl rand -hex 16`. The token is verified exactly as configured and is never rewritten, so a value senders could not present as written — empty, under the 16-byte minimum, over the 512-byte maximum (the token has to fit the header block knell reads, capped at 8704 bytes), carrying surrounding spaces or tabs, or carrying a control character HTTP forbids in a header — fails startup instead. `BEAT_TOKEN_FILE` points at a mounted secret file instead | _none_ | Yes |
| `LISTEN_ADDR` | TCP listen address (`host:port`) | `:9190` | No |
| `ALLOWED_HOSTS` | comma-separated exact-match `Host` allowlist (bare hostnames or IPs, an optional port, no scheme or path), e.g. `knell.internal,10.0.0.5`. Unset accepts every `Host`; setting it rejects any other `Host` with 403 `host_not_allowed`, which is what blocks DNS rebinding from a browser inside your network. A caller is exempt only when its socket address AND its `Host` are both loopback, so an in-container `curl http://127.0.0.1:9190/healthz` keeps working while a rebinding request — which carries the attacker's hostname in `Host` — never qualifies; the baked `knell health` check needs no exemption at all, since it reads the marker file and sends no request. The allowlist covers `/healthz` and `/metrics` too, so list every hostname or IP your probes and Prometheus scraper reach knell by, not just the browser-facing one. Malformed entries fail startup: an entry no `Host` can match (a scheme, a path, a CIDR, a lone port) would leave an allowlist that is not the one you configured, and every non-loopback caller under a hostname you believed was listed — pings included — would be refused 403 | _(unset)_ | No |
| `TRUSTED_PROXIES` | comma-separated CIDRs or bare IPs of reverse proxies in front of knell, e.g. `10.0.0.0/24,192.168.1.5`. When the connecting peer is one of them, `X-Forwarded-For` is believed and the access line's `client_ip` names the real sender instead of the proxy; unset means no forwarded header is honored and `client_ip` is the socket peer, which is the spoof-proof default for a directly published port. Set it whenever you follow the TLS-proxy advice below, or every access line — including the 401s a token-guessing run writes — names the proxy rather than an address you can block. List exactly the hops in front of knell and nothing more: the forwarded header is walked from the right and the walk stops at the first address that is not in the set, so a missing hop is reported as the client instead of the sender, while a range wider than your proxies (a whole container subnet, say, rather than the proxy's own address) lets anything inside it choose its own `client_ip` — the spoofable state an unset variable cannot reach. Malformed entries are logged and ignored rather than failing startup, because dropping one only narrows whose forwarded header is believed; the startup `configuration loaded` line reports `trusted_proxies=<count>` so you can see how many were kept | _(unset)_ | No |
| `LOG_LEVEL` | `debug`/`info`/`warn`/`error`; unknown falls back to `info` | `info` | No |

knell serves plain HTTP, so `BEAT_TOKEN` crosses the network in cleartext on every ping. Since the token is the only thing standing between a stranger who can reach the port and a forged heartbeat, TLS is a requirement rather than a nicety: put a TLS reverse proxy in front, or keep pings on a network you trust to that same standard, because anything that can read one ping can replay it forever.

`BEAT_TOKEN` gates `/beat/{id}` only: `/healthz` and `/metrics` stay open on the same port so probes and scrapes keep working. `/metrics` publishes every configured beat id plus each beat's last-seen timestamp and freshness, so anyone who can reach the port can enumerate the beats and see which one is about to fire, even with a token set. Publish the port to a trusted network only (the compose example maps it on every host interface), or put an authenticating proxy in front of `/metrics`.

A trusted network is not enough on its own against a browser: a page an operator opens can make their browser send requests to knell under a hostname the attacker controls (DNS rebinding), and `/metrics` enumerates every beat. Set `ALLOWED_HOSTS` to the hostnames knell is actually reached by, and any other `Host` is refused on every endpoint before a beat can be recorded or the exposition read. Check that it took effect in the `configuration loaded` line knell logs at startup: `allowed_hosts=allowlist(2)` names how many hosts the gate holds, while `allowed_hosts=any` means no allowlist is active and every `Host` is accepted — which is what a misspelled variable name looks like, since knell cannot tell it from an unset one.

A malformed `BEATS` or `DISCORD_WEBHOOK_URL` fails startup rather than falling back, and so does a `NODE_NAME` over 256 bytes. For the webhook URL that means any scheme other than `https`, a URL that carries no path (host-only, root-only, or one whose credential sits in a query string — a Discord webhook always carries its credential in its path, `/api/webhooks/{id}/{token}`), and a URL containing a space or another invisible character such as a non-breaking or zero-width space (each is percent-encoded on every request, so the host and path that reach the other end are not the configured ones). `BEAT_TOKEN` is required, so startup fails when it is unset entirely, and again whenever the configured value is not one a sender could present or one a stranger could not guess: when it is set but empty, when it is shorter than 16 bytes, when it is longer than 512 bytes (knell reads at most 8704 bytes of request headers, so a longer token leaves no room for the request line, `Host`, and the `Bearer` prefix with its trailing space that the token travels behind, and every ping would be answered 431), when it carries surrounding spaces or tabs, or when it carries a control character HTTP forbids in a header value. Surrounding whitespace is refused rather than trimmed, because the value is verified verbatim: knell compares `Bearer <token>`, so a leading run of spaces sits inside the header value and reaches the verifier, while a trailing one is stripped on the wire — silently rewriting the token would arm the gate for a value that differs from the one you configured, and knell would then reject every ping while reporting itself gated, then declare every beat missing one deadline later. A non-ASCII space such as `U+00A0` does survive a header value, so a token containing one is accepted as configured. When a `_FILE` variable is set but its file cannot be read, because it is missing, unreadable, or empty, startup fails instead of falling back to the plain variable; only an unset `_FILE` variable falls back. A dead-man switch running with the wrong config is worse than one that refuses to start.

## Endpoints

| Endpoint | Purpose |
| -------- | ------- |
| `POST /beat/{id}` | record a ping, with `Authorization: Bearer <BEAT_TOKEN>`; `{"ok":true}` on success, 401 without the token, 404 for unknown ids, 405 for any other method |
| `GET /healthz` | liveness (`{"status":"OK"}`) |
| `GET /metrics` | Prometheus exposition |

Request bodies on `/beat/{id}` are ignored, so webhook-shaped senders (an Alertmanager `webhook_configs` target, a CI notification hook) can point at it unchanged. Up to 1 MiB of the body is read and discarded so the connection stays reusable; a payload larger than that still records the ping and answers `{"ok":true}`, logs one `warn` line saying the body was not fully read, and closes that connection instead of draining the rest (so an oversized sender loses keep-alive, never its ping). A ping is never refused for its payload: the body is not what the switch is listening for.

Headers are bounded the other way round, because they are the part knell has to parse before it knows anything about the caller: at most 8704 bytes of request headers are read (net/http's own default is 1 MiB, one megabyte per connection an unauthenticated caller can spend), and a request whose header block is larger is answered 431. That figure is the 512-byte `BEAT_TOKEN` maximum plus 8 KiB for the request line, `Host`, the `Bearer` prefix, and whatever sits in front of knell adds. The 8 KiB matches what a default nginx or Apache allows in one header line; their whole-block limits are higher, so a proxy that piles on `X-Forwarded-*`, tracing and cookie headers can still deliver a block knell answers 431. If pings start failing that way, trim the headers the proxy adds.

Only `POST` records. `GET` and `HEAD` are answered with 405 and never feed the switch, so nothing that merely fetches a URL — a chat client's link preview, a crawler, an uptime prober, an `<img>` on a page an operator opens — can keep a beat looking alive. Repeated pings that present no valid token are throttled together and answered 429 with a `Retry-After` hint once the shared budget is spent, which caps both guessing and the log flood a bad sender would otherwise write; a ping with the right token is never throttled, however many senders you run.

`/healthz` and `/metrics` are logged as machine probes: a successful probe or scrape lands at `debug` (out of the log at the default level, visible under `LOG_LEVEL=debug` when the question is whether the prober arrives at all), while one answering 4xx or 5xx lands at `warn`/`error`. So a scrape that stopped landing shows up in the log without raising the level.

## Notification semantics

A live incident and one that is already over are reported differently: nothing an operator reads should announce a resolved outage as a beat that is down right now.

- **Missing**: sent once per live outage, when a beat first passes its deadline. A failed delivery (Discord outage, network) is retried on every 15s sweep until one succeeds; the beat is only marked notified after a delivered send.
- **Recovered**: sent on the first accepted ping after a missing notice, best-effort. Delivery uses bounded retries with jittered backoff and honors `Retry-After` on rate limits. It is fire-once: the queued transition is consumed before the send, so a delivery that still fails has nothing left to retry from and that recovery notice will never arrive. It therefore counts as `knell_notifications_dropped_total{kind="recovered"}`, not as a failure you can wait out.
- **Ended outages**: an outage that starts while an earlier missing notice is still undelivered gets its own queued record instead of being collapsed into that earlier one and lost. Records whose outage has already ended by the time they can be delivered are reported once in the past tense, and the notice says why it is late, because the two reasons ask for different things. An outage whose report was held up by delivery points at the webhook:

  > 🕓 [knell server-1] beat **cron-backup** was missing for 12m0s, recovered at 2026-07-23 14:07 UTC. This notice is late because delivery was delayed - check the webhook.

  An outage that ended before a sweep detected it never had an alert to deliver, so there is nothing to check:

  > 🕓 [knell server-1] beat **cron-backup** was missing for 12m0s, recovered at 2026-07-23 14:07 UTC. This notice is late only because the outage ended before a sweep detected it - nothing was wrong with delivery.

  That second wording is only used while nothing about the outage has failed to send. If the past-tense notice itself fails and is retried, the outage is late because of delivery after all, so the notice that eventually arrives carries the webhook wording instead — it never vouches for a webhook that just refused it.

  Several become one summary. When the batch mixes the two reasons, it reports both counts rather than blaming one for all of them:

  > 🕓 [knell server-1] beat **cron-backup** had 3 outages: longest 47m0s, last recovered at 2026-07-23 14:07 UTC. Delivery was delayed for 2 (check the webhook); 1 ended before a sweep detected it.

  The whole run of ended outages goes out in a single sweep, so a genuinely live outage queued behind it waits one sweep rather than one sweep per stale record. Because the notice states the outages are over, no recovered notice follows for them.
- **Queued outages**: each beat queues up to 8 records and reports them oldest first. When a beat's queue is full, the newest record is not queued, and the two cases that can arise differ in consequence:
  - an outage a ping has already ended is **dropped for good**: its record was the last trace of it, so no notice for it will ever arrive. `knell_outage_records_dropped_total{beat}` increments and a warning is logged, once for that outage. Reconstruct the missed window from `knell_beat_last_seen_timestamp_seconds`.
  - an outage still in progress **loses nothing**: it stays detected (`knell_beat_outages_total{beat}` already counted it), and it is queued and delivered once a slot opens. That is ordinary back-pressure while notifications are failing, so it is logged at debug level and moves no delivery counter.
- The webhook URL is treated as a secret: it is never logged and never appears in error messages.

## Metrics

| Metric | Type | Notes |
| ----- | ------- | ----- |
| `knell_beat_fresh{beat}` | gauge | 1 = last ping within deadline, 0 = overdue. The aggregation input for multi-observer quorum rules |
| `knell_beat_last_seen_timestamp_seconds{beat}` | gauge | Unix time of the last accepted ping (process start until the first ping) |
| `knell_beat_deadline_seconds{beat}` | gauge | the beat's configured silence deadline. Add it to the last-seen gauge to get when an overdue beat fires, and compare it across observers to catch a `BEATS` skew before one node alerts alone |
| `knell_beats_received_total{beat}` | counter | accepted pings; unknown ids are rejected, not counted |
| `knell_beat_outages_total{beat}` | counter | outages detected per beat, counted when the deadline is crossed and independent of any delivery. Count outages with this one, not with the notification counters |
| `knell_outage_records_dropped_total{beat}` | counter | ended-outage records discarded per beat because the beat's queue was full. Counted per RECORD, not per message: the record was that outage's last trace, so no notice for it will ever arrive. Reconstruct the missed window from `knell_beat_last_seen_timestamp_seconds` |
| `knell_notifications_sent_total{kind}` | counter | delivered webhook notifications (`missing`, `recovered`, `history`), one per delivered message: a `history` message covering several ended outages counts once |
| `knell_notifications_failed_total{kind}` | counter | delivery attempts that failed after retries and will be retried, one per failed message. This counter means exactly that: something was sent, did not get through, and its record is still queued. In practice that is `missing` and `history`, which the next sweep tries again |
| `knell_notifications_dropped_total{kind}` | counter | notification messages that will never be delivered, one per lost message. In practice that is `recovered`, the one fire-once kind: its send failed with nothing left to retry from. Nothing retries a drop. A lost outage RECORD is counted on `knell_outage_records_dropped_total` instead, because a record is not a message |
| `knell_pre_route_refusals_total{reason}` | counter | requests refused before any route ran, by cause: `non_canonical_beat_path` (a sender pinging a malformed `/beat` URL), `host_not_allowed` (a `Host` missing from `ALLOWED_HOSTS`, or a DNS-rebinding attempt), `auth_throttled` (failed authentication over the throttle's budget). A diagnostic, not an alert source — see below |
| `knell_http_requests_total{method,path,status}` | counter | served requests, labelled by the matched route template (never the raw path) and a closed method set. The only view of a REFUSED ping: a 401, 404, 405 or 503 never reaches `knell_beats_received_total`. A refusal answered before routing has no template, so it lands under `path="unmatched"` and is named by cause on `knell_pre_route_refusals_total` |
| `knell_http_request_duration_seconds` | histogram | served-request latency across the whole surface, deliberately unlabelled |

Plus standard `go_*` / `process_*` runtime metrics.

`knell_pre_route_refusals_total` deliberately has no alert rule of its own, and should not get one. A sender whose pings are refused is not feeding its beat, so that beat crosses its deadline and `KnellBeatOverdue` (below) fires anyway; a rule here would page a second time for a condition already covered. Read it when a beat has gone missing and you need to know why the pings stopped landing — a malformed URL, a `Host` the deployment forgot to allow, or a rotated token throttling the fleet — rather than waiting for it to page. All three refusals happen before any route matches, which is why they need their own counter: on `knell_http_requests_total` they are indistinguishable from port scans in the `path="unmatched"` bucket. Every reason is exposed at zero from startup, so `increase()` over it works from a cold start.

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

# knell could not get a notification through, for one of three reasons the
# counters keep apart:
#   failed  = a delivery attempt failed after retries (the webhook is
#             unreachable) and its record is still queued, so it is retried
#             every 15s sweep. The notice is late, not lost: wait for it.
#   dropped = a MESSAGE that will never arrive, in practice a fire-once
#             recovered notice whose send failed with nothing left to
#             retry from.
#   outage records dropped = an ended outage whose record was discarded by a
#             full per-beat queue. Counted per record rather than per message,
#             because one history message covers several records. Reconstruct
#             the missed window from knell_beat_last_seen_timestamp_seconds.
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

GPL-3.0. See [LICENSE](LICENSE).
