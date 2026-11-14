# knell

[![Image Size](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/knell/badges/size.json)](https://github.com/cplieger/knell/pkgs/container/knell) [![Platforms](https://img.shields.io/badge/platforms-amd64%20%7C%20arm64-blue)](https://github.com/cplieger/knell/pkgs/container/knell) [![base: scratch](https://img.shields.io/badge/base-scratch-000000)](https://github.com/cplieger/knell/blob/main/Dockerfile) [![Mutation](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/knell/badges/mutation.json)](https://github.com/cplieger/knell/issues?q=label%3Agremlins-tracker) [![SBOM](https://img.shields.io/badge/SBOM-SPDX-1D4ED8)](https://github.com/cplieger/knell/releases)

<!-- hub-overview BEGIN -->
knell sends you a Discord message when a cron job, backup script or alerting pipeline stops checking in. Each job pings knell over HTTP. knell never runs the jobs.

## What it does

knell tells you when a scheduled job stops running:

- Posts to Discord when a job misses its deadline, and again when it checks in.
- Can repeat that post while the job stays silent.
- Reports a job that never checks in at all, one deadline after knell starts.
- Reports an outage that ended while Discord was down, once Discord answers again.
- Publishes each job's state as Prometheus metrics.

Each watched job is a beat. Its deadline is the longest silence allowed between two pings.

## Who it is for

knell is built for a home lab that gets its alerts in Discord and wants a small watchdog with no database or web page. It sends only to a Discord webhook or a relay that accepts Discord's message format. It keeps its state in memory, so a restart starts every deadline over.

You need a Discord webhook, a Docker host and jobs that can send an HTTP POST. Keep its port on a network you trust.

Two projects suit a different setup:

- Consider [Healthchecks](https://github.com/healthchecks/healthchecks) if you want a web dashboard, cron schedules and 25+ integrations.
- Consider [Uptime Kuma](https://github.com/louislam/uptime-kuma) if you want status pages and active HTTP, DNS and ping checks.

knell is free software under the GPL-3.0-or-later license.
<!-- hub-overview END -->

## Quick start

The image is on GitHub Container Registry and Docker Hub, for `amd64` and `arm64`. This is the [`compose.yaml`](compose.yaml) in this repository.

```yaml
services:
  knell:
    image: ghcr.io/cplieger/knell:latest
    container_name: knell
    restart: unless-stopped

    environment:
      BEATS: "cron-backup:26h,pipeline-watchdog:20m"  # one id:deadline pair per job you watch
      DISCORD_WEBHOOK_URL: "https://discord.com/"  # replace with your full Discord webhook URL, or knell refuses to start
      BEAT_TOKEN: "CHANGEME"  # replace with the output of "openssl rand -hex 16", or knell refuses to start
      NODE_NAME: "server-1"  # names this instance in every notice

    ports:
      # /metrics needs no token and lists every beat. Publish this port only to a
      # network you trust, see README "Security".
      - "9190:9190"
```

1. In Discord, open Server Settings, then Integrations, and create a webhook for the channel that should get the notices.
2. Copy the webhook URL.
3. Run `openssl rand -hex 16` and keep the output as your token.
4. Save the file above as `compose.yaml`. Put the webhook URL in `DISCORD_WEBHOOK_URL`, the token in `BEAT_TOKEN`, and one `id:deadline` pair per job in `BEATS`.
5. Run `docker compose up -d`.
6. Set `BEAT_TOKEN` to the same token in each job's environment, or paste the token in place of `$BEAT_TOKEN` below.
7. Add this line at the end of each job, after the commands that must succeed, and end the URL with the beat's id. A job that fails earlier sends no ping, and knell never learns why:

```sh
curl -fsS -X POST -H "Authorization: Bearer $BEAT_TOKEN" http://192.0.2.10:9190/beat/cron-backup
```

Replace `192.0.2.10` with the address of the host that runs knell, as other devices on your network reach it. A job that runs directly on that host, outside a container, can use `localhost:9190`.

Run `docker logs knell`. You should see a `configuration loaded` line and then `listening`. If it shows `knell exited with error` instead, its `error` field names the setting to fix, usually a placeholder you have not replaced yet.

When a beat goes quiet past its deadline, the channel gets a message naming the beat, how long it has been silent, when the silence began and which knell instance saw it. When the beat pings again, a recovered message follows.

## Configuration reference

knell reads its settings from environment variables when it starts and never reloads them. An invalid value stops it at startup with an error naming the setting. The two exceptions are `LOG_LEVEL`, which falls back to `info`, and `TRUSTED_PROXIES`, which logs and drops a malformed entry. [Configuration](docs/configuration.md) has the full rules for each setting.

| Variable | Description | Default |
| --- | --- | --- |
| `BEATS` | Comma-separated `id:deadline` list, such as `api:20m,backup:26h`. Deadlines are at least `30s`, at most 64 beats | required |
| `DISCORD_WEBHOOK_URL` | The `https` webhook URL notices are posted to. `DISCORD_WEBHOOK_URL_FILE` reads it from a secret file instead | required |
| `BEAT_TOKEN` | The token every ping presents as `Authorization: Bearer <token>`, 16 to 512 bytes. `BEAT_TOKEN_FILE` reads it from a secret file | required |
| `NODE_NAME` | Names this instance in every notice, at most 256 bytes | container hostname |
| `LISTEN_ADDR` | TCP listen address, `host:port` | `:9190` |
| `ALLOWED_HOSTS` | Comma-separated exact-match `Host` names or IPs to serve. Unset accepts every `Host` | _(unset)_ |
| `TRUSTED_PROXIES` | Comma-separated CIDRs or IPs of the reverse proxies whose `X-Forwarded-For` names the real sender | _(unset)_ |
| `MISSING_REPEAT` | Repeats a beat's missing notice at this interval while it stays missing, such as `6h`. At least `10m`. `0` sends one notice per outage | `0` |
| `LOG_LEVEL` | `debug`, `info`, `warn` or `error`. An unknown value falls back to `info` | `info` |

| Port | Description |
| --- | --- |
| `9190` | Pings on `POST /beat/<id>`, the health endpoint `/healthz` and Prometheus metrics on `/metrics` |

knell needs no volume.

## Security

knell serves plain HTTP, so `BEAT_TOKEN` crosses the network in cleartext on every ping, and anything that reads one ping can replay it. Put a [TLS reverse proxy](https://github.com/cplieger/docs/blob/main/docs/reverse-proxy.md) in front, or keep pings on a network you trust to the same standard.

One token gates `POST /beat/<id>` for every beat, so any job that holds it can ping any beat. `/healthz` and `/metrics` answer anyone who can reach the port, and `/metrics` lists every beat with its last ping. Publish the port to a trusted network only, or put an authenticating proxy in front of `/metrics`. Set `ALLOWED_HOSTS` to the names you use to reach knell. Then a web page in your browser cannot read `/metrics` through a domain an attacker points at knell.

The image runs as the non-root user 65534 on `scratch`, and the webhook URL never appears in a log line or an error. [Security](docs/hardening.md) covers reverse proxies, `ALLOWED_HOSTS` and the hardened compose profile.

## Troubleshooting

The image checks itself every 30 seconds with `knell health`, which asks `/healthz` inside the container. `docker ps` shows `healthy` while knell is serving. Unhealthy means knell is running but not answering, as happens while it shuts down. A restarting container has exited, and `docker logs knell` shows why. Docker health alerts nobody, so to hear that knell stopped, check `/healthz` from another machine as [Knowing the app is alive](docs/monitoring.md#knowing-the-app-is-alive) shows.

- If a ping is answered `401`, the `Authorization` header does not match `BEAT_TOKEN` exactly.
- If a ping is answered `404`, the id is not in `BEATS` or the URL has an extra path segment.
- If a ping is answered `403` with `host_not_allowed`, add the name the sender uses to `ALLOWED_HOSTS`.
- If a ping is answered `405`, the sender used `GET` or another method. Only `POST` records a ping.
- If a down beat never alerts, check whether knell keeps restarting. Each start gives every beat a full deadline, so a beat never fires while knell restarts more often than that.
- If Discord or the network is down, knell retries a missing notice every 15 seconds until Discord takes it.

[How knell works](docs/how-it-works.md) has the retry rules and every answer a ping can get.

## Monitoring

knell publishes Prometheus metrics on `/metrics` and writes logfmt logs, one `key=value` line per event, to standard error. Eight alert rules that watch knell ship in [`alerts/`](alerts/), six in `alerts/promql.yaml` and two in `alerts/logql.yaml`. Load both. The [Grafana dashboard](docs/dashboard.md), shipped as [`grafana-dashboard.json`](grafana-dashboard.json), needs Grafana 13.2 or newer. [Monitoring and alerts](docs/monitoring.md) lists the metrics and rules, and says who is told when knell itself stops.

## Documentation

- [Configuration](docs/configuration.md) lists every setting, its limits, secret files and what stops startup.
- [How knell works](docs/how-it-works.md) explains deadlines, notices, retries and every answer a ping can get.
- [Monitoring and alerts](docs/monitoring.md) lists the log lines, the metrics and the alert rules.
- [The Grafana dashboard](docs/dashboard.md) describes each tab and how to update it.
- [Watching your alert pipeline](docs/alert-pipeline.md) makes knell tell you when Alertmanager goes quiet.
- [Security](docs/hardening.md) covers exposure, reverse proxies and the hardened compose profile.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Disclaimer

This project is built with care and follows security best practices, but it is intended for personal / self-hosted use. No guarantees of fitness for production environments. Use at your own risk.

This project was built with AI assistance. The human maintainer defines architecture, supervises implementation, and makes all final decisions.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE). The image carries the license text of every bundled component under `/usr/share/licenses/`.
