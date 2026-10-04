# knell

[![Image Size](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/knell/badges/size.json)](https://github.com/cplieger/knell/pkgs/container/knell) [![Platforms](https://img.shields.io/badge/platforms-amd64%20%7C%20arm64-blue)](https://github.com/cplieger/knell/pkgs/container/knell) [![base: scratch](https://img.shields.io/badge/base-scratch-000000)](https://github.com/cplieger/knell/blob/main/Dockerfile) [![Mutation](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/knell/badges/mutation.json)](https://github.com/cplieger/knell/issues?q=label%3Agremlins-tracker) [![SBOM](https://img.shields.io/badge/SBOM-SPDX-1D4ED8)](https://github.com/cplieger/knell/releases)

<!-- hub-overview BEGIN -->
knell is a dead man's switch for your scheduled jobs. It sends you a Discord message when a cron job, backup script or alerting pipeline stops pinging it over HTTP. It only listens and never runs them.

## What it does

knell tells you when a scheduled job stops, in four ways:

- Posts once to Discord when a job stays quiet past its deadline, and again when it pings.
- Counts each deadline from knell's start, so a job silent since a restart is still reported.
- Tells you afterwards about an outage that ended before its message could go out.
- Publishes each job's state as Prometheus metrics, so your metrics stack can combine several instances.

Each watched job is a beat. Its deadline is the longest silence allowed between two pings.

## Who it is for

knell is built for a home lab that gets its alerts in Discord and wants a small watchdog with no database or web page. It is one static binary set up from environment variables, keeps its state in memory and sends only to Discord or a Discord-compatible relay.

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
6. Add this line at the end of each job. Set `BEAT_TOKEN` in that job's environment to the same token, or paste the token in its place, and end the URL with the beat's id:

```sh
curl -fsS -X POST -H "Authorization: Bearer $BEAT_TOKEN" http://192.0.2.10:9190/beat/cron-backup
```

Replace `192.0.2.10` with the address of the host that runs knell, as other devices on your network reach it. A job that runs directly on that host, outside a container, can use `localhost:9190`.

Run `docker logs knell`. You should see a `configuration loaded` line and then `listening`. If it shows `knell exited with error` instead, its `error` field names the setting to fix, usually a placeholder you have not replaced yet.

When a beat goes quiet past its deadline, the channel gets a message naming the beat, with how long it has been silent, when the silence began and which knell instance saw it.

## Configuration reference

knell reads its settings from environment variables when it starts and never reloads them. An invalid value stops it at startup with an error naming the setting, instead of falling back to a default. [Configuration](docs/configuration.md) has the full rules for each setting.

| Variable | Description | Default |
| --- | --- | --- |
| `BEATS` | Comma-separated `id:deadline` list, such as `api:20m,backup:26h`. Deadlines are at least `30s`, at most 64 beats | required |
| `DISCORD_WEBHOOK_URL` | The `https` webhook URL notices are posted to. `DISCORD_WEBHOOK_URL_FILE` reads it from a secret file instead | required |
| `BEAT_TOKEN` | The token every ping presents as `Authorization: Bearer <token>`, 16 to 512 bytes. `BEAT_TOKEN_FILE` reads it from a secret file | required |
| `NODE_NAME` | Names this instance in every notice, at most 256 bytes | container hostname |
| `LISTEN_ADDR` | TCP listen address, `host:port` | `:9190` |
| `ALLOWED_HOSTS` | Comma-separated exact-match `Host` names or IPs to serve. Unset accepts every `Host` | _(unset)_ |
| `TRUSTED_PROXIES` | Comma-separated CIDRs or IPs of the reverse proxies whose `X-Forwarded-For` names the real sender | _(unset)_ |
| `LOG_LEVEL` | `debug`, `info`, `warn` or `error`. An unknown value falls back to `info` | `info` |

| Port | Description |
| --- | --- |
| `9190` | Pings on `POST /beat/<id>`, the health endpoint `/healthz` and Prometheus metrics on `/metrics` |

knell needs no volume.

## Security

knell serves plain HTTP, so `BEAT_TOKEN` crosses the network in cleartext on every ping, and anything that reads one ping can replay it. Put a TLS reverse proxy in front, or keep pings on a network you trust to the same standard.

The token gates `POST /beat/<id>` only. `/healthz` and `/metrics` answer anyone who can reach the port, and `/metrics` lists every beat with its last ping. Publish the port to a trusted network only, or put an authenticating proxy in front of `/metrics`. Set `ALLOWED_HOSTS` to the names you use, so a web page an operator opens cannot reach knell through a hostname an attacker controls.

The image runs as the non-root user 65534 on `scratch`, and the webhook URL never appears in a log line or an error. [Security](docs/hardening.md) covers the startup check for `ALLOWED_HOSTS`, the proxy settings and the hardened compose profile.

## Troubleshooting

The image has a built-in healthcheck, `knell health`, which reads a marker file the server writes once its listener is up and removes when it stops. `docker ps` shows `healthy` while knell is serving. An unhealthy container is running but not serving, as happens while knell shuts down. A restarting one has exited. In both cases `docker logs knell` shows why.

- If the container restarts in a loop with `knell exited with error`, the `error` field names the setting knell refused.
- If a ping is answered `401`, the `Authorization` header does not match `BEAT_TOKEN` exactly.
- If a ping is answered `404`, the id is not in `BEATS` or the URL has an extra path segment.
- If a ping is answered `403` with `host_not_allowed`, add the name the sender uses to `ALLOWED_HOSTS`.
- If a ping is answered `405`, the sender used `GET` or another method. Only `POST` records a ping.
- If a beat that is down never alerts, check how often knell restarts. Each start resets every deadline, so restarts closer together than a beat's deadline keep it from ever firing.

[How knell works](docs/how-it-works.md) explains the deadlines, retries and every answer a ping can get.

## Monitoring

knell publishes Prometheus metrics on `/metrics` and writes logfmt logs to standard error. If `knell_outage_records_dropped_total` rises, an ended outage was lost before a notice could be built. Rebuild that window from `knell_beat_last_seen_timestamp_seconds`. Eight alert rules that watch knell itself ship in the [`alerts/`](alerts/) folder, with five PromQL rules in [`alerts/promql.yaml`](alerts/promql.yaml) and three LogQL rules in [`alerts/logql.yaml`](alerts/logql.yaml). [Monitoring and alerts](docs/monitoring.md) lists the metrics and rules and shows how to load them.

## Documentation

- [Configuration](docs/configuration.md) lists every setting, its limits, secret files and what stops startup.
- [How knell works](docs/how-it-works.md) explains deadlines, notices, retries and every answer a ping can get.
- [Monitoring and alerts](docs/monitoring.md) lists the log lines, the metrics and the alert rules.
- [Security](docs/hardening.md) covers exposure, reverse proxies and the hardened compose profile.

## Contributing

Issues and pull requests are welcome, see the [contributing guide](https://github.com/cplieger/.github/blob/main/CONTRIBUTING.md). Build the binary with `go build -trimpath -ldflags="-s -w" -o knell .` or the image with `docker build -t knell .`.

## Disclaimer

This project is built with care and follows security best practices, but it is intended for personal / self-hosted use. No guarantees of fitness for production environments. Use at your own risk.

This project was built with AI-assisted tooling using [Claude](https://claude.com), [GPT](https://openai.com), and [Kiro](https://kiro.dev). The human maintainer defines architecture, supervises implementation, and makes all final decisions.

## License

GPL-3.0-or-later. See [LICENSE](LICENSE). The image carries the license text of every bundled component under `/usr/share/licenses/`.
