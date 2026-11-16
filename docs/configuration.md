# Configuration

This page lists every rule knell applies to its settings, for operators writing a `BEATS` list, wiring secret files or putting knell behind a proxy. The README's [Configuration reference](../README.md#configuration-reference) has the summary table.

knell reads environment variables once, at startup, and has no config file and no reload. Change a value, then recreate the container.

## Beats and deadlines

`BEATS` is a comma-separated list of `id:deadline` entries, such as `api:20m,backup:26h`.

- An id matches `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`. It starts with a letter or digit, then has up to 63 letters, digits, `_` or `-`. Each id appears once.
- A deadline is a Go duration with an explicit unit, `s`, `m` or `h`, of at least `30s`. Go durations have no day unit, so use `26h` for a daily job.
- A list holds at most 64 beats.
- Whitespace around an entry and around its colon is ignored, so `api:20m, backup:26h` is the same list.
- A blank segment between commas is skipped, so a trailing comma is fine.

The deadline is the silence knell tolerates. A job that runs once a day with a 26h deadline gets two hours of slack before it is reported missing.

## Missing notice repeat

`MISSING_REPEAT` sends a beat's missing notice again while the beat stays missing, such as `6h`. Unset, blank or `0`, knell sends one missing notice per outage.

- It is a Go duration with an explicit unit, of at least `10m`. A shorter interval reposts one outage faster than anyone can act on it.
- It applies to every beat in `BEATS`.
- The interval counts from the last delivered missing notice. Each repeat reports the whole silence so far.
- A repeat that fails to send is retried on the next sweep, like the first notice.
- The first ping after the outage ends the repeats and sends the recovered notice.

## Webhook URL

`DISCORD_WEBHOOK_URL` is the webhook every notice is posted to. Its path is the credential Discord issues, `/api/webhooks/<id>/<token>`, so knell accepts it only when:

- the scheme is `https`,
- it has a host, and a port between 1 and 65535 when it names one,
- it has a path, because a host-only URL cannot deliver anything,
- it contains no space and no invisible character.

Any other `https` path is accepted, so a Discord-compatible relay works too, provided it accepts Discord's `content` plus `embeds` message shape.

## Beat tokens

`BEAT_TOKENS` gives each beat its own token. A sender presents it as `Authorization: Bearer <token>` on `POST /beat/<id>`, and it is the only gate on that endpoint. knell does not start without it.

```text
cron-backup:3f9c0a1b2c3d4e5f6a7b8c9d0e1f2a3b
backup+backup-offsite:9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d
```

- Each line is `<beats>:<token>`, where `<beats>` is one id from `BEATS`, or several joined with `+` for a sender that pings each of them. The line splits at its first colon, so a token may hold a colon or a comma.
- A token pings only the beats its line names. A valid token on another beat is answered `403 beat_not_allowed`.
- Every beat in `BEATS` needs a line. A beat with none could never be pinged, so it stops startup.
- One token may not appear on two lines, because it would ping the beats of both. Two lines may name one beat, which lets you move a job to a new token without a gap.
- Blank lines are skipped. At most 128 lines, two per beat.
- Each token is 16 to 512 bytes. `openssl rand -hex 16` makes a 32-character token.
- Each token is checked exactly as written. One with leading or trailing spaces, tabs or a carriage return is refused.
- A token with a control character HTTP forbids in a header is refused, and so is one with an invisible character, such as a zero-width space or a bidi control, or with bytes that are not valid UTF-8.
- Save a token file with LF line endings. In a file with CRLF endings every line but the last keeps its carriage return, and knell refuses it.
- A refusal names the line number, never the token. A line written token first, `<token>:<beat>`, is refused for naming a beat not in `BEATS`.

In compose, write several lines as a YAML block, `BEAT_TOKENS: |`. Prefer `BEAT_TOKENS_FILE` with a Docker secret, so the tokens stay out of `docker inspect` output.

knell refuses whitespace instead of trimming it. HTTP strips a trailing space or tab from a header and cannot carry a line break at all, so a token with them never arrives as configured and every ping would be rejected. A leading space is invisible in the value you read. Rather than quietly change your credential, knell stops and names the problem.

The 512-byte ceiling keeps a token inside the 8704-byte request-header limit with room for the rest of the request.

knell v3's `BEAT_TOKEN` and `BEAT_TOKEN_FILE` are no longer read. With `BEAT_TOKENS` unset and one of them set, the refusal says so. With both set, knell starts and warns you to unset the old one.

## Secret files

`DISCORD_WEBHOOK_URL_FILE` and `BEAT_TOKENS_FILE` read the same values from a file, such as [a Docker secret](https://github.com/cplieger/docs/blob/main/docs/hardening.md#secrets-in-files) under `/run/secrets/`.

- A `_FILE` variable that is set but empty, names a missing or unreadable file, or names an empty file stops startup. Only an unset `_FILE` falls back to the plain variable.
- The path must be clean, with no `..` segment, no doubled `/` and no trailing `/`.
- The file holds at most 1 MiB. One trailing line ending is removed.
- When both the file and the plain variable are set, the file wins and knell logs a warning naming the plain variable to unset.

## Node name

`NODE_NAME` names this instance in every notice. It defaults to the container hostname, or `unknown` when the hostname is blank. It holds at most 256 bytes, because it prefixes every notice and Discord caps a message at 2000 characters.

## Listen addresses

knell opens two listeners, so the port you publish for pings never serves `/metrics`.

- `LISTEN_ADDR` is the `host:port` of the pings, `:9190` by default.
- `OPS_LISTEN_ADDR` is the `host:port` of `/healthz` and `/metrics`, `:9191` by default. The built-in `knell health` check asks this one.
- A blank value uses the default. Spaces and invisible characters around a value are removed.
- The two must use different ports. `OPS_LISTEN_ADDR` on `LISTEN_ADDR`'s port stops startup, so if your `LISTEN_ADDR` is `:9191`, set `OPS_LISTEN_ADDR` to another port.
- A port another process holds stops startup, with the error naming the setting.

## Host allowlist

`ALLOWED_HOSTS` is a comma-separated list of exact `Host` names to serve, such as `knell.internal,192.0.2.10`. Each entry is a bare hostname or IP with an optional port, with no scheme, path or CIDR.

- Unset, knell answers every `Host`.
- Set, a request with any other `Host` is refused with `403 host_not_allowed` on both ports. This is what blocks DNS rebinding from a browser inside your network.
- It covers `/healthz` and `/metrics` on port `9191` too, so list every name your probes and your Prometheus scraper use.
- knell's own requests are always served. Those are a request from loopback to a loopback `Host`, and one from the address a listener binds to that same address as `Host`.
- An entry no `Host` could ever match stops startup, because a dropped entry would leave pings to that name refused.
- The built-in `knell health` check is one of those requests when the host in `OPS_LISTEN_ADDR` is empty, `0.0.0.0`, `::`, `localhost` or an IP address without a zone. When `OPS_LISTEN_ADDR` names any other host, such as `knell.lan:9191`, the check sends that name as `Host`, so list it in `ALLOWED_HOSTS`.

## Trusted proxies

`TRUSTED_PROXIES` lists the reverse proxies in front of knell as comma-separated CIDRs or bare IPs, such as `192.0.2.0/24,198.51.100.5`. knell believes their `X-Forwarded-For` header, so the `client_ip` on each access log line names the real sender.

- Unset, knell honors no forwarded header and `client_ip` is the connecting address.
- Set it when a TLS proxy sits in front. Otherwise every access line names the proxy instead of an address you can block, and every client behind the proxy shares one failure limit. knell logs a warning the first time a ping with a valid token comes through a proxy it does not trust. Check that the `peer` it names is your proxy before you trust it.
- List exactly those hops. A range wider than your proxies lets anything inside it choose its own `client_ip`.
- A malformed entry is logged and dropped, and startup goes on.

## Log level

`LOG_LEVEL` is `debug`, `info`, `warn` or `error`, and `info` by default. An unknown value falls back to `info` with a warning. Keep `info` if you load the `KnellNoticeLostForGood` alert rule, because the lines it reads sit at `info` and `warn`. [Monitoring and alerts](monitoring.md) describes that rule.

## Startup summary

After the settings pass, knell logs one `watching beat` line per beat and one `configuration loaded` line. That line carries `beats`, `node`, `listen_addr`, `ops_listen_addr`, `webhook` and `beat_tokens` as `env` or `file`, the `beat_token_entries` count, `allowed_hosts` as `any` or `allowlist(N)`, the `trusted_proxies` count, `log_level` and `missing_repeat` as a Go duration, `0s` when unset. It never carries a credential. A misspelled `ALLOWED_HOSTS` looks the same as an unset one, `allowed_hosts=any`, so check that field after you set it. While `ALLOWED_HOSTS` is unset, knell also logs a warning at startup.

## What stops startup

knell refuses to start rather than run with a setting it cannot honor, because a dead man's switch running with the wrong configuration is worse than one that does not start. These stop it:

- a `BEATS` entry that is not `id:deadline` with a valid id and a deadline of at least `30s`, a duplicate id, or more than 64 beats,
- a webhook URL that is not `https`, has no host or no path, or holds a space or an invisible character,
- a `MISSING_REPEAT` that is not a Go duration, is negative, or is above `0` and below `10m`,
- a `NODE_NAME` over 256 bytes,
- an `ALLOWED_HOSTS` entry no `Host` could match,
- a `BEAT_TOKENS` that is unset or empty, a line with no colon or naming a beat not in `BEATS`, a beat no line names, a token on two lines, or more than 128 lines,
- a token outside 16 to 512 bytes, wrapped in whitespace, or carrying a forbidden control character, an invisible character or invalid UTF-8,
- a `_FILE` variable that is set but unusable,
- an `OPS_LISTEN_ADDR` on `LISTEN_ADDR`'s port,
- a listen address it cannot bind.

Each one logs `knell exited with error` with an `error` field naming the setting and the fix.
