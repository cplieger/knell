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

## Webhook URL

`DISCORD_WEBHOOK_URL` is the webhook every notice is posted to. Its path is the credential Discord issues, `/api/webhooks/<id>/<token>`, so knell accepts it only when:

- the scheme is `https`,
- it has a host, and a port between 1 and 65535 when it names one,
- it has a path, because a host-only URL cannot deliver anything,
- it contains no space and no invisible character.

Any other `https` path is accepted, so a Discord-compatible relay works too, provided it accepts Discord's `content` plus `embeds` message shape.

## Beat token

`BEAT_TOKEN` is the bearer token every sender presents as `Authorization: Bearer <token>` on `POST /beat/<id>`, and the only gate on that endpoint. knell does not start without one.

- It is 16 to 512 bytes. `openssl rand -hex 16` makes a 32-character token.
- It is checked exactly as configured. A token with leading or trailing spaces, tabs or line breaks is refused, and so is one with a control character HTTP forbids in a header.

knell refuses whitespace instead of trimming it. HTTP strips a trailing space or tab from a header and cannot carry a line break at all, so a token with them never arrives as configured and every ping would be rejected. A leading space is invisible in the value you read. Rather than quietly change your credential, knell stops and names the problem.

The 512-byte ceiling keeps the token inside the 8704-byte request-header limit with room for the rest of the request.

## Secret files

`DISCORD_WEBHOOK_URL_FILE` and `BEAT_TOKEN_FILE` read the same values from a file, such as a Docker secret under `/run/secrets/`.

- A `_FILE` variable that is set but empty, names a missing or unreadable file, or names an empty file stops startup. Only an unset `_FILE` falls back to the plain variable.
- The path must be clean, with no `..` segment, no doubled `/` and no trailing `/`.
- The file holds at most 1 MiB. One trailing line ending is removed.
- When both the file and the plain variable are set, the file wins and knell logs a warning naming the plain variable to unset.

## Node name

`NODE_NAME` names this instance in every notice. It defaults to the container hostname, or `unknown` when the hostname is blank. It holds at most 256 bytes, because it prefixes every notice and Discord caps a message at 2000 characters.

## Listen address

`LISTEN_ADDR` is the `host:port` knell binds, `:9190` by default. A blank value uses the default. A port another process holds stops startup.

## Host allowlist

`ALLOWED_HOSTS` is a comma-separated list of exact `Host` names to serve, such as `knell.internal,192.0.2.10`. Each entry is a bare hostname or IP with an optional port, with no scheme, path or CIDR.

- Unset, knell answers every `Host`.
- Set, a request with any other `Host` is refused with `403 host_not_allowed` on every endpoint. This is what blocks DNS rebinding from a browser inside your network.
- It covers `/healthz` and `/metrics` too, so list every name your probes and your Prometheus scraper use.
- A request from loopback to a loopback `Host` is always served.
- An entry no `Host` could ever match stops startup, because a dropped entry would leave pings to that name refused.
- The built-in `knell health` check reads a file and sends no request, so no allowlist can break it.

## Trusted proxies

`TRUSTED_PROXIES` lists the reverse proxies in front of knell as comma-separated CIDRs or bare IPs, such as `192.0.2.0/24,198.51.100.5`. knell believes their `X-Forwarded-For` header, so the `client_ip` on each access log line names the real sender.

- Unset, knell honors no forwarded header and `client_ip` is the connecting address.
- Set it when a TLS proxy sits in front. Otherwise every access line, including the `401` lines a token-guessing run writes, names the proxy instead of an address you can block.
- List exactly those hops. A range wider than your proxies lets anything inside it choose its own `client_ip`.
- A malformed entry is logged and dropped, and startup goes on.

## Log level

`LOG_LEVEL` is `debug`, `info`, `warn` or `error`, and `info` by default. An unknown value falls back to `info` with a warning. Keep `info` if you load the `KnellNoticeLostForGood` alert rule, because the lines it reads sit at `info` and `warn`. [Monitoring and alerts](monitoring.md) describes that rule.

## Startup summary

After the settings pass, knell logs one `watching beat` line per beat and one `configuration loaded` line. That line carries `beats`, `node`, `listen_addr`, `webhook` and `beat_token` as `env` or `file`, `allowed_hosts` as `any` or `allowlist(N)`, the `trusted_proxies` count and `log_level`. It never carries a credential. A misspelled `ALLOWED_HOSTS` looks the same as an unset one, `allowed_hosts=any`, so check that field after you set it.

## What stops startup

knell refuses to start rather than run with a setting it cannot honor, because a dead man's switch running with the wrong configuration is worse than one that does not start. These stop it:

- a `BEATS` entry that is not `id:deadline` with a valid id and a deadline of at least `30s`, a duplicate id, or more than 64 beats,
- a webhook URL that is not `https`, has no host or no path, or holds a space or an invisible character,
- a `NODE_NAME` over 256 bytes,
- an `ALLOWED_HOSTS` entry no `Host` could match,
- a `BEAT_TOKEN` that is unset, empty, outside 16 to 512 bytes, wrapped in whitespace or carrying a forbidden control character,
- a `_FILE` variable that is set but unusable,
- a listen address it cannot bind.

Each one logs `knell exited with error` with an `error` field naming the setting and the fix.
