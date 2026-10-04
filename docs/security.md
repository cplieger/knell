# Security

This page covers what knell exposes, how to put it behind a reverse proxy and how to run it with a hardened compose profile. It is for operators who publish knell's port beyond one host.

## What is exposed

knell listens on one port, `9190` by default, and serves plain HTTP.

- `POST /beat/<id>` needs `BEAT_TOKEN`. Without it, a stranger who can reach the port could send forged pings that keep a dead job looking alive.
- `/healthz` and `/metrics` need no token, so probes and scrapes keep working. `/metrics` publishes every beat id with its last ping and its state. Anyone who can reach the port can list the beats and see which one is about to fire.

The compose example maps the port on every host interface. Publish it to a trusted network only, or put an authenticating proxy in front of `/metrics`.

## The token on the wire

knell has no TLS of its own, so the token crosses the network in cleartext on every ping. Anything that can read one ping can replay it forever. Put a TLS reverse proxy in front of knell, or keep pings on a network you trust to that same standard.

Pings with a wrong or missing token share one throttle budget. Once it is spent they are answered `429` with a `Retry-After` hint, which limits both token guessing and the log lines it writes. A ping with the right token is never throttled.

## Behind a reverse proxy

Set `TRUSTED_PROXIES` to the addresses of your proxies. Otherwise every access line, including the `401` lines a token-guessing run writes, names the proxy instead of an address you can block. List exactly those hops, because a wider range lets anything inside it choose its own `client_ip`.

A proxy that adds many headers can push a ping past knell's 8704-byte header limit, which answers `431`. Trim the headers the proxy adds if that happens.

## Browsers and DNS rebinding

A trusted network is not enough against a browser. A web page an operator opens can make their browser reach knell under a hostname an attacker controls. `ALLOWED_HOSTS` refuses every `Host` it does not list, which blocks that. List every name your senders, probes and Prometheus scraper use.

Check that the list took effect in the `configuration loaded` line knell logs at startup. `allowed_hosts=allowlist(2)` means two names are set. `allowed_hosts=any` means every `Host` is accepted, which is also what a misspelled variable name looks like.

## Secrets

The webhook URL is a credential, because its path is the token Discord issued. knell accepts it over `https` only and never writes it to a log line or an error message. The `configuration loaded` line reports only whether each credential came from its variable or from a `_FILE` secret. Use `DISCORD_WEBHOOK_URL_FILE` and `BEAT_TOKEN_FILE` to keep both out of `docker inspect` output, as [Configuration](configuration.md#secret-files) describes.

## What the image contains

The image is built on `scratch`. It holds the static knell binary, a CA certificate bundle for the HTTPS connection to Discord and an empty `/tmp` for the health marker. It also carries the license text of every bundled component under `/usr/share/licenses/`. It has no shell. knell runs as the numeric non-root user 65534 and writes only its health marker in `/tmp`.

## Hardened compose profile

Add these lines to the `knell` service to run it with a read-only root filesystem, no Linux capabilities and no privilege escalation. The `tmpfs` gives the health marker somewhere to live.

```yaml
    read_only: true
    cap_drop: [ALL]
    security_opt:
      - no-new-privileges:true
    tmpfs:
      - /tmp:rw,noexec,nosuid,nodev,size=16m,mode=1777
```
