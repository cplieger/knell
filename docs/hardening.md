# Security

This page covers what knell exposes, how to put it behind a reverse proxy and how to run it with a hardened compose profile. It is for operators who publish knell's port beyond one host.

## What is exposed

knell listens on two ports and serves plain HTTP on both. Pings use `9190` (`LISTEN_ADDR`), and `/healthz` and `/metrics` use `9191` (`OPS_LISTEN_ADDR`).

- `POST /beat/<id>` needs a token from `BEAT_TOKENS` that names that beat. Without one, a stranger who can reach the port could send forged pings that keep a dead job looking alive.
- `/healthz` and `/metrics` need no token, so probes and scrapes keep working. `/metrics` publishes every beat id with its last ping and its state. Anyone who can reach port `9191` can list the beats and see which one is about to fire.

The compose example maps `9190` on every host interface, so publish it to a trusted network only. It leaves `9191` unpublished. A scraper on the network it shares with knell reaches `knell:9191`. To probe from another host, publish `9191` to the address that host reaches, such as `"192.0.2.10:9191:9191"`, and never to the internet.

## Tokens on the wire

knell has no TLS of its own, so a token crosses the network in cleartext on every ping. Anything that can read one ping can replay it until you replace that token. Put a TLS reverse proxy in front of knell, or keep pings on a network you trust to that same standard. Never ping with `curl -v`, which prints the `Authorization` header into the job's log.

Each beat has its own token, so a leaked or replayed token keeps only the beats its line names looking alive. Make every token with `openssl rand -hex 16`, because knell checks a token's length, not how hard it is to guess.

## Failed tokens

knell limits failed tokens per client address, and counts an IPv6 address as its /64.

- An address that sends 10 wrong tokens within 15 minutes is throttled.
- All addresses together may send 60 wrong tokens within 15 minutes. Past that, every address is throttled except one whose ping was answered `200` recently and that has sent no wrong token in the last 15 minutes.
- A throttled address gets `429` with a `Retry-After` hint, whatever token it sends, so a guesser cannot tell a right token from a wrong one. knell still checks the token and records a valid ping behind that `429`, so throttling never makes a beat look missing.
- Only a ping answered `200` makes an address exempt. A sender answered `429` stays throttled until the limit it hit clears.
- The limit stops a guesser from confirming a token, not from keeping a beat alive with a right one it sends blind. Only a token from `openssl rand -hex 16` rules that out.
- A throttled request writes no `info` access line, so a guessing run cannot flood the log.

`KnellAuthFailures` and `KnellPingsThrottled` report both cases, as [Monitoring and alerts](monitoring.md#alerting) describes.

## Reaching knell from the internet

Expose knell to the internet only through a TLS reverse proxy, and set it up like this:

1. Publish only the proxy. The proxy terminates TLS and forwards to knell's port `9190`.
2. Never publish `9191`. Your scraper and prober reach it on the network they share with knell.
3. Set `TRUSTED_PROXIES` to the proxy's address. Without it, every client behind the proxy shares one failure limit, and knell logs a warning the first time a ping with a valid token comes through the proxy. Check that the `peer` it names is your proxy before you trust it.
4. Set `ALLOWED_HOSTS` to the public name plus every name your local jobs, prober and scraper use, such as `knell.example.com,knell`. It covers both ports.
5. Give each beat its own token from `openssl rand -hex 16`, in `BEAT_TOKENS_FILE`.

Caddy hides the `Authorization` header in its access log unless you set [`log_credentials`](https://caddyserver.com/docs/caddyfile/options#log-credentials). This Caddyfile site does the forwarding:

```caddyfile
knell.example.com {
	reverse_proxy knell:9190
}
```

## Behind a reverse proxy

[Running an app behind a reverse proxy](https://github.com/cplieger/docs/blob/main/docs/reverse-proxy.md) sets knell up behind Caddy, nginx, Traefik or Nginx Proxy Manager. Set `TRUSTED_PROXIES` to the addresses of your proxies. Otherwise every access line names the proxy instead of an address you can block, and every client behind it shares one failure limit. List only those hops, as [Telling the app about the proxy](https://github.com/cplieger/docs/blob/main/docs/reverse-proxy.md#telling-the-app-about-the-proxy) explains.

A proxy that adds many headers can push a ping past knell's 8704-byte header limit, which answers `431`. Trim the headers the proxy adds if that happens.

## Browsers and DNS rebinding

A trusted network is not enough against a browser. A web page an operator opens can make their browser reach knell under a hostname an attacker controls. `ALLOWED_HOSTS` refuses every `Host` it does not list, which blocks that. List every name your senders, probes and Prometheus scraper use.

Check that the list took effect in the `configuration loaded` line knell logs at startup. `allowed_hosts=allowlist(2)` means two names are set. `allowed_hosts=any` means every `Host` is accepted, which is also what a misspelled variable name looks like.

## Secrets

The webhook URL is a credential, because its path is the token Discord issued. knell accepts it over `https` only and never writes it to a log line or an error message. The `configuration loaded` line reports only whether each credential came from its variable or from a `_FILE` secret. Use `DISCORD_WEBHOOK_URL_FILE` and `BEAT_TOKENS_FILE` to keep both out of `docker inspect` output, as [Configuration](configuration.md#secret-files) describes.

## What the image contains

The image is built on `scratch`. It holds the static knell binary, and a CA certificate bundle for the HTTPS connection to Discord. It also carries the license text of every bundled component under `/usr/share/licenses/`. It has no shell. knell runs as the numeric non-root user 65534 and writes no file. Its beats and its health verdict live in memory.

## Hardened compose profile

Add these lines to the `knell` service to run it with a read-only root filesystem, no Linux capabilities and no privilege escalation. knell writes no file, so it needs no `tmpfs` or volume. `/healthz` never depends on a writable folder.

```yaml
    read_only: true
    cap_drop: [ALL]
    security_opt:
      - no-new-privileges:true
```

[The hardened example](https://github.com/cplieger/docs/blob/main/docs/hardening.md#the-hardened-example) runs knell with every setting, including secret files and resource limits.
