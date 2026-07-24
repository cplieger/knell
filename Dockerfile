# check=error=true
FROM golang:1.26-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS builder
ENV GOTOOLCHAIN=auto

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY *.go ./
COPY internal/ internal/
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /knell .

# Directory skeleton for the scratch stage: /tmp for the health marker
# (world-writable + sticky so any runtime uid works).
RUN mkdir -p /outfs/tmp && chmod 1777 /outfs/tmp

FROM scratch

# CA bundle for the outbound Discord webhook TLS handshake.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
# --chmod: a plain COPY recreates the target dir 0755 regardless of the source
# mode, and engines that replicate the image dir's mode onto a tmpfs mount
# (observed on Docker 24 / DSM) then make /tmp unwritable for the nonroot
# user even when the compose tmpfs says mode=1777. Bake the 1777.
COPY --from=builder --chmod=1777 /outfs/tmp /tmp
COPY --from=builder --chmod=755 /knell /knell

# Non-root numeric uid:gid (scratch has no /etc/passwd). knell binds a high
# port and writes only its /tmp health marker, so it never needs root.
USER 65534:65534
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=15s \
    CMD ["/knell", "health"]
ENTRYPOINT ["/knell"]
