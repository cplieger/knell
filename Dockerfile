# check=error=true
FROM golang:1.27-alpine@sha256:7d5cbf6833f7331dafd25a2e8b9673477f559759ff8ed4ca8efabe6795ad08db AS builder
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

# Directory skeleton for the scratch stage: /tmp for the health marker. The
# mode is deliberately NOT set here: COPY recreates the destination directory
# and takes its mode from --chmod below, never from this source dir.
RUN mkdir -p /outfs/tmp

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
