# v3TradeBot — builds every service binary into one small static image (PR27).
# No config or secrets are baked in: configs/config.toml is mounted at runtime.
FROM golang:1.25 AS build
WORKDIR /src

# Cache modules first.
COPY go.mod go.sum ./
RUN go mod download

# Build all cmd/* binaries (collector, trade-engine, order-executor, reconciler,
# balance-sync, health-monitor, dashboard, retention-worker, migrate) into /out.
COPY . .
RUN CGO_ENABLED=0 GOFLAGS=-trimpath go build -o /out/ ./cmd/...

# Minimal runtime: static distroless (includes CA certs for TLS to exchange APIs).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
# The config is mounted by the operator (compose / k8s); none is baked in.
# Default command shows usage; compose overrides `command:` per service.
ENTRYPOINT ["/usr/local/bin/dashboard"]
CMD ["-config", "/etc/v3tradebot/config.toml"]
