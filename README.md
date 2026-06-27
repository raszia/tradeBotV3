# v3TradeBot

A multi-binary, crash-safe arbitrage trading system between **Binance** and
**Iranian exchanges**. MariaDB is the source of truth; Redis carries live market
data only.

> **Read [PROJECT_ARCHITECTURE.md](PROJECT_ARCHITECTURE.md) first.** It is the
> authoritative technical description and is kept up to date with every change.

## Status

**PR1 — project skeleton and shared foundation.** All binaries boot, verify the
database schema, and idle. No trading logic yet (lands in later, separately
reviewed PRs — see the PR history in the architecture doc).

## Binaries (`cmd/`)

`collector`, `trade-engine`, `order-executor`, `reconciler`, `balance-sync`,
`health-monitor`, `dashboard`, `retention-worker`, `migrate`.

## Quickstart (local)

Requires Go 1.25+, MariaDB 10.6+, and Redis.

```sh
# 1. Configure (FILE ONLY — this project does not read configuration from env vars).
cp configs/config.example.toml configs/config.toml   # then edit the DSN

# 2. Apply database migrations (the only normal-ops schema writer).
make migrate-up                                  # ./cmd/migrate, default -config configs/config.toml
go run ./cmd/migrate -config /etc/v3/config.toml # or point at another file

# 3. Build everything / run a binary (each takes -config).
make build                                        # binaries land in ./bin
go run ./cmd/dashboard -config configs/config.toml  # serves /healthz

# 4. Tests.
make test
# Migration integration test against a THROWAWAY DB (V3_TEST_MYSQL_DSN is a
# TEST-only switch for the test harness — not runtime config):
V3_TEST_MYSQL_DSN='root:pw@tcp(127.0.0.1:3306)/v3_scratch' go test ./internal/migrate/...
```

## Configuration

Configuration is read from a **TOML file only** (located via the `-config` flag,
default `configs/config.toml`); **no environment variables** are used. The file
holds only **bootstrap** settings (DSN, Redis address, dashboard bind address,
master key, log settings). **All trading/operational configuration** (exchanges,
symbols, spreads, sizes, fees, retention, regime, concurrency limits) and
**exchange API keys** are stored in the database — the API keys encrypted at rest
— and edited from the dashboard.
