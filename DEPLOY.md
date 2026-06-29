# v3TradeBot — Local / Staging Deployment & Dry-Run Readiness (PR27)

This packages the system for **local/staging** with **no real credentials and no live
trading**. Default execution mode is `off`/`dry_run`; the kill switch starts engaged; no
exchange API key is required. Live trading is a separate, gated operator flow (`RUNBOOK.md`).

## 1. What ships

- `Dockerfile` — one small static image with every binary (`collector`, `trade-engine`,
  `order-executor`, `reconciler`, `balance-sync`, `health-monitor`, `dashboard`,
  `retention-worker`, `migrate`). No config/secret is baked in.
- `docker-compose.yml` — MariaDB 10.6 + Redis 7 + a one-shot `migrate` + all 8 services.
- `configs/production.example.toml` — placeholders only (no API keys, no plaintext
  credential, `master_key` empty, `[execution] mode = "off"`).

## 2. Startup order (services NEVER self-migrate)

1. **Start the database** (MariaDB 10.6+). In compose this is the `mariadb` service with a
   healthcheck.
2. **Start Redis** (live market data only — losing it never loses trading state).
3. **Run `migrate`** to apply the schema. Every service calls `migrate.EnsureCurrent` on
   startup and **fails fast** if the schema is behind the code, so migrate must complete
   first. In compose the services `depend_on` migrate via
   `service_completed_successfully`.
4. **Start the services** (collector, trade-engine, order-executor, reconciler,
   balance-sync, health-monitor, retention-worker, dashboard).
5. **Verify the dashboard** — `http://localhost:8080/healthz` returns `ok`; `GET /api/live`
   shows mode `off`/`dry_run` and the kill switch engaged.
6. **Verify public market data** — once the collector has targets configured + enabled,
   `GET /api/comparisons` / `GET /api/regime` show fresh rows; Redis holds `orderbook:`/
   `price:` keys.
7. **Verify a dry-run** (section 4).

## 3. Bring it up

```sh
cp configs/production.example.toml configs/config.toml
# Edit configs/config.toml: keep mariadb/redis hosts, set the DB password, leave master_key
# empty (credential-free), and set [execution] mode = "off" (or "dry_run").
docker compose up -d --build
docker compose logs -f migrate          # confirm "applied" / "schema current"
curl -s localhost:8080/healthz          # -> ok
```

Each live-capable binary logs a secret-free **startup safety summary** (execution mode, live
enabled, kill switch, canary scope, caps, credential status, active session, whether new live
buys are allowed) so you can confirm the danger level at a glance.

## 4. Local/staging dry-run (full lifecycle, ZERO real exposure)

Set `[execution] mode = "dry_run"` and restart `trade-engine` + `order-executor`. Dry-run
wires **simulated** clients (`internal/simexec`) — no network, no real `PlaceOrder`/
`CancelOrder`. With a market enabled for collection/signal/trading and a fresh book, a full
cycle runs through the REAL boundaries:

`signal → cycle → symbol lock → buy request → executor claim → simulated fill → sell → close`

Watch it:
- `GET /api/cycles/open` / `…/closed` — the cycle progresses to `CLOSED` (dry_run flagged).
- `GET /api/orders`, `/api/fills`, `/api/requests` — the simulated lifecycle, all `DRY_RUN`.
- `GET /api/logs`, `/api/api-logs` (masked) — audit trail, no secrets.
- A successful dry-run (`CLOSED`) is also the **prerequisite** the live preflight checks
  before the first real order.

`scripts/local-dryrun-check.sh <dashboard-url>` is a small read-only smoke check (healthz +
no-secret responses + mode).

## 5. Safe defaults (enforced)

- `[execution] mode` defaults to `off`; the example never sets `live`.
- `live_controls.kill_switch` column **defaults engaged (1)**.
- No `live_run_sessions` active → live buys denied even if everything else were set.
- Empty `master_key` → credential loading + live execution are safe-disabled.
- No exchange API key in any file — credentials are DB-encrypted and provisioned via the
  dashboard (`RUNBOOK.md`).

## 6. What remains after PR27

Production hardening of the image/orchestration (resource limits, secrets management, a real
reverse proxy + TLS for the dashboard, k8s manifests if desired), and the operator-driven
first real venue order (gated by the full PR20–PR25 stack). Rule #3 keeps automated tests
venue-free.
