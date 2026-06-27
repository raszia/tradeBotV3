# v3TradeBot — Project Architecture

> **This document is part of the source code.** Every PR that adds, removes, or
> changes anything important (a table, a state transition, a binary, a Redis key,
> queue/config/recovery behaviour, a safety rule, a limitation, or a deferral)
> MUST update this file in the same PR. Outdated docs are treated as a bug.

Last updated: **PR1 — Project skeleton and shared foundation.**

---

## 1. System goal

We receive crypto prices and order books from **Binance** and from **Iranian
exchanges** (Nobitex, Wallex, Bitpin, Ramzinex, Tabdeal, Exir, ...). When an
Iranian exchange is cheaper than Binance by at least a configurable number of
basis points (bps), the system **buys on the Iranian exchange**, then places a
**sell order slightly below the Binance price**. As the Binance price moves, the
system may cancel and replace (reprice) the sell order, but never more often than
a configurable minimum interval (seconds). After the buy is matched the result is
persisted; the sell order continues to be managed as the Binance price moves.

This project implements the **software architecture** around that strategy so it
stays consistent and recoverable across restarts, timeouts, partial fills,
WebSocket disconnects, Redis resets, database errors, and exchange-side
inconsistencies. **The trading logic itself is owner-defined and is not modified
here.**

## 2. Fixed trading logic summary (do not change)

1. Compare Iranian-exchange price vs Binance price; if Iranian is cheaper by
   ≥ `min_spread_bps` (per-symbol, fee-adjusted), a buy signal is produced.
2. Buy on the Iranian exchange.
3. After the buy matches, place a sell order slightly below the Binance price
   (`sell_offset_bps`).
4. When the Binance price changes, optionally cancel/replace the sell order,
   honoring a minimum `reprice_interval_seconds` between repricing actions.
5. Persist the cycle result; keep managing the sell order as Binance moves.

The architecture treats these as **parameters and a flow**, never hardcoding the
numbers — they come from versioned config (and may be influenced by market
regime, configurably).

## 3. Binaries and responsibilities

The system is **multi-binary and decoupled**. Restarting any one binary (notably
the dashboard) must not affect the others. Each lives under `cmd/`.

| Binary | Responsibility | Exchange API? |
|---|---|---|
| `collector` | Normalize Binance + Iranian books/prices into Redis; publish price-change events; record collector health. No trading decisions, no orders. | read-only market data |
| `trade-engine` | Consume price events; read Redis + in-memory config/regime; compute spread & fee-adjusted spread; check symbol lock; write comparison_events/signals; create cycle, acquire lock, register order, enqueue buy/sell/cancel — all transactionally. | **never** |
| `order-executor` | The **only** component that issues order-mutating exchange API calls. Claims queue requests, sends, records responses, drives order/cycle state. | **yes** |
| `reconciler` | Startup + periodic reconciliation; recover cycles/locks; mark unclear cases `NEEDS_RECONCILE`. | read-only (GET_*) |
| `balance-sync` | Continuously poll balances → current + history (hash dedup). | read-only |
| `health-monitor` | Track per-exchange REST/WS/latency/error/auth/rate-limit health. | probes |
| `dashboard` | HTTP initial load + WebSocket live updates; config editing. **No trading control.** Separate binary. | no |
| `retention-worker` | Batched/partition-aware cleanup of high-volume tables. | no |
| `migrate` | Apply embedded migrations and exit. The only normal-ops schema writer. | no |

## 4. Package / module structure

Module `v3TradeBot` (Go 1.25).

```
cmd/<binary>/main.go            one main per binary
internal/
  config/      bootstrap config (DSN/addresses/ports/secrets) + redaction   [PR1]
  logging/     slog logger construction                                     [PR1]
  clock/       Clock seam for deterministic time in tests                   [PR1]
  db/          Store{*sql.DB}, pool tuning, WithTx (atomic critical writes)  [PR1]
  redis/       go-redis wrapper (live market data only)                     [PR1]
  migrate/     embed.FS migration runner + schema_migrations + migrations/  [PR1]
  service/     shared bootstrap: config+logger+signals+RunWithDB lifecycle  [PR1]
  models/      row structs + enum constants                                 [PR2/PR3]
  state/       cycle/order state machine (transitions + persisted events)   [PR3]
  events/      market events + normalized order-event model                 [PR5/PR10]
  domain/      normalized market types (ported)                             [PR4]
  execution/   normalized order-execution contracts (ported)                [PR4]
  exchanges/   exchange clients + factory + iolog + market rules (ported)   [PR4]
  orders/      order registration + order-event processing                  [PR9/PR10]
  queue/       DB-backed exchange-request priority queue                    [PR7]
  symbollock/  DB-backed symbol lock (composite scope)                      [PR9/PR12]
  reconciler/  startup + periodic reconciliation                            [PR12]
  collector/   collector service                                            [PR5]
  engine/      trade-engine loop                                            [PR8/PR9/PR11]
  executor/    order-executor worker pool                                   [PR7/PR10/PR11]
  balance/     balance sync + hash dedup                                    [PR13]
  health/      exchange health tracking                                     [PR14]
  regime/      market-regime baskets + calculation                          [PR15]
  dashboard/   HTTP + WebSocket + config editing                            [PR16/PR17]
configs/       bootstrap TOML example (trading config lives in the DB)
```

Reuse: `domain`, `execution`, `exchanges`, and the `db`/`redis` patterns are
copied and adapted from the sibling system at `/home/ras/myco/iranArb`.

## 5. Database schema overview

**MariaDB 10.6+ is the single source of truth.** Conventions: `BIGINT UNSIGNED`
PKs, `DECIMAL(36,18)` base qty / `DECIMAL(36,8)` quote-price, `TIMESTAMP(6)` /
`DATETIME(6)`, `JSON` payloads, explicit indexes, `version` columns for
optimistic concurrency.

Implemented so far (**PR1**):

- `schema_migrations` — applied migration versions + checksums (created by the runner).
- `app_meta` — minimal key/value infrastructure metadata (migration `001`).

Planned (**PR2**): exchanges, assets, markets, exchange_markets, config_versions,
symbol_configs, exchange_configs, cycles, cycle_state_events, orders,
order_events, fills, symbol_locks, exchange_requests, wallet_balances_current,
wallet_balance_history, exchange_health_current, exchange_health_samples,
api_call_logs, app_logs, comparison_events, signals, exchange_fees,
cycle_fee_snapshots. **PR15**: market_regime_baskets, market_regime_basket_symbols,
market_regime_timeframes, market_regime_current, market_regime_history.

Records that are kept permanently (unless explicitly configured otherwise):
trades/orders/fills/cycles. High-volume tables (api_call_logs, comparison_events,
exchange_health_samples, app_logs, market_regime_history) are subject to
retention.

## 6. Migration rules

All schema/reference-data changes ship as embedded `internal/migrate/migrations/
NNN_name.sql` files applied by the `migrate` binary. **No external/manual SQL.**

- **Serialization:** a MariaDB session advisory lock (`GET_LOCK`) on a single
  **pinned connection** prevents two `migrate` runs from double-applying. All
  statements in a run execute on that same pinned connection.
- **Tracking & checksums:** `schema_migrations` records each applied version with
  the file's SHA-256 checksum. **Editing an applied migration changes its
  checksum and is a HARD STOP** — add a new `NNN` file instead.
- **DDL vs DML (MariaDB DDL auto-commits, so DDL is NOT truly transactional):**
  each file declares its kind via a leading comment directive:
  - `-- migrate:dml` → statements **and** the `schema_migrations` row run inside
    one transaction (atomic).
  - `-- migrate:ddl` → statements run directly (each auto-commits); the version
    row is written only **after** all statements succeed. DDL migrations must be
    **one idempotent change** (`CREATE TABLE IF NOT EXISTS`, ...) so a re-run
    after a mid-file failure is safe. (Default when no directive is present.)
  - **Never mix dangerous DDL and data changes in the same file.**
- **Statement splitting** handles quotes/backticks/line+block comments/escapes;
  stored programs / `DELIMITER` are unsupported in migrations.
- **Fail-fast:** service binaries call `migrate.EnsureCurrent` on startup and
  refuse to run if any migration is pending. Services never self-migrate.

## 7. Redis key structure

Redis holds **live market data only** and is **never** the source of truth. Key
schema is finalized in PR5; the planned layout (ported from the sibling system):

- `orderbook:{exchange}:{canonical_symbol}` — latest normalized order book (JSON).
- `price:{exchange}:{canonical_symbol}` — latest price/best bid-ask (JSON).
- Pub/sub channel for price/book updates with a compact `exchange|symbol` payload.

If Redis is flushed or restarted, the system loses at most fresh market data
(repopulated by collectors); it never loses cycles, orders, fills, locks,
balances, or queue state.

## 8. Exchange-request queue design (PR7)

A database-backed **priority queue** decouples decision-making (trade-engine,
which enqueues) from API calls (order-executor, which claims and sends).

- **`exchange_requests`** columns include: exchange, symbol, cycle_id, order_id,
  request_type (`PLACE_ORDER`/`CANCEL_ORDER`/`GET_ORDER`/`GET_OPEN_ORDERS`/
  `GET_BALANCE`/...), priority, **status**, payload (JSON), timeout_ms,
  retry_count, max_retries, next_retry_at, **idempotency_key (UNIQUE)**,
  claimed_by, claimed_at, response, created_at, updated_at.
- **Status enum (fixed system-wide):** `QUEUED`, `CLAIMED`, `IN_FLIGHT`,
  `SUCCEEDED`, `FAILED`, `RETRY_SCHEDULED`, `DEAD`. These exact names are used in
  the schema, models, queue code, tests, and dashboard.
- **Claiming** uses `FOR UPDATE SKIP LOCKED` (MariaDB 10.6+), ordered by priority
  then age, respecting a **per-exchange concurrency limit**.
- **Per-exchange concurrency across multiple executor processes:** the limit must
  hold even if more than one `order-executor` runs. It is protected at the DB
  level (per-exchange `GET_LOCK` around count+claim, or slot/lease rows). **Until
  distributed slot leasing is implemented, only one `order-executor` instance per
  exchange is supported, and that constraint is documented — not assumed away.**
- **Transactional completion:** on a successful response, the queue row status
  and the order state/event are updated in the **same transaction**.
- **Idempotency & crash-after-send:** see §15.

## 9. State-machine design (PR3)

Cycle and order states change **only** through defined transitions; every
transition is persisted as an event row, and rows carry a `version` column for
optimistic concurrency (the update is guarded by `WHERE id=? AND state=? AND
version=?`). Transition + event insert + any side effect (e.g. enqueue) share one
transaction.

- **CycleState:** `NEW`, `SIGNAL_DETECTED`, `BUY_REQUEST_QUEUED`, `BUY_SUBMITTED`,
  `BUY_PARTIALLY_FILLED`, `BUY_FILLED`, `SELL_REQUEST_QUEUED`, `SELL_SUBMITTED`,
  `SELL_REPRICE_PENDING`, `SELL_PARTIALLY_FILLED`, `SELL_FILLED`, `CANCEL_PENDING`,
  `CANCELLED`, `FAILED`, `NEEDS_RECONCILE`, `CLOSED`.
- **OrderState:** `NEW`, `REGISTERED`, `QUEUED`, `SUBMITTED`, `ACKED`,
  `PARTIALLY_FILLED`, `FILLED`, `CANCEL_PENDING`, `CANCELLED`, `REJECTED`,
  `EXPIRED`, `FAILED`, `NEEDS_RECONCILE`.
- `NEEDS_RECONCILE` is reachable from any non-terminal state and is exited only by
  an **operator** path — never automatically.

## 10. Symbol-lock design (PR9/PR12)

A DB-backed lock prevents a symbol with an active cycle from accepting a new
signal. It survives restarts and is recoverable by the reconciler.

- **Scope is explicit and composite** — it includes the **exchange** and the
  **canonical market** (and, later, strategy), e.g. `nobitex|BTC/USDT`. So
  `BTC/USDT` on Nobitex does not block `BTC/USDT` on Wallex unless a deliberately
  global scope is configured.
- Acquiring the lock, creating the cycle, and enqueuing the first request happen
  in **one transaction** (no lock without a cycle; no cycle without its request).
- A unique-when-active constraint (generated column) enforces one active lock per
  scope; released locks free the scope.
- The reconciler reclaims a stale lock **only after** positively determining the
  owning cycle is safe — never on lease expiry alone.

## 11. Reconciler behaviour (PR12)

On startup (poll-only — WebSocket carries no history for the downtime gap): load
open cycles/orders; per exchange fetch open orders, balances, recent fills;
compare; per cycle decide **Continue / SafeClose / NEEDS_RECONCILE**. Locks are
released only on `SafeClose`. The reconciler **never auto-cancels and never
auto-sends**; the safe default for any ambiguity is `NEEDS_RECONCILE`.
`SafeClose` requires positive proof every leg is terminal on the exchange (an
order merely missing from open-orders is **not** proof it never filled). A
periodic poll backstop complements steady-state WebSocket order updates.

## 12. Exchange abstraction design (PR4)

A normalized interface (`GetMarkets`, `GetBalances`, `PlaceOrder`, `CancelOrder`,
`GetOrder`, `GetOpenOrders`, `SubscribeOrderBook`, `SubscribeOrderUpdates`) hides
per-exchange differences, with normalized models (market, order book, balance,
order status, **order event**, API error). Both WebSocket updates and polling
results convert into the **same** normalized order-event model, so the order
processor has one code path regardless of how an exchange reports.

Order **type/time-in-force** is a field set by the owner's trading logic; the
abstraction does not impose IOC or any strategy. Clients are ported/adapted from
the sibling system; **no new clients are written from scratch** without first
inspecting the existing code (decision: copy & adapt from `iranArb`).

## 13. Config & versioning design (PR6)

**Bootstrap vs database config — a hard split:**

- **Bootstrap config** (`internal/config`) is read from a **TOML file only**,
  located via each binary's **`-config` flag** (default `configs/config.toml`).
  **This project does NOT read configuration from environment variables** — no
  DSN, password, key, or parameter is taken from the environment. The file holds
  only the minimal settings needed before the database is reachable: MySQL DSN
  (embeds the DB password), Redis address/password, dashboard bind address, log
  level/format, and the master encryption key. Secrets are redacted before
  logging (`Config.LogValue`).
- **Everything operational lives in the database**, not the file: enabled/
  disabled exchanges and symbols, per-symbol min spread bps, buy size, sell
  offset bps, reprice interval, timeouts, retry settings, per-exchange
  concurrency limits, fees, regime settings, retention settings, market-discovery
  settings, and any other trading/operational parameter.
- **Exchange API keys/secrets are NOT in the config file.** They are stored in
  the database, **encrypted at rest** (via the master key), edited from the
  dashboard, and **masked in all outputs** (logs, dashboard, API responses, raw
  request/response logs). A temporary development-only path, if ever added, must
  be clearly marked temporary and never used in production. (Schema: PR2.)

**All database config is versioned** and edited from the dashboard. Each config
change records who/old/new/version/activation-time (auditable). The engine loads
config into an in-memory cache and reloads it **without blocking the trading
path**. Each cycle stores the `config_version` (and regime config version) used
when its signal was created.

## 14. Dashboard responsibilities (PR16/PR17)

Initial load over HTTP; live updates over WebSocket. Shows full trade detail
(signal time, Binance/Iranian price at signal, spread, fee-adjusted spread, buy/
sell results, market regime + level, config version, order states, fills, fees,
raw API logs, exchange health) plus balances, signals, comparison events, and
regime. Edits config (creating new versions / audit records) and per-exchange
concurrency limits. **It never places, cancels, or reprices orders directly** —
trading stays in the engine/executor/reconciler flow. Separate binary, so
restarting it never affects trading.

## 15. Market regime design (PR15)

Configurable **baskets** of Binance symbols define overall market condition.
A basket config has: name, enabled flag, symbol list, timeframe windows (e.g.
1m/3m/5m/15m), optional per-symbol weights, regime-level thresholds, update
interval, and config version. The regime module reads Binance data from **Redis**
(it must **not** call Binance directly) and produces a normalized result:
direction, level, confidence, basket score, per-timeframe score, per-symbol
contribution, timestamp, config version — stored as both **current** state and
**history**. The module is decoupled from the trade-engine, which reads the
latest regime from cache/DB. Each cycle stores a **regime snapshot** at signal
time. Regime may influence configurable parameters (accept/reject, signal score,
buy size, min spread, sell offset, repricing) — never hardcoded. The exact
formula may start as a placeholder; the data model and flow are correct from the
start.

## 16. Logging and retention rules

**File/stderr logs vs database logs — what goes where:**

- **File/stderr logs are for critical service-lifecycle and infrastructure
  events only:** service started/stopped, config loaded, DB connection failed,
  Redis connection failed, migration pending/mismatch, panic/crash, fatal startup
  error. They answer "did this binary start, stop, crash, or fail to connect?"
- **Operational/trading logs live in DATABASE tables, not files:** order-lifecycle
  logs, raw API request/response logs, exchange health samples, comparison events,
  signals, balance updates, queue events, reconciler decisions, dashboard/config
  audit events.
- **File-log retention/rotation:** the structured logger currently writes to
  stderr (no log files). **If/when file logging is added, it must rotate** by
  maximum age, maximum file size, and maximum number of retained files — local
  log files must never grow forever. (Implementation when file logging is added.)

- **Structured logging** via slog (`internal/logging`); JSON by default.
- **Raw API request/response logging** (PR4/PR7): exchange, method, URL, request
  headers, request body, response status/headers/body, latency, error, timeout
  flag, created_at — with **API keys, secrets, signatures, authorization headers,
  and tokens masked/omitted before storage** (see §17).
- **Async batched writes** for high-volume data (raw API logs, comparison events,
  app logs, health samples, regime history); **synchronous transactional writes**
  for critical state (cycle/lock/order/queue/transition/fill).
- **Retention** (PR18) for high-volume tables, configurable by days (and, where
  partitioned, by retained-partition count ≈ volume), using batched/partition-
  aware deletes. Trades/orders/fills/cycles kept permanently by default.
- **Partitioning note (MariaDB):** a partitioned InnoDB table requires every
  unique/primary key to include the partition column. High-volume tables will
  therefore include the time/partition column in their PK/unique keys, or start
  **non-partitioned with strong `created_at` indexes + batched retention** and
  add partitioning in a later PR. (Decided per-table in PR2/PR18.)

## 17. Safety rules (the hard rules)

1. **No real order before it is recorded.** create cycle/order/request in MySQL →
   commit → executor reads the queued request → sends → records response. If the
   DB is unavailable, fail safe and send nothing.
2. **MySQL is the source of truth** for active cycles, orders, fills, symbol
   locks, the request queue, config version used, and recovery state. **Redis is
   never the source of truth.**
3. **Critical trading state is never memory-only.** Caches are for performance.
4. **Don't trust WebSocket blindly** — events may be delayed/missed/duplicated;
   polling + reconciliation are always available as backup.
5. **Reconciler is part of the first usable version** (PR12, inside the safety
   core), not deferred to the end.
6. **Deterministic order lifecycle** — only defined state transitions; no ad-hoc
   updates from multiple places.
7. **Every external request** has timeout, retry policy, idempotency key (when
   applicable), request id, exchange id, latency measurement, raw log, final
   status.
8. **Internal client order id always.** Every order has a `local_client_order_id`
   even if the exchange has no client-order-id support; nothing is sent before it
   is registered.
9. **Idempotency / crash-after-send (conservative):** an `idempotency_key` is
   kept internally for every request and passed to the exchange as the client
   order id **only when the exchange supports it**. After a crash with a request
   stuck `IN_FLIGHT`, a `PLACE_ORDER`/`CANCEL_ORDER` is **never blindly
   re-sent**. Recovery is conservative:
   - if the exchange order id is known → `GetOrder(exchange_order_id)`;
   - else try to positively identify the order via safe exchange data (recent
     open orders / recent fills matched on side/symbol/price/quantity/time
     window);
   - if not positively identified → mark the order/cycle `NEEDS_RECONCILE`;
   - re-send only when provably safe.
10. **Secrets never leak into logs** — masked/encrypted before storage; the
    master key and auth headers are never logged.
11. **Config changes are versioned and auditable.**

## 18. Known limitations (current)

- **PR1 is skeleton only.** Service binaries boot, verify the schema, and idle;
  no market data, no signals, no orders, no reconciliation yet.
- Only `schema_migrations` and `app_meta` exist; the trading schema is PR2.
- The exchange-request queue's multi-process per-exchange concurrency protection
  and the conservative crash-after-send recovery are **designed** (§8, §17) but
  implemented in PR7+. Until then, assume one executor per exchange.
- Partitioning decisions for high-volume tables are deferred to PR2/PR18 (may
  start non-partitioned).
- The migration statement splitter does not support stored programs/`DELIMITER`.

## 19. Pending work (delivery plan)

First delivery is the **safety core: PR1–PR7 + PR12**, built and then stopped for
review before anything can trade. Subsequent phases (one reviewed PR at a time):
PR8 (engine signal loop), PR9 (cycle/lock/buy enqueue), PR10 (order-status/fill
processing), PR11 (sell + repricing), PR13 (balance sync), PR14 (health),
PR15 (regime), PR16 (dashboard read views), PR17 (dashboard config editing),
PR18 (retention), PR19 (dry-run), PR20 (limited live).

## 19a. Decisions log

- **PR1 (amended) — config is file-only; no environment variables.** Bootstrap
  config is read from a TOML file located via the `-config` flag (default
  `configs/config.toml`); a missing file is a hard error (no env fallback). The
  earlier env-override mechanism (`V3_*` vars) was removed. Rationale: the owner
  requires that secrets/DSN/params not be loaded from the environment; bootstrap
  secrets go in the file, and all trading params + exchange API keys live in the
  database. (`V3_TEST_MYSQL_DSN` remains a TEST-only switch for the gated
  migration integration test — it is not runtime configuration.)
- **PR1 — `go.mod` kept tidy.** `go mod tidy` removed dependencies that were
  pre-declared but not yet imported by PR1 code: `centrifugal/centrifuge-go`,
  `gorilla/websocket`, `pquerna/otp`, `prometheus/client_golang` +
  `client_model`, `shopspring/decimal`, `golang.org/x/crypto`,
  `golang.org/x/net` (plus their indirect deps). This keeps `go.mod` honest (a
  reviewer's `go mod tidy` shows no diff). They return automatically when the
  code that needs them lands: decimal/websocket/otp/crypto in PR4 (exchange
  layer) and PR16/17 (dashboard); prometheus when metrics are wired. No risk to
  PR2 (schema/SQL only) or PR4 (deps re-added by `go get`/`tidy` as imports
  appear). Retained: `go-sql-driver/mysql`, `redis/go-redis/v9`,
  `pelletier/go-toml/v2`, `DATA-DOG/go-sqlmock`.

## 20. PR history / implementation phases

| PR | Branch | Status | Summary |
|---|---|---|---|
| PR1 | `pr1-project-skeleton` | **in review** | Project skeleton & shared foundation: module layout, all 9 binaries bootable, **file-only bootstrap config** (no env; `-config` flag; secret redaction), slog logging, `db.Store`+pool+`WithTx`, Redis wrapper, in-code migration runner (GET_LOCK + checksum + DDL/DML rules) with `schema_migrations` + `001_app_meta`, scaffold packages, tests, this document. No trading logic. |
| PR2 | `pr2-database-schema` | planned | Full trading schema & migrations. |
| PR3 | `pr3-state-machine` | planned | Cycle/order models, state machine, validation. |
| PR4 | `pr4-exchange-abstraction` | planned | Normalized exchange layer (ported from iranArb). |
| PR5 | `pr5-redis-collector` | planned | Redis key schema + collector. |
| PR6 | `pr6-config-system` | planned | DB-backed versioned config + cache. |
| PR7 | `pr7-exchange-request-queue` | planned | Queue + order-executor foundation. |
| PR12 | `pr12-startup-reconciler` | planned | Startup reconciler (closes the safety core). |
