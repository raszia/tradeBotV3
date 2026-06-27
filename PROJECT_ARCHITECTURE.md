# v3TradeBot — Project Architecture

> **This document is part of the source code.** Every PR that adds, removes, or
> changes anything important (a table, a state transition, a binary, a Redis key,
> queue/config/recovery behaviour, a safety rule, a limitation, or a deferral)
> MUST update this file in the same PR. Outdated docs are treated as a bug.

Last updated: **PR6 — DB-backed versioned config system and cache.**

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

Infrastructure (**PR1**): `schema_migrations` (runner), `app_meta` (migration `001`).

Implemented (**PR2**, migrations `002`–`007`):

- **Reference / discovery (002):** `exchanges`, `assets`, `markets`,
  `exchange_markets` (per-venue listing: exchange-native symbol, canonical
  mapping, trading rules, venue status, raw metadata, `last_discovery_at`, and the
  four per-symbol enable flags).
- **Credentials (003):** `exchange_credentials` (encrypted-only secret columns +
  `encryption_algorithm`/`key_version`; no plaintext columns),
  `exchange_credential_audit` (never stores secrets).
- **Config (004):** `config_versions`, `config_change_audit`, `symbol_configs`
  (trading params), `exchange_configs` (per-exchange concurrency + timeouts),
  `exchange_fees`, `retention_settings`.
- **Trading core (005):** `cycles` (+ config_version + regime snapshot),
  `cycle_state_events`, `orders` (+ `local_client_order_id`,
  `client_order_id_sent`, `version`), `order_events`, `fills`, `symbol_locks`
  (composite-scope, generated `active_key`), `exchange_requests` (queue),
  `cycle_fee_snapshots`.
- **Observability (006):** `wallet_balances_current`, `wallet_balance_history`,
  `exchange_health_current`, `exchange_health_samples`, `api_call_logs`,
  `app_logs`, `comparison_events`, `signals`.
- **Discovery runs (007):** `market_discovery_runs`.
- **PR4 (008):** adds `api_call_logs.exchange_code` (+ index) so the raw-API IO
  logger can attribute logs by exchange code.

Planned (**PR15**): `market_regime_baskets`, `market_regime_basket_symbols`,
`market_regime_timeframes`, `market_regime_current`, `market_regime_history`.

Records kept permanently (unless explicitly configured otherwise): orders, fills,
cycles, signals. High-volume tables (`api_call_logs`, `comparison_events`,
`exchange_health_samples`, `app_logs`, `wallet_balance_history`, and later
`market_regime_history`) are subject to retention — they are **non-partitioned
with a strong `created_at` index and carry NO foreign keys** so inserts stay cheap
(async/batched) and pruning is a simple batched `DELETE` (see §9 decision).

**Mapping note:** `exchange_markets.market_id` is the authoritative link to the
canonical `markets` row; `canonical_symbol` is denormalized and must equal
`markets.canonical_symbol` when `market_id` is set (both stored, relationship
unambiguous).

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

## 7. Redis key structure (implemented in PR5)

Redis holds **live market data only** and is **never** the source of truth.

**Keys** (`internal/redis/market.go`):

- `orderbook:{exchange_code}:{canonical_symbol}` → JSON `events.BookSnapshot`
  (the full normalized book + timestamps).
- `price:{exchange_code}:{canonical_symbol}` → JSON `events.PriceSnapshot`
  (top-of-book: best bid/ask + qty + timestamps).
- Both keys carry a TTL (default 5m) so stale data expires if a collector dies.

Canonical symbols (e.g. `BTC/USDT`) contain `/`. Redis keys are binary-safe, so
`/` is kept verbatim — the format stays human-readable and consistent. The
`{exchange_code}` is the normalized exchange code (`binance`, `nobitex`, …).

**Pub/sub:** a single shared channel **`market_events`** carries JSON
`events.MarketEvent` (`type`, `exchange`, `symbol`, `best_bid`/`best_ask`, +
timestamps). Subscribers (trade-engine, regime) read one channel and filter by
exchange/symbol.

**Timestamps (rule #4)** on every payload/event so later PRs can detect
staleness: `exchange_time` (when the data is from), `received_at` (collector
received), `stored_at`/`published_at` (written to / published on Redis), plus
`exchange` and canonical `symbol`. `BookSnapshot.Stale(maxAge, now)` is provided.

**Restart safety:** if Redis is flushed/restarted, the system loses at most fresh
market data — collectors reconnect and repopulate. It never loses cycles, orders,
fills, locks, balances, or queue state (all in MariaDB). The collector itself is
stateless: on restart it reloads its collection set from the DB and resumes.

## 7a. Collector (implemented in PR5)

The `collector` binary (`internal/collector`) is the read-only market-data plane:

- Uses **only** `exchanges.PublicClient` (no credentials, no private calls). It
  makes **no** trading decisions, creates **no** cycles, writes **no** orders.
- Loads its collection set from the DB (`exchange_markets.enabled_for_collection`
  on enabled exchanges) — the dashboard/discovery toggles this later.
- For each exchange: a WebSocket order-book subscriber when the venue supports it
  (Binance), otherwise a REST poll loop (the Iranian venues — WS deferred, see
  §12). Each observed book is normalized, written to the `orderbook:`/`price:`
  keys, and published on `market_events`; per-exchange health is recorded in
  `exchange_health_current`/`exchange_health_samples`.
- Raw API calls are logged through the secret-masking IO logger (§12).
- Decoupled via interfaces (`MarketStore`, `HealthRecorder`) so it is unit-tested
  with fakes and never needs a live exchange/Redis/DB in unit tests.

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

## 8a. Per-symbol enable/disable (schema in PR2; behaviour enforced in later PRs)

Each `exchange_markets` row carries four independent flags so every exchange
market is individually controllable from the dashboard:

- `enabled_for_collection` — collect this market's book/price into Redis.
- `enabled_for_signal` — evaluate signals on it.
- `enabled_for_trading` — allowed to **start new buy cycles**.
- `enabled_for_sell_manage` — manage existing sell/reprice/cancel (defaults TRUE).

**Disabling means "do not start new trades", NOT "forget existing open orders".**
When `enabled_for_trading` is turned off for a market that already has an open
cycle, the system must: stop accepting new signals / not start new buy cycles for
it, **but continue** managing the existing open cycle/order (sell, reprice,
cancel, reconcile) to a safe resolution, keep the symbol lock correct, and
**release the lock only when safe**. `enabled_for_sell_manage` should normally
stay TRUE until all open cycles are resolved. Disabling `enabled_for_collection`
can make safe management impossible unless another market-data source still feeds
the prices that sell/reprice decisions need — so it must be used with care while a
cycle is open. PR2 provides only the flags; this behaviour is enforced in the
engine (PR8/PR9), executor (PR10/PR11), and reconciler (PR12).

## 9. State-machine design (implemented in PR3 — `internal/state`)

Cycle and order states change **only** through the validated transitions in
`internal/state`; no other package mutates a `state` column with ad-hoc SQL. This
is what keeps the lifecycle deterministic and auditable.

- **CycleState:** `NEW`, `SIGNAL_DETECTED`, `BUY_REQUEST_QUEUED`, `BUY_SUBMITTED`,
  `BUY_PARTIALLY_FILLED`, `BUY_FILLED`, `SELL_REQUEST_QUEUED`, `SELL_SUBMITTED`,
  `SELL_REPRICE_PENDING`, `SELL_PARTIALLY_FILLED`, `SELL_FILLED`, `CANCEL_PENDING`,
  `CANCELLED`, `FAILED`, `NEEDS_RECONCILE`, `CLOSED`.
- **OrderState:** `NEW`, `REGISTERED`, `QUEUED`, `SUBMITTED`, `ACKED`,
  `PARTIALLY_FILLED`, `FILLED`, `CANCEL_PENDING`, `CANCELLED`, `REJECTED`,
  `EXPIRED`, `FAILED`, `NEEDS_RECONCILE`.
- **RequestStatus** constants (`QUEUED`/`CLAIMED`/`IN_FLIGHT`/`SUCCEEDED`/`FAILED`/
  `RETRY_SCHEDULED`/`DEAD`) are defined here too, so the queue (PR7), dashboard and
  tests share one vocabulary matching the DB enum.

Rules enforced:

- **Authoritative transition maps** (`cycleTransitions`, `orderTransitions`) with
  **no self-loops** and **no exits from terminal states**. `ValidateCycleTransition`
  / `ValidateOrderTransition` are pure legality checks.
- **Terminal states:** cycle = `CLOSED`/`CANCELLED`/`FAILED`; order =
  `FILLED`/`CANCELLED`/`REJECTED`/`EXPIRED`/`FAILED`. They have no outgoing normal
  transitions. `NEEDS_RECONCILE` is **not** terminal — it is a holding state.
- **`NEEDS_RECONCILE`** is reachable from any non-terminal state but has **no
  automatic exit** — there is deliberately no normal transition out of it. The
  only sanctioned exit is a future explicit operator/reconciler resolution path
  (PR12); it is not implemented in the normal transition functions.
- **Optimistic concurrency + atomic event:** `ApplyCycleTransition` /
  `ApplyOrderTransition` take a caller-supplied `*sql.Tx` and, in that one
  transaction, run `UPDATE … SET state=?, version=version+1 WHERE id=? AND state=?
  AND version=?` and insert the `*_state_events` row (`from_state`, `to_state`,
  new `version`, `event_type`, `message`, `payload_json`). Same tx ⇒ both commit
  or both roll back. `UNIQUE(parent_id, version)` additionally blocks duplicate
  events.
- **Zero-row disambiguation:** when the guarded update affects 0 rows the row is
  re-read and the outcome is classified as **replay** (already in target →
  idempotent no-op, `Result.Replayed=true`, no second event), **stale version**
  (`ErrStaleVersion`), **state mismatch** (`ErrStateMismatch`), or **missing row**
  (`ErrUnknownRow`). This is how crash/duplicate-event replays stay safe.

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

## 12. Exchange abstraction layer (implemented in PR4)

The required interface surface is split into two interfaces so the collector/
regime modules never depend on private credentials (rule #8):

- **`exchanges.PublicClient`** (no credentials): `GetMarkets`, `GetOrderBook`,
  `SubscribeOrderBook`.
- **`exchanges.PrivateClient`** (credentials): `GetBalances`, `PlaceOrder`,
  `CancelOrder`, `GetOrder`, `GetOpenOrders`, `SubscribeOrderUpdates`.

Normalized models: `domain.{OrderBook,Level,Balance,SymbolRules}`,
`exchanges.NormalizedMarket`, `exchanges.NormalizedAPIError` (+ `ErrorCategory`,
wraps `execution.Err*` sentinels), and `execution.{OrderRequest,OrderAck,
OrderStatus,Fill,NormalizedOrderState,NormalizedOrderEvent}`. Both WebSocket and
polling results convert into the **same** `NormalizedOrderEvent`
(`EventFromStatus`/`EventFromAck`), so PR10's processor has one code path.

- **Order type/time-in-force are request fields** set by the owner's logic — the
  layer imposes no IOC/market/post-only.
- **Credentials** come from a `CredentialProvider` (real impl reads encrypted DB
  rows later; PR4 uses `StaticCredentialProvider` in tests). Never from env.
- **Secret masking** is centralized: `BuildHTTPClient` wraps the transport with a
  logging round-tripper (`internal/exchanges/mask.go` + `iolog.go`) that masks
  API keys/secrets/passphrases/Authorization/signatures/tokens/cookies in
  headers, JSON/form bodies, and signed URL query params **before** writing
  `api_call_logs`. Adapters never log themselves. (PR4 added migration `008`
  adding `api_call_logs.exchange_code`.)
- **Factory**: each adapter self-registers in `init()` via `Register(...)` with
  its capability matrix + public/private constructors. `NewPublicClient` /
  `NewPrivateClient` build by code; unknown ops return a typed `ErrUnsupported`.

**Exchanges ported (copy & adapt from `iranArb`):** Binance (public-only — it is
the price REFERENCE; the strategy never trades on it), Nobitex, Wallex, Bitpin
(public + private), Ramzinex, Tabdeal, Exir (public-only price sources).

**Capability matrix** (✓ supported, — not / unsupported-for-now):

| code | Markets | Book REST | Book WS | Balances | Place/Cancel/GetOrder/Open | OrderUpd WS | Status poll | ClientOrderID |
|---|---|---|---|---|---|---|---|---|
| binance | ✓ | ✓ | ✓ | — | — (read-only) | — | — | — |
| nobitex | ✓ | ✓ | — | ✓ | ✓ | — | ✓ | ✓ |
| wallex | ✓ | ✓ | — | ✓ | ✓ (keyed by client_id) | — | ✓ | ✓ |
| bitpin | ✓ | ✓ | — | ✓ | ✓ | — | ✓ | ✓ (identifier) |
| ramzinex | ✓ | ✓ | — | — | — | — | — | — |
| tabdeal | — | ✓ | — | — | — | — | — | — |
| exir | — | ✓ | — | — | — | — | — | — |

**Per-exchange limitations (also in each adapter's file header):**
- **Binance** read-only here; WS delivers `depth20@100ms` partial-book snapshots.
- **Nobitex** quotes in RIAL (×0.1 → IRT); Token auth (no signing); `clientOrderId`
  ≤32 chars; multipart-form placement, JSON reads; 200-with-`status:failed`
  business errors; private/book WS (Centrifuge) deferred → polling.
- **Wallex** orders are **keyed by `client_id`, not an exchange order id** (cancel/
  get take the client id); TMN→IRT; `x-api-key` auth; WS deferred → polling.
- **Bitpin** JWT access/refresh token flow (cached ~14m); rate-limit sensitive
  (429 back-off); underscore symbols (`BTC_IRT`); `identifier` = client order id;
  WS deferred → polling.
- **Ramzinex** order book keyed by numeric pair-id (resolved from the pairs
  endpoint); RIAL (×0.1 → IRT); Centrifuge WS deferred.
- **Tabdeal / Exir** public-only price sources, TOMAN/IRT, no markets-list
  endpoint (`GetMarkets` → `ErrUnsupported`); WS deferred.

**Decision — WebSocket subscriptions deferred:** PR4 ports the REST surfaces
faithfully and honestly reports `OrderBookWS`/`OrderUpdatesWS` = false for the
Iranian venues (returning `ErrUnsupported`), with polling as the supported path.
The reconciler/poll backstop already make polling the safety baseline; WS is an
additive enhancement for a later PR. Binance order-book WS is implemented.

## 13. Config & versioning design (read path + versioning implemented in PR6)

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

**`internal/configstore` (PR6)** implements the DB-backed read path + versioning:

- **Snapshot** — an immutable point-in-time view: per-market config
  (`MarketConfig` = the four `enabled_for_*` flags from `exchange_markets` LEFT
  JOIN the trading params from `symbol_configs`), per-exchange config
  (concurrency/timeouts), fees, retention settings, and the active
  `config_version`. `Store.LoadSnapshot` builds it; a missing active version is
  tolerated (`Version = 0`, the "unconfigured" state).
- **Cache** — a copy-on-write `atomic.Pointer[Snapshot]`. Readers (the trade-engine
  hot path, later) call `cache.Snapshot()` with **no lock and no DB query**
  (rule #4). `cache.Run` reloads off the trading path on an interval; a reload
  **failure retains the previous good snapshot** (a DB blip never wipes config).
- **Version stamping** (rule #5) — `Snapshot.ConfigVersion()` gives the value a
  cycle stamps at signal time (PR9); `MarketConfig.SymbolConfigVersion` and
  `ExchangeConfig.ConfigVersion` are available per entity.
- **Versioned + audited writes** (rule #3) — `ActivateVersion` supersedes the
  prior active `config_versions` row and inserts a new active one;
  `UpdateMinSpreadBps` is the representative write that, in ONE transaction,
  activates a new version, updates the row, and writes a `config_change_audit`
  row (entity/field/old/new/changed_by/reason/version/time). **Audit rows never
  contain secrets** (credential changes use `exchange_credential_audit`).
- **Validation** (rule #6) — `ValidateMarket`/`ValidateExchange`/`ValidateSnapshot`
  check value sanity (non-negative spreads/intervals/retries, positive timeouts/
  concurrency, positive `buy_size` when trading is enabled, valid `buy_size_unit`)
  and the **enable-flag hierarchy** `trading ⊆ signal ⊆ collection` (so a symbol
  can't be half-enabled by incomplete config). Referential integrity is enforced
  by schema FKs.

The dashboard EDIT forms (creating versions/audit via this layer) are PR17; the
trade-engine consuming the cache is PR8. Each cycle stores the `config_version`
(and regime config version) used when its signal was created.

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
- **Partitioning decision (PR2, MariaDB):** a partitioned InnoDB table requires
  every unique/primary key to include the partition column, which complicates the
  `AUTO_INCREMENT` PKs here. **PR2 therefore creates all high-volume tables
  NON-PARTITIONED** with a strong `created_at` index (and no foreign keys), so
  retention is a simple batched `DELETE ... WHERE created_at < ? LIMIT N`.
  Partitioning may be introduced later if volume demands it; because the
  `created_at` index already exists, that change is additive, not breaking.

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

- **Through PR6 the system collects market data and can load/version config, but
  does not trade.** The collector caches books/prices; the config cache serves
  snapshots — but nothing yet consumes events or config to evaluate signals,
  place orders, or reconcile.
- **PR6 is the config read path + versioning primitives.** The cache is not yet
  wired into any binary (the trade-engine consumes it in PR8). The only
  versioned-write helper is the representative `UpdateMinSpreadBps`; the full set
  of dashboard edit forms is PR17. Credential encryption is still schema-only
  (PR2); no plaintext is exposed anywhere.
- **Collector data source:** until market-discovery (later PR) populates
  `exchange_markets` and the dashboard enables symbols, the collector has zero
  targets and idles. It reads `enabled_for_collection`; there is no discovery
  execution yet (PR-deferred).
- The Iranian adapters poll (WS deferred, §12), so collector latency for those
  venues is the poll interval; Binance uses WS.
- **PR4 adapters: WebSocket subscriptions are deferred** for all Iranian venues
  (`SubscribeOrderBook`/`SubscribeOrderUpdates` return `ErrUnsupported`; polling is
  the supported path). Binance order-book WS is implemented. Adapter REST surfaces
  are ported faithfully from iranArb but are tested only with httptest fakes
  (rule #3 forbids live calls); they should be validated against each real venue
  in the dry-run / limited-live phases (PR19/PR20) before trading.
- **PR3 state machine** exists and is tested but is not yet called by any service.
  Cycle/order *creation* is PR9; fill processing is PR10; the operator/reconciler
  exit from `NEEDS_RECONCILE` is PR12.
- Credential **encryption is not implemented** (PR2 added only the at-rest
  schema); **market-discovery execution** is not implemented (only its storage);
  the per-symbol enable/disable **behaviour** (§8a) is documented but enforced in
  PR8–PR12.
- The exchange-request queue's multi-process per-exchange concurrency protection
  and the conservative crash-after-send recovery are **designed** (§8, §17) but
  implemented in PR7+. Until then, assume one executor per exchange.
- High-volume tables are **non-partitioned** for now (§16 decision); partitioning
  is a later additive change if volume requires it.
- The migration statement splitter does not support stored programs/`DELIMITER`.

## 19. Pending work (delivery plan)

First delivery is the **safety core: PR1–PR7 + PR12**, built and then stopped for
review before anything can trade. Subsequent phases (one reviewed PR at a time):
PR8 (engine signal loop), PR9 (cycle/lock/buy enqueue), PR10 (order-status/fill
processing), PR11 (sell + repricing), PR13 (balance sync), PR14 (health),
PR15 (regime), PR16 (dashboard read views), PR17 (dashboard config editing),
PR18 (retention), PR19 (dry-run), PR20 (limited live).

## 19a. Decisions log

- **PR6 — config cache is copy-on-write** (`atomic.Pointer[Snapshot]`): readers
  never lock or hit the DB; reloads swap a new immutable snapshot; a failed reload
  keeps the last good one.
- **PR6 — `MarketConfig` merges `exchange_markets` (enable flags) + `symbol_configs`
  (trading params)** via LEFT JOIN, so a market with no symbol_config still shows
  its flags (`HasSymbolConfig=false`).
- **PR6 — enable-flag hierarchy `trading ⊆ signal ⊆ collection`** is enforced by
  validation to prevent accidental half-enablement.
- **PR6 — every config write is one transaction: activate version → update row →
  audit** (`UpdateMinSpreadBps` is the reference). Audit never stores secrets.
- **PR6 — no active config is a valid state** (`Version=0`); the system runs
  unconfigured rather than erroring.

- **PR5 — Redis key format keeps `/`.** Canonical symbols (`BTC/USDT`) are used
  verbatim in keys (`orderbook:nobitex:BTC/IRT`); Redis keys are binary-safe so no
  escaping is needed, and it stays readable/consistent.
- **PR5 — single shared `market_events` channel** (JSON `MarketEvent`) rather than
  per-symbol channels, so a consumer subscribes once and filters.
- **PR5 — collector reads its collection set from the DB** (`enabled_for_collection`),
  not from a config file; Redis stays a pure cache with a TTL.
- **PR5 — collector decoupled via `MarketStore`/`HealthRecorder` interfaces** so
  unit tests use fakes (no live exchange/Redis/DB); a gated test covers the real
  Redis store.

- **PR4 — exchange abstraction is a redesign, not a verbatim copy.** iranArb's
  `PlaceIOC` (forced IOC) + legacy `*IRT` field names were replaced with a generic
  `PlaceOrder` (owner-set type/TIF) and quote-native normalized models. Public and
  private interfaces are split (collector never sees credentials).
- **PR4 — Binance is public-only** (price reference; the strategy trades on the
  Iranian venues, never on Binance).
- **PR4 — WebSocket subscriptions deferred** for Iranian venues; REST + polling is
  the supported path now, honestly reported via capability flags. WS is additive
  later. (Binance book WS implemented.)
- **PR4 — secret masking centralized** in the logging round-tripper; all raw API
  logs pass through it. Added migration `008` (`api_call_logs.exchange_code`).
- **PR4 — adapters tested with httptest only** (no live exchange calls, rule #3);
  real-venue validation happens in PR19/PR20.

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
- **PR2 — high-volume tables non-partitioned + no FKs.** See §16. Keeps inserts
  cheap and retention simple; `created_at` index makes future partitioning
  additive.
- **PR2 — credential secrets stored as structured AES-256-GCM payloads.** Each
  `encrypted_*` column holds `nonce ‖ ciphertext ‖ tag` (no separate nonce
  column); `encryption_algorithm` + `key_version` allow rotation. No plaintext
  secret columns exist. (Encryption code is a later PR; PR2 is schema only.)
- **PR2 — `exchange_markets` (what a venue supports) is kept separate from
  `symbol_configs` (how we trade).** Trading parameters never live on
  `exchange_markets`.
- **PR2 — reserved word avoided.** The app-log producer column is `source_binary`
  (`binary` is reserved in MariaDB).

## 20. PR history / implementation phases

| PR | Branch | Status | Summary |
|---|---|---|---|
| PR1 | `pr1-project-skeleton` | **accepted** | Project skeleton & shared foundation: module layout, all 9 binaries bootable, **file-only bootstrap config** (no env; `-config` flag; secret redaction), slog logging, `db.Store`+pool+`WithTx`, Redis wrapper, in-code migration runner (GET_LOCK + checksum + DDL/DML rules) with `schema_migrations` + `001_app_meta`, scaffold packages, tests, this document. No trading logic. |
| PR2 | `pr2-database-schema` | **accepted** | Full trading schema (migrations `002`–`007`, 29 tables): reference/discovery, encrypted credentials + audit, versioned config + audit, trading core (cycles/orders/fills/events, composite-scope symbol_locks, exchange_requests queue), observability (balances/health/logs/comparison/signals), market_discovery_runs. Offline SQL unit tests + gated MariaDB integration tests (tables/indexes/FKs/uniques/enum/no-plaintext-creds/active-lock uniqueness). Schema only — no behaviour. |
| PR3 | `pr3-state-machine` | **accepted** | `internal/state`: CycleState/OrderState/RequestStatus enums, authoritative transition maps (no self-loops, no terminal exits, NEEDS_RECONCILE entry-only), `Validate*Transition`, `Apply{Cycle,Order}Transition` (tx + version-guarded CAS + atomic event insert + replay/stale/mismatch/missing disambiguation). Minimal `internal/models` (Cycle/Order/StateEvent). Table-driven transition tests + sqlmock Apply tests + real-MariaDB integration test. No trading behaviour; functions not yet wired into services. |
| PR4 | `pr4-exchange-abstraction` | **accepted** | Exchange abstraction layer (copy & adapt from iranArb): normalized `domain`/`execution` models, split `exchanges.PublicClient`/`PrivateClient` interfaces, `Capabilities`, `CredentialProvider`, `NormalizedAPIError`, factory registry, centralized secret-masking IO logger (+ migration `008`), tuned HTTP client. Adapters: Binance (public), Nobitex/Wallex/Bitpin (public+private), Ramzinex/Tabdeal/Exir (public). WS deferred for Iranian venues (capability flags honest). Fake private client for tests/dry-run. 77 exchange test funcs (httptest only, no live calls) + masking proof. No trading behaviour; adapters not wired into services. |
| PR5 | `pr5-redis-collector` | **accepted** | Redis market-data layer + collector. `internal/events` (BookSnapshot/PriceSnapshot/MarketEvent with timestamps), `internal/redis` market store (orderbook:/price: keys + TTL, `market_events` pub/sub, ErrNotFound), `internal/collector` (Collector using only PublicClient; WS-or-poll; DB-driven targets; DB health recorder; `MarketStore`/`HealthRecorder` interfaces), `FakePublicClient`, cmd/collector wired. Tests: events, collector (fakes: poll/WS/health/shutdown/public-only), sqlmock targets+health, gated real-Redis round-trip. Redis stays cache-only; no trading/order/cycle code. |
| PR6 | `pr6-config-system` | **in review** | `internal/configstore`: DB-backed versioned trading config. `Snapshot` (MarketConfig merging exchange_markets flags + symbol_configs params, ExchangeConfig, fees, retention, active version), `Store.LoadSnapshot`/`ActiveVersion`, copy-on-write `Cache` + background `Run` reloader (non-blocking; keeps good config on reload failure), `ActivateVersion` + audited `UpdateMinSpreadBps` (version+audit in one tx, no secrets), validation (value sanity + enable-flag hierarchy), version-stamping helpers. Tests: sqlmock loaders/version/audit, cache COW/reload/concurrent-read, validation, gated MariaDB full-path. File-only bootstrap unchanged; no env config; not yet wired into a binary. |
| PR6 | `pr6-config-system` | planned | DB-backed versioned config + cache. |
| PR7 | `pr7-exchange-request-queue` | planned | Queue + order-executor foundation. |
| PR12 | `pr12-startup-reconciler` | planned | Startup reconciler (closes the safety core). |
