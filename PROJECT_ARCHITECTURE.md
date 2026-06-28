# v3TradeBot — Project Architecture

> **This document is part of the source code.** Every PR that adds, removes, or
> changes anything important (a table, a state transition, a binary, a Redis key,
> queue/config/recovery behaviour, a safety rule, a limitation, or a deferral)
> MUST update this file in the same PR. Outdated docs are treated as a bug.

Last updated: **PR14 — Exchange health monitor.**

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

## 2a. Owner-defined buy/queue flow rules (documented now; implemented PR8–PR17)

These owner-defined rules are authoritative. They are documented here so every PR
(including PR12) is built to be consistent with them; their full implementation is
spread across later PRs (see "implementation placement" at the end).

**One active pending buy-intent per (exchange-market / symbol / strategy scope).**
Signal spam must not create competing buy requests. When a new signal arrives for
a symbol that already has a pending buy:

- if a **QUEUED** request already exists for that scope → **update** it to the
  newest valid signal (do not enqueue a duplicate);
- if the new signal **invalidates** the previous one → **remove/cancel the QUEUED
  request before it is sent**;
- if the existing request is already **CLAIMED / IN_FLIGHT** → do **not** mutate it
  blindly;
- if it was already **sent / status unknown** → use reconciliation/safe handling,
  do not replace it.

(Enforced in PR8/PR9 when the engine creates/updates queued buy requests; PR9
cycle/order/request creation respects the one-active-intent rule.)

**How PR9 enforces "one active intent" — the symbol lock is the gate.** From PR9 on,
a buy intent is a **cycle** that owns an **ACTIVE `symbol_locks` row** (`UNIQUE`
`active_key = scope|canonical_symbol`). The lock is acquired in the **same
transaction** that creates the cycle/order/request, so a second concurrent signal
for the same scope fails on the unique key → `ErrSymbolLocked` → the whole
transaction rolls back (no orphan cycle) → no competing cycle. While the cycle's
buy request is still **QUEUED (unsent)**, a newer valid signal **updates that
request's payload** (price/mode) rather than enqueuing a second one.

**Audit rule — do not physically delete a cycle-tied request.** A standalone
*pre-cycle* QUEUED intent may be deleted, but once a request is tied to a
cycle/order (which, from PR9, is always — `orders.cycle_id` is `NOT NULL`) it must
**not** be physically deleted; it is marked obsolete/`DEAD` or carried with an event
trail so audit/recovery stay intact. Consequently **PR9 never deletes** a buy
request: when the latest signal *invalidates* a started cycle's still-QUEUED buy,
PR9 leaves it untouched and the **lifecycle logic (PR10/PR11)** cancels/abandons it
through the state machine. PR9 only **creates** (no active lock) or **refreshes the
QUEUED payload** (active lock, request still unsent); `CLAIMED`/`IN_FLIGHT`/sent
requests are never mutated by the engine.

**Maker-first / taker-fallback buy policy (owner-defined; DB-configurable per
exchange-market/symbol).** The buy intent supports a configurable maker-first,
taker-fallback escalation to reduce fees when possible while still capturing a
persistent edge:

- **First attempt → maker-style.** The first valid signal for a scope prepares a
  maker-style limit buy **below** the Iranian ask by a configured offset
  (`maker_price_offset_bps`): `limit = ask × (1 − maker_price_offset_bps/10000)`.
- **Repeated opportunity → taker fallback.** Each valid signal for the scope is an
  *opportunity* and **advances one shared attempt counter within a rolling window**
  (`maker_signal_window_seconds`), whether the signal **creates** a new cycle or
  **refreshes** the existing still-QUEUED (unsent) one. The attempt number is
  `1 + (the most recent in-window cycle's attempt)`; while `attempt ≤
  maker_attempts_before_taker` the mode is `MAKER_FIRST` (attempt 1) / `MAKER_RETRY`
  (attempt > 1); once exhausted the mode is `TAKER_FALLBACK` and the limit is the
  **ask** (`taker_price_mode='ASK'`), capped by `max_taker_slippage_bps` against the
  signal price. When the window elapses the counter **resets to maker-first**. If
  `maker_first_enabled=false`, every attempt is taker.
- **One shared counter across create AND refresh (load-bearing).** Because the
  symbol lock means only one cycle is active per scope at a time, repeated signals
  hit the still-QUEUED cycle and **refresh it in place** — and that refresh
  **re-runs the maker/taker decision**, so the SAME QUEUED request can escalate
  `MAKER_FIRST → MAKER_RETRY → TAKER_FALLBACK` (updating its mode, price, attempt,
  and payload) **without ever creating a duplicate request**. A new cycle created
  after a prior attempt released the lock continues the same counter from the most
  recent in-window cycle. Either path escalates identically; neither double-counts
  (a refresh advances the active cycle's own counter; a new cycle advances from the
  prior cycle's counter). `CLAIMED`/`IN_FLIGHT`/sent requests are never refreshed.
- **Config parameters** (on `symbol_configs`, versioned): `maker_first_enabled`,
  `maker_attempts_before_taker`, `maker_signal_window_seconds`,
  `maker_wait_before_cancel_ms`, `maker_price_offset_bps`, `taker_price_mode`,
  `max_taker_slippage_bps`.
- **Both modes still execute via the simulated-IOC flow** (below): maker = place
  below ask → wait `maker_wait_before_cancel_ms` → cancel remainder → confirm fill;
  taker = place at/near ask (slippage-capped) → confirm fill. Execution and fill
  accounting are PR10/PR11.
- **Persisted for audit/dashboard** (PR9 stamps what it knows; PR10/PR11 fill the
  rest): `orders.intended_execution_mode` / `actual_execution_mode` /
  `maker_attempt_number` / `maker_offset_bps` / `ask_price_at_decision`;
  `cycles.intended_execution_mode` / `maker_attempt_number` /
  `opportunity_window_started_at`; and, in the `PLACE_ORDER` request payload, the
  full executor instruction set (mode, intended price/qty, maker wait + cancel-after,
  final-status-check flag, offset, taker price mode + slippage cap, signal price
  context, quote unit + reference rate, fee assumptions, config version,
  `local_client_order_id`, idempotency key).

**Simulated IOC buy (Iranian venues lack native IOC).** From the strategy's view
the buy is IOC, but the system SIMULATES it via the flow rather than assuming a
native capability:

1. `PLACE_ORDER` the buy,
2. wait a **DB-config-configurable** interval,
3. `CANCEL_ORDER` the remaining open amount,
4. `GET_ORDER` / status check for the final outcome,
5. record the actually filled quantity + fees,
6. continue the cycle **only** with the filled quantity.

Rules: never assume native IOC unless the exchange capability explicitly says so;
do not force native IOC in the abstraction (it stays a request field); the wait
duration is DB config; the final filled amount is confirmed from exchange
status/fills; **a missing open order is NEVER proof that nothing filled**.

- **No-fill:** mark the order not-filled/cancelled/expired via the state machine;
  do **not** proceed to sell; release the lock only when safe; a *fresh* valid
  signal may later create/update a new queued buy — do not auto-retry the same
  stale signal.
- **Partial-fill:** persist the exact filled qty, compute avg price + fees, and
  run the sell/reprice flow on the **filled** amount only; the unfilled remainder
  is not inventory.
- **Ambiguity:** if the cancel or final status check is ambiguous → mark the
  order/cycle `NEEDS_RECONCILE`.

**PR12 (reconciler) must respect this:** a `PLACE_ORDER` followed by an intended
`CANCEL_ORDER`/`GET_ORDER` may be part of this simulated-IOC flow. If the system
crashes mid-flow, the reconciler must **not resend the buy blindly**: unclear
status → `NEEDS_RECONCILE`; cancel-done-but-fill-unknown → `NEEDS_RECONCILE`;
no-open-order is not proof of zero fill; proven zero-fill may safely close/expire
the attempt; proven partial/full fill must preserve the filled amount and keep the
cycle safe for later sell management.

**Implementation placement:** pre-cycle no-duplicate-queued-signal (update/remove an
unsent intent) → PR8; one-active-pending-intent via the **symbol lock** +
maker/taker decision + buy enqueue → **PR9**; simulated-IOC execution + fill/status
recording + `actual_execution_mode` → PR10; sell/reprice on filled qty → PR11;
reconciler crash/ambiguity handling → PR12; dashboard exposes the maker/taker policy,
simulated-IOC wait interval, and per-attempt mode/fill reporting → PR16/PR17. Each
of those PRs adds the corresponding tests (duplicate-signal-updates-not-inserts,
claimed/in-flight-not-mutated, duplicate-lock-blocks-second-cycle, atomic-rollback,
maker-first-then-taker-after-N-attempts, window-reset, zero-fill-abandons-as-CANCELLED,
partial-fill-continues-with-filled-amount, ambiguous→NEEDS_RECONCILE,
no-open-order-not-proof-of-zero-fill).

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
  reconciler/  startup + periodic reconciliation (read-only; never sends)  [PR12]
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

## 7b. Trade-engine signal loop (implemented in PR8 — `internal/engine`)

The `trade-engine` binary consumes `market_events`, reads the latest books/prices
from Redis and trading config from the configstore cache, computes the spread, and
writes `comparison_events` / `signals`. **Hard boundaries (PR8):** it NEVER calls
an exchange (no private clients, no `PlaceOrder`/`CancelOrder` — a reflection guard
asserts no engine field can place/cancel), and it does **not** create cycles or
orders (that is PR9). Its only writes are the observability rows and, defensively,
the update/removal of an existing not-yet-claimed QUEUED buy intent (§2a below).

**Event fan-out (`targets`).** On a reference-venue (Binance) tick, every Iranian
`enabled_for_signal` market on the same base asset is re-evaluated (its reference
moved); on an Iranian tick, only that market is. An unconfigured system (active
config **version 0**) produces no targets at all.

**Spread formula (owner-confirmed).** The strategy buys on the Iranian exchange at
its **best ask** when cheaper than the Binance **best bid**:

```
spread_bps        = (binanceRefBid_in_quote − iranianAsk) / iranianAsk × 10000
fee_adjusted_bps  = spread_bps − buyFeeBps − sellFeeBps      (buy=taker, sell=maker)
passed            = fee_adjusted_bps ≥ symbol_config.min_spread_bps
```

**Quote units (never mixed silently).** USDT-quoted Iranian markets compare
directly with Binance `BASE/USDT`. IRT/IRR-quoted markets convert the Binance USDT
bid into rial using the **same Iranian exchange's** `USDT/IRT` rate from Redis
(`price:{exchange}:USDT/IRT`, best bid); if that rate is missing or stale there is
**no signal**. `comparison_events`/`signals` store `binance_price` already
converted into the Iranian quote, plus `quote_unit` and the `reference_rate` used
(NULL for USDT), so every comparison is auditable (migration 009 added those two
columns).

**Stale / missing data → no signal (fail-safe).** A comparison is written only when
both sides are present and **fresh** (`exchange_time` within `MaxBookAge`, default
10s) and the prices are positive; otherwise nothing is written. Disabled-for-signal
markets and markets without a `symbol_config` are skipped. Every comparison and
signal row is stamped with the active `config_version`.

**What PR8 writes:** `comparison_events` (every computable comparison, pass or
fail) and `signals` (only when `passed`). It does **not** write `cycles`, `orders`,
or new `exchange_requests`.

**No-duplicate pending buy intent (§2a, scope = `exchange_market_id`).** PR8 does
not create buy requests (PR9 does, transactionally under the symbol lock). It only
keeps the one pending intent fresh: on a **passing** signal for a trading-enabled
market it UPDATES the payload of an existing **QUEUED** entry-buy `PLACE_ORDER`
request (so signal spam supersedes rather than duplicates); on an **invalidated**
signal it DELETEs the not-yet-sent QUEUED request. Both helpers
(`UpdatePendingBuyRequest`/`RemovePendingBuyRequest`) `SELECT … FOR UPDATE` the row
and guard on `status='QUEUED'`, so a `CLAIMED`/`IN_FLIGHT`/already-sent request is
**never** touched (left to the executor/reconciler), even against a concurrent
claimer. Repeated identical events therefore never create competing requests.

## 8. Exchange-request queue + order-executor (implemented in PR7)

A database-backed **priority queue** (`internal/queue`) decouples decision-making
(trade-engine, which ENQUEUES) from API calls (order-executor in
`internal/executor`, which CLAIMS and sends). Queue state is authoritative
recovery state, so it lives in MariaDB — **never Redis** (a Redis flush must not
lose what we were about to send / have sent).

- **`exchange_requests`** columns (PR2): exchange_id, symbol, cycle_id, order_id,
  `request_type`, priority, **status**, payload (JSON), response, timeout_ms,
  retry_count, max_retries, next_retry_at, **idempotency_key (UNIQUE)**,
  claimed_by, claimed_at, inflight_at, last_error, created_at, updated_at.
- **Status enum (fixed):** `QUEUED`, `CLAIMED`, `IN_FLIGHT`, `SUCCEEDED`,
  `FAILED`, `RETRY_SCHEDULED`, `DEAD` (reused from `internal/state.RequestStatus`).
- **`RETRY_SCHEDULED` is overloaded — disambiguated by `retry_count`** (no separate
  `SCHEDULED` enum value; the enum stays fixed). A row is `RETRY_SCHEDULED` with a
  future `next_retry_at` in two cases:
  - **`retry_count == 0` → a scheduled NEXT STEP** (`EnqueueScheduled`): a *planned*
    future request — the simulated-IOC cancel/status (PR10) or a sell reprice step
    (PR11). **Not a failure.**
  - **`retry_count > 0` → an actual RETRY** (`ScheduleRetry`): the request ran, hit a
    transient error, and is backing off.
  Dashboard/reporting MUST use `retry_count` to tell a planned step from a failed
  retry. (Verified by `queue.TestScheduledStepVsRetryConvention`.)
- **Mutating vs read-only:** `PLACE_ORDER`/`CANCEL_ORDER` are MUTATING;
  `GET_ORDER`/`GET_OPEN_ORDERS`/`GET_BALANCE` are read-only. They get different
  retry/recovery policy (below).
- **Enqueue** (`Enqueue(ctx, tx, Request)`) runs inside the caller's transaction
  (atomic with the cycle/order rows that justify it). A duplicate
  `idempotency_key` is rejected by the UNIQUE index and surfaced as
  `ErrDuplicateIdempotencyKey` (rule #8).
- **Claim algorithm** (cross-process safe — rule #3/#4): on a single pinned
  connection, take a **per-exchange advisory lock** (`GET_LOCK`), then in one
  transaction: count in-flight (`CLAIMED`+`IN_FLIGHT`), compute free slots vs the
  per-exchange limit, `SELECT … WHERE status=QUEUED OR (RETRY_SCHEDULED AND
  next_retry_at<=NOW) AND request_type IN (allowed) AND exchange enabled ORDER BY
  priority,id LIMIT slots FOR UPDATE SKIP LOCKED`, and mark them `CLAIMED`. The
  GET_LOCK makes the per-exchange limit exact even across multiple executor
  processes; SKIP LOCKED is belt-and-suspenders. (Verified by a concurrent-claimer
  test.)
- **Retry/backoff:** `ScheduleRetry` bumps `retry_count`, sets
  `RETRY_SCHEDULED` with exponential capped backoff, or → `DEAD` at max_retries.
- **Crash-after-send (rule #6):** the executor commits `MarkInFlight` BEFORE
  sending a mutating request. `SweepStuck` finds `IN_FLIGHT` rows past their
  timeout and recovers them **conservatively**: read-only → re-queued; **mutating
  → `DEAD` + the owning order pushed to `NEEDS_RECONCILE` (NEVER blindly re-sent)**.
- **Order-executor** (`internal/executor`): the ONLY component that issues
  order-mutating calls. It is driven SOLELY by the queue — **there is no exported
  method/CLI that sends an order directly** (rule #1, asserted by a reflection
  test). Outcome handling:
  - read-only success → `SUCCEEDED`; retryable error → `RETRY_SCHEDULED`;
    non-retryable → `FAILED`.
  - mutating: `MarkInFlight` → send → on **success** complete the request AND
    advance the order (`QUEUED→SUBMITTED`, stamping `exchange_order_id`) in **one
    transaction** (rule #9; if the order transition fails the whole tx rolls back
    and the request stays `IN_FLIGHT` for the sweeper); on a **definite rejection**
    (clear 4xx / insufficient balance) → `FAILED` (order unchanged); on an
    **ambiguous** outcome (timeout/network/5xx/unknown) → `DEAD` + order
    `NEEDS_RECONCILE`.
- **Live-execution guard (rule #2):** `Config.AllowLiveExecution` defaults
  **false**; with it off the executor claims only read-only request types, so a
  dev service can never place a real order by inserting a row. The PR7
  `order-executor` binary wires **no real private clients** (credential decryption
  + live gating land later), so it cannot send a real order. Richer order-state
  mapping (ACK/partial/full fill, cancel resolution) is deferred to **PR10**.

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

## 10. Symbol-lock design (acquire in PR9, release/reclaim in PR12)

A DB-backed lock prevents a symbol with an active cycle from accepting a new
signal. It survives restarts and is recoverable by the reconciler.

- **Scope is explicit and composite** — it includes the **exchange** and the
  **canonical market** (and, later, strategy), e.g. `nobitex|BTC/IRT`. So
  `BTC/IRT` on Nobitex does not block `BTC/IRT` on Wallex unless a deliberately
  global scope is configured. (PR9 uses the exchange **code** as the scope.)
- Acquiring the lock, creating the cycle, and enqueuing the first request happen
  in **one transaction** (no lock without a cycle; no cycle without its request).
- A unique-when-active constraint (generated column `active_key`) enforces one
  active lock per scope; released locks free the scope. PR9's
  `symbollock.Acquire(tx, scope, symbol, cycleID, expiresAt)` inserts the lock and
  maps a duplicate-key (1062) to **`ErrSymbolLocked`**, so the creating transaction
  rolls back cleanly when the scope is already locked.
- The reconciler reclaims a stale lock **only after** positively determining the
  owning cycle is safe — never on lease expiry alone. (Lease heartbeat renewal is a
  later PR; PR9 sets `expires_at` from a configured lease.)

## 10a. Cycle creation & buy enqueue (implemented in PR9 — `internal/buyflow`)

PR9 is the first code that **creates** trading rows. It is driven by an *accepted*
signal from the engine and, like everything before PR10, **executes nothing**: it
prepares and persists the buy intent and enqueues a `PLACE_ORDER` request for the
order-executor to claim later. It never calls an exchange (no private client) and
never sends an order.

**The critical path is one transaction** (`db.WithTx`), all-or-nothing — if any
step fails the whole thing rolls back and **no** cycle/lock/order/request exists
(source-of-truth rule #1: nothing is sent before it is committed, and an uncommitted
intent simply does not exist):

1. **Eligibility (pre-tx, fail-safe):** the market must be `enabled_for_signal` **and**
   `enabled_for_trading`, have a `symbol_config`, an active `config_version`, and a
   **fresh** Iranian ask (stale/missing market data ⇒ no cycle). Disabled or stale ⇒
   return without creating anything.
2. **Maker/taker decision (pure):** continue the shared per-scope attempt counter
   from the most recent in-window cycle (reset on window expiry), compute
   `attempt_number`, choose `MAKER_FIRST`/`MAKER_RETRY`/`TAKER_FALLBACK` and the limit
   price (see §2a). The choice itself is a pure function (`buyflow.Decide`), unit-
   tested offline.
3. **Insert the cycle** (`state='NEW'`), stamped with `config_version`, the signal
   context (`signal_time`, prices, spread, `buy_size`), the chosen
   `intended_execution_mode`, `maker_attempt_number`, and `opportunity_window_started_at`.
4. **Acquire the symbol lock** referencing the new cycle — duplicate active scope ⇒
   `ErrSymbolLocked` ⇒ rollback (no orphan cycle). **This is the one-active-intent gate.**
5. **Insert the buy order** (`side='buy'`, `role='entry_buy'`, `state='NEW'`,
   `order_type='limit'`, **`time_in_force` left NULL — native IOC is never forced**),
   with a generated **`local_client_order_id`** (`c{cycleID}-buy`, `UNIQUE`), the
   intended `limit_price`/`quantity`, and the execution-mode audit columns.
6. **Advance states through the state machine** (never ad-hoc SQL): cycle
   `NEW → SIGNAL_DETECTED → BUY_REQUEST_QUEUED`; order `NEW → REGISTERED → QUEUED`.
   Each transition writes its `*_state_events` row in the same tx.
7. **Enqueue the `PLACE_ORDER` request** (`queue.Enqueue`, status `QUEUED`) tied to
   the cycle+order, with a deterministic **idempotency key** (`place-order:c{cycleID}:buy`,
   `UNIQUE`), `timeout_ms`/`max_retries` from config, and the full intent **payload**.
8. **Commit.** The executor (PR7) later claims the QUEUED request and sends it; until
   PR10 wires real private clients with `AllowLiveExecution=true`, nothing is sent.

**Request payload shape** (everything PR10/PR11 need to execute the simulated-IOC
buy without re-deriving anything): `execution_mode`, `order_type='limit'`,
`side='buy'`, `intended_price`, `intended_quantity`, `buy_size_unit`,
`maker_attempt_number`, `maker_attempts_before_taker`, `maker_offset_bps`,
`maker_wait_before_cancel_ms`, `cancel_after_wait=true`, `final_status_check_required=true`,
`simulated_ioc=true`, `taker_price_mode`, `max_taker_slippage_bps`,
`ask_price_at_decision`, `signal_binance_price`, `signal_iranian_price`,
`quote_unit`, `reference_rate`, `buy_fee_bps`, `sell_fee_bps`, `config_version`,
`local_client_order_id`.

**No-duplicate / refresh / invalidation** (per §2a): if the scope is already locked,
PR9 does **not** create a second cycle; if that cycle's buy request is still
**QUEUED (unsent)** a newer valid signal **refreshes it in place**
(`buyflow.RefreshActiveCycleBuy`, `SELECT … FOR UPDATE` + `status='QUEUED'` guard) —
re-running the maker/taker decision so the SAME request can escalate
`MAKER_FIRST → MAKER_RETRY → TAKER_FALLBACK` (mode + price + attempt + payload all
updated) without a duplicate, never touching a `CLAIMED`/`IN_FLIGHT`/sent request,
never physically deleting a cycle-tied request. Invalidation of a started cycle's buy
is left to the PR10/PR11 lifecycle (the engine does not delete it).

**What PR9 does NOT do:** place/cancel/query any exchange order; record fills or
actual execution mode; run the simulated-IOC wait/cancel/status sequence; release
the lock (only the safe terminal flow / reconciler does). Those are PR10/PR11/PR12.

## 10b. Order/fill processing & simulated-IOC buy execution (implemented in PR10 — `internal/orders`)

PR10 is the buy-side order-processing layer: it turns order-executor *results* into
persisted order/cycle state and fills, and runs the buy as a **simulated IOC**. It
still never calls an exchange itself — the executor (PR7) does the transport and
hands the typed result to `internal/orders`, which does the database work inside the
executor's transaction. **Conservative throughout: ambiguity is always
NEEDS_RECONCILE, never a guess; a missing order is never proof of zero fill.**

**Simulated IOC as queued work (no worker sleeps).** Iranian venues lack native IOC,
so it is simulated by chaining queued/scheduled requests — a worker is never blocked
sleeping for the wait:

```
PLACE_ORDER ──(ack)──► OnPlaceAck:  order QUEUED→SUBMITTED→ACKED, cycle →BUY_SUBMITTED,
                                    schedule CANCEL_ORDER  (next_retry_at = now + maker_wait)
CANCEL_ORDER ─(ok)──► OnCancelResult: order →CANCEL_PENDING,
                                    schedule GET_ORDER     (next_retry_at = now + final_status_delay)
GET_ORDER ───────────► ProcessFinalStatus: classify → fills + transitions + lock
```

`queue.EnqueueScheduled` inserts the next step as `RETRY_SCHEDULED` with a future
`next_retry_at`, which the claimer only picks up once due. Native IOC/TIF is never
forced — `OrderRequest.TimeInForce` is left empty (the IOC behaviour is the flow).

**Classification (`orders.Classify`, pure, unit-tested).** From the final
`execution.OrderStatus` (+ any fetch error):
- **Full** — `filled`, remaining 0, qty agrees.
- **Partial** — `canceled`/`expired`/`partially_canceled` with a usable partial fill.
- **Zero** — settled with filled = 0 (the only *proven* no-exposure outcome).
- **Ambiguous** — a fetch error (incl. `ErrOrderUnknown`/missing — **never** proof of
  zero fill), a contradictory "filled" (qty short), a partial with no price, still
  open/new/unknown, or a `rejected` arriving after a successful place (contradictory).

**Outcome → state machine (always via `internal/state`; NEEDS_RECONCILE is the safe
fallback for any illegal/ambiguous case):**

| Class | order → | cycle → | symbol lock | fill row |
|---|---|---|---|---|
| Full | `FILLED` | `BUY_FILLED` | **held** (sell pending) | recorded |
| Partial | `PARTIALLY_FILLED` | `BUY_PARTIALLY_FILLED` | **held** (sell the filled qty) | recorded |
| Zero | `CANCELLED` | `CANCELLED` (reason `SIMULATED_IOC_ZERO_FILL`) | **released** | none |
| Ambiguous | `NEEDS_RECONCILE` | `NEEDS_RECONCILE` | **held** | — |

Zero-fill is a clean no-fill → **CANCELLED, never FAILED** (owner rule). A definite
**place rejection** (the order never reached the book — insufficient balance, bad
request) is handled separately by `OnPlaceRejected`: request FAILED, order + cycle
FAILED, lock released (no exposure). An **ambiguous** place/cancel (timeout, network)
→ request DEAD + order **and** cycle NEEDS_RECONCILE, lock held, never re-sent.

**Fill accounting (`orders` + the `fills` table).** The order persists
`filled_quantity`, `remaining_quantity`, `avg_fill_price`, `quote_spent`,
`fee_amount`, `fee_asset`, `actual_execution_mode`, `fill_result`, and
`last_normalized_status` (migration 011 added `remaining_quantity`/`fill_result`/
`last_normalized_status`; `actual_execution_mode` came in 010). Partial fills
**continue with the filled quantity only** — the unfilled remainder is not inventory
(PR11's sell uses `filled_quantity`, not the requested qty). One aggregate `fills`
row is written per final status with a **deterministic** `exchange_fill_id`
(`final:<exchange_order_id>`) so repeated processing is idempotent
(`UNIQUE(order_id, exchange_fill_id)`).

**actual_execution_mode** is mapped from the venue's liquidity flag
(`execution.OrderStatus.Liquidity`): `maker`→`MAKER`, `taker`→`TAKER`, otherwise
`UNKNOWN` (never guessed).

**Atomicity + idempotency.** Each result is processed in ONE executor transaction:
the queue status update, order/cycle transitions, fill upsert, and lock release all
commit together or roll back together — a request is never marked SUCCEEDED if the
state/fill work failed (it stays for the sweeper). Repeated processing of the same
final status does not duplicate fills (unique key) or state events (the state machine
replays a same-state transition as a no-op). A transient (retryable) status-fetch
error simply reschedules the read instead of finalizing.

**What PR10 does NOT do (→ PR11):** the sell side — enqueueing/managing the exit
sell on the filled quantity, repricing, and closing the cycle. Steady-state WS order
updates are also later. The operator exit from NEEDS_RECONCILE remains a later PR.

## 10c. Exit sell, management & repricing (implemented in PR11 — `internal/sellflow`)

PR11 is the exit side: once a buy is confirmed **filled or partially filled** (PR10
leaves the cycle at `BUY_FILLED`/`BUY_PARTIALLY_FILLED`, lock held, with the buy
order's `filled_quantity` recorded), the trade-engine's **sell Manager** creates and
manages a resting limit sell on the **filled quantity only**, reprices it as Binance
moves, and closes the cycle on a full exit. Like all decision-side code it NEVER
calls an exchange — it writes DB rows + queue requests; the executor sends them and
`internal/orders` processes the results.

**Driver.** The trade-engine runs a periodic `sellflow.Manager.Pass` (default every
2s) alongside the market-event loop. Each pass scans open sell cycles and, per cycle
(only when `enabled_for_sell_manage`), dispatches: create the sell, poll its status,
or reprice it. The Manager gets the Binance reference price (converted into the
Iranian quote, same as the buy signal) from the engine; a missing/stale price simply
skips price-dependent actions that pass.

**Sell quantity = actual filled inventory.** `CreateSell` sells `bought − already
sold` (the buy order's `filled_quantity` minus the sum of prior sell fills), floored
to the venue `step_size`. The originally *requested* buy quantity is never treated as
inventory.

**Sell price (owner rule, not hardcoded).** `price = floor( binanceRef ×
(1 − sell_offset_bps/10000) , tick_size )` — slightly below the Binance reference so
the resting sell fills against local buyers, floored to the venue tick. Both
`sell_offset_bps` and the venue `tick_size`/`step_size`/`min_order_*` come from DB
config (`MarketConfig`). A sell below the venue minimum (`min_order_quantity` /
`min_order_amount`) is not placed.

**Transactional create (one tx).** Insert the `exit_sell` order → cycle
`BUY_FILLED`/`BUY_PARTIALLY_FILLED`/`SELL_REPRICE_PENDING → SELL_REQUEST_QUEUED` +
order `NEW→REGISTERED→QUEUED` (state machine) → enqueue the sell `PLACE_ORDER`
(payload tagged `side:sell`) → commit. Rollback on any failure. **No-duplicate:** it
refuses (`ErrSellExists`) when an active sell order already exists for the cycle.

**Resting place + fill polling.** A sell `PLACE_ORDER` is routed by the executor
(payload `side`) to `OnSellPlaceAck`: order `→ACKED`, cycle `→SELL_SUBMITTED`, and —
unlike the buy IOC — **no auto-cancel** is scheduled (it rests). The Manager keeps at
most one outstanding `sell_status` `GET_ORDER` poll per resting sell; the executor
runs it through `ProcessSellStatus`, which records fills and drives the cycle on the
**cumulative** sold-vs-bought quantity:
- partial (resting) → order `PARTIALLY_FILLED`, cycle `SELL_PARTIALLY_FILLED` (keep
  managing the remainder — never sell more than the held inventory);
- full (cumulative sold ≥ bought) → close (below);
- ambiguous / fetch error / missing order → order+cycle `NEEDS_RECONCILE` (lock held).

**Repricing (cancel → replace, interval-gated).** When the reference moves, the
Manager reprices — but only when `reprice_interval_seconds` has elapsed
(`last_reprice_at`), and **never** while a sell place/cancel is `CLAIMED`/`IN_FLIGHT`.
`RepriceSell` cancels the resting sell (cycle `→SELL_REPRICE_PENDING`, order
`→CANCEL_PENDING`, enqueue a `sell_reprice` `CANCEL_ORDER`); `OnSellCancelResult`
schedules a final `sell_status` read (the cancel may have raced a fill, so we never
assume). That read records the cancelled order's fills, then the Manager creates the
replacement sell for the **remaining** inventory at the fresh price (or, if the cancel
raced a full fill, the cycle closes directly from `SELL_REPRICE_PENDING`). An
ambiguous cancel/place → `NEEDS_RECONCILE`, never re-sent.

**Close + PnL + lock release.** A full exit moves the cycle `→SELL_FILLED→CLOSED`,
writes the exit accounting (`sold_quantity`, `avg_sell_price`, `sell_quote`,
`sell_fee`/`sell_fee_asset`, `net_quantity`, `realized_quote`, `close_reason`,
`closed_at`; migration 012), and **releases the symbol lock** — the only point a
fully-exited cycle frees its scope. `realized_quote = sell_proceeds − buy_cost`, with
fees netted **only when denominated in the quote currency** (fees in other assets are
stored raw, not silently folded in). Everything (queue status, order/cycle
transitions, fills, lock release) commits in one transaction; repeated processing is
idempotent.

**What PR11 does NOT do (→ later):** steady-state WebSocket order updates (polling is
the supported path); per-venue individual fills (one aggregate fill row per status);
multi-leg / cross-asset fee conversion for `realized_quote`; the operator exit from
`NEEDS_RECONCILE`.

## 11. Reconciler (implemented in PR12 — `internal/reconciler`)

The reconciler makes the system safe after restart/timeout/partial-fill/
disconnect/divergence. It is **READ-ONLY toward exchanges and never auto-sends or
auto-cancels** — enforced by construction: it holds a `ReadOnlyClient` interface
that **lacks** `PlaceOrder`/`CancelOrder` (a reflection-style test plus a
panic-on-call fake confirm they are never invoked). The only writes it makes are
local state transitions (via `internal/state`), conservative lock releases, and
decision logs.

**Startup + periodic flow.** `ReconcileStartup` loads open (non-terminal) cycles
and their orders, reconciles each, and reports stuck `IN_FLIGHT` / `DEAD`
requests (it observes but never resends them — PR7's `SweepStuck` already moves
stuck mutating IN_FLIGHT to `DEAD` + order `NEEDS_RECONCILE`). `RunPeriodic` runs
the same pass on an interval; it is **idempotent** — a `NEEDS_RECONCILE` cycle is
left for the operator (no auto-exit), and the state machine's `UNIQUE(version)`
prevents duplicate events, so repeated runs neither duplicate events nor oscillate
state.

**Capability-based (rule #4).** Behaviour depends on each exchange's PR4
`Capabilities`. No client wired for an exchange → the order is **skipped**
(NoAction), not flagged. A client present but lacking the needed capability (e.g.
`GetOrder` unsupported) → `NEEDS_RECONCILE` (can't verify). Recent-fills is not
yet exposed by the v3 adapters, so that resolution path always falls through to
`NEEDS_RECONCILE`.

**Per-order decision matrix** (`decide.go`, pure/unit-tested):
- **Known `exchange_order_id` + `GetOrder` supported:** map the status —
  `FILLED`→advance FILLED; `CANCELED`(zero fill)→advance CANCELLED (only legal
  from `CANCEL_PENDING`, the cancel we requested); `REJECTED`/`EXPIRED`(zero
  fill)→advance; `OPEN`/`NEW`/`PARTIALLY_FILLED`→Continue (still working);
  `PARTIALLY_CANCELED` or any terminal-**with-a-fill**→`NEEDS_RECONCILE`
  (inventory/accounting is PR10); `UNKNOWN`→`NEEDS_RECONCILE`.
- **`GetOrder` returns "order unknown" / API error:** `NEEDS_RECONCILE` — **a
  missing/unknown order is NOT proof it never filled**.
- **Unknown `exchange_order_id` (only `local_client_order_id`):** **never
  resend**. If `ClientOrderID`+`FetchByOrderID` are supported, look it up by the
  client id and, if positively identified, **attach** the `exchange_order_id` and
  continue; otherwise → `NEEDS_RECONCILE`.

**Cycle decision.** After its orders are reconciled: any order needs-reconcile →
cycle `NEEDS_RECONCILE` (lock kept); some order still active → Continue (resume,
lock kept); all orders terminal **with any fill** → `NEEDS_RECONCILE` (exposure
pending PR10 accounting); all orders terminal **with zero fill** → **SafeClose**.

**Terminal-state decision for clean zero-fill (owner rule).** A zero-fill,
zero-exposure attempt is **not a failure** — it is a clean no-fill/abandon. So
SafeClose moves the cycle to **`CANCELLED`** (reason
`SIMULATED_IOC_ZERO_FILL`/`NO_FILL`), **not `FAILED`**, and releases the lock in
the same transaction. `FAILED` is reserved for real failures (unrecoverable
internal error, confirmed exchange/business failure, unrecoverable invalid state).
If `CANCELLED` is not a legal transition from the cycle's current state, the cycle
is flagged `NEEDS_RECONCILE` for the operator — **never forced to `FAILED`**. (The
state machine permits `CANCELLED` from the buy-phase states `NEW`,
`SIGNAL_DETECTED`, `BUY_REQUEST_QUEUED`, `BUY_SUBMITTED`, and `CANCEL_PENDING` for
exactly this clean-abandon case; a `FILLED` buy keeps `→CANCELLED` illegal because
it holds inventory.)

**Safe-close & lock-release criteria (rules #8/#9).** A lock is released **only**
when its cycle is positively terminal/safe — never because it is old, the process
died, no open order is visible, Redis lacks data, or an API call failed.
`SafeClose` requires **positive proof of zero exposure** (every order terminal,
zero fills); "missing from open orders" alone is never such proof.

**Persistence (rule #11).** State changes go through the state machine
(`order_events`/`cycle_state_events`, atomic with the row update). Each meaningful
decision (checked / result / decision / reason / ids) is logged to `app_logs`
(`source_binary='reconciler'`, JSON fields) — no new schema, no secrets.

**Operator-only.** Exit from `NEEDS_RECONCILE` is never automatic; an operator
path resolves it (later). The reconciler also does not perform fill accounting
(PR10) — it conservatively flags filled/partial cases instead of closing them.

The PR12 `reconciler` binary wires **no read-only clients yet** (credential
decryption lands later), so it inspects DB state and safely leaves
unverifiable-exchange orders alone.

## 11a. Balance sync (implemented in PR13 — `internal/balance`)

The `balance-sync` binary continuously records exchange balances so inventory can be
verified across a cycle (before/after buy, after sell, after close) and shown on the
dashboard. **MariaDB is the source of truth** (`wallet_balances_current` +
`wallet_balance_history`); Redis is not used.

**Read-only by construction.** The syncer holds a narrow `BalanceClient` interface
exposing **only** `Name()` + `GetBalances()` — no `PlaceOrder`/`CancelOrder` is
reachable. It makes no trading decisions, creates no cycles, and mutates no queue.
(A reflection test asserts the interface has exactly those two methods.)

**Current + history with hash dedup.** Each poll, per exchange/asset:
- compute a stable content hash `sha256(asset | available | locked | total)` over the
  **canonical** decimal strings (so `1.50 == 1.5`, and any change to
  available/locked/total flips the hash);
- **`wallet_balance_history`** gets a new row **only when the hash changed** (or it's
  the first observation) — unchanged balances never spam history;
- **`wallet_balances_current`** is upserted every observation, bumping `last_seen_at`
  (migration 013); the balance columns/hash effectively change only when the values
  do. `updated_at` tracks the last write, the history rows are the change log.

**Decimal-safe.** Balances are `decimal.Decimal` end-to-end into `DECIMAL(36,18)` —
never float; exchange precision is preserved (verified to 18 dp). `total` is taken
from the venue, or derived as `available + locked` when the venue omits it.

**Failure isolation (never wipe/zero on error).** Each exchange is polled under a
per-exchange timeout, bounded by a concurrency limit (no unbounded fan-out). If one
exchange's `GetBalances` errors or times out, it is logged (no secrets) and **its
current balances are left exactly as they were** — a failed read is never treated as
zero — and the other exchanges sync normally.

**Missing-asset behaviour (documented choice).** Only assets PRESENT in a response
are touched. An asset that **stops appearing** is **not** deleted or zeroed; its
`wallet_balances_current` row survives and its `last_seen_at` simply goes stale, which
lets later reconciliation/dashboard flag it. A balance becomes zero only when the
venue response explicitly reports zero.

**Startup / secrets.** Real authenticated balance clients need decrypted credentials
(a later PR); until then the binary wires **no clients** and idles safely (no panic,
no live credentials required, even in tests). API keys/secrets are never logged; raw
API logs still pass the secret masker.

**Relationship to cycles.** PR13 changes **no** cycles or orders and never auto-fixes
anything — it only records balances so PR12's reconciler and the dashboard can
compare expected vs actual inventory later.

## 11b. Exchange health monitor (implemented in PR14 — `internal/health`)

The `health-monitor` binary tracks per-exchange health with **read-only** probes and
records it in `exchange_health_current` (upserted) + `exchange_health_samples`
(append-only, high-volume, timestamp-indexed, no FK on the write path → retention
friendly). It makes **no** trading calls (no `PlaceOrder`/`CancelOrder` is reachable),
no decisions, and no cycle/order/queue writes — the Monitor only ever invokes
caller-supplied read-only `ProbeFunc`s (a reflection test asserts nothing it holds can
place/cancel).

**Public ≠ private health (tracked separately).** A healthy public API does not prove
the authenticated private API is healthy, so public REST (`public_status`) and private
REST/auth (`private_status` + `api_key_status`) are recorded independently; `ws_status`
covers WebSocket. The public probe is a read-only `GetMarkets`; the private probe (when
a credentialed client exists) is a read-only balance call.

**Normalized vocabularies (no adapter-invented names).**
- **Status:** `HEALTHY` · `DEGRADED` (reachable but erroring) · `UNAVAILABLE` (timeout/
  network/5xx) · `AUTH_FAILED` · `RATE_LIMITED` · `UNKNOWN` (no probe / undetermined).
- **Error category:** `timeout` · `network` · `exchange_5xx` · `exchange_4xx` · `auth` ·
  `rate_limit` · `unsupported` · `invalid_response` · `unknown`.

`health.Classify(err)` maps probe errors to `(Status, Category)` from the execution
sentinels (`ErrAuthFailed`/`ErrRateLimited`/timeout), `exchanges.NormalizedAPIError`
categories, `exchanges.ErrUnsupported`, and JSON decode errors → `invalid_response`.
`context.Canceled` (our shutdown) is **not** recorded as a health verdict.

**Failure tracking (migration 014 added `public_status`/`private_status`/
`consecutive_failures`/`last_error_category`/`last_error_message`).** Each probe
updates the status column for its kind plus the shared `latency_ms`,
`last_success_at`/`last_failure_at`, `consecutive_failures` (incremented on failure,
**reset to 0 on success**), `error_count`, and the category counters
(`timeout_count`/`rate_limit_error_count`/`auth_error_count`). A private auth error is
the only thing that flips `api_key_status` to `invalid`.

**Never wipe on a transient failure.** A failed probe records the failure and
increments counters but **preserves** `last_success_at` and prior context — it is never
erased. Each exchange is probed under a per-probe timeout, bounded by a concurrency
limit; one exchange's failure is isolated from the others.

**Missing credentials / secrets / startup.** Private health needs decrypted credentials
(a later PR); until then the binary wires only public probes and `private_status` stays
`UNKNOWN` (never marked unhealthy on absence; no panic; no live creds in tests). API
keys/secrets are never logged or stored; adapter errors are pre-masked and the stored
`last_error_message`/sample `error` are truncated.

**Reusable recorder.** `health.Recorder` is the canonical health-reporting helper; the
collector/executor/balance-sync/reconciler can adopt it to report health consistently
(PR14 does not rewrite those — the collector keeps its existing recorder for now).

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

- **The safety core (PR1–PR7 + PR12) is complete; PR8 adds signal detection but
  still nothing trades.** The trade-engine now writes `comparison_events`/`signals`
  and keeps a pending buy intent fresh, but it does **not** create cycles/orders or
  enqueue new requests (PR9), so the executor still has nothing to claim; the
  `order-executor` binary wires no real private clients (`AllowLiveExecution=false`);
  and the `reconciler` binary wires no read-only clients yet (credential decryption
  is a later PR), so it inspects DB state and safely skips unverifiable exchanges.
- **PR8 does not create the buy request, so its §2a intent helpers update/remove an
  existing QUEUED request but never create one** — until PR9 creates buy requests,
  the update/remove paths are exercised only by tests. The simulated-IOC execution
  parameters (wait/cancel) are not in the schema yet; PR8's refreshed intent payload
  carries only price/quantity/config context. `comparison_events` are written
  synchronously (async batching is a later optimization). For IRT/IRR markets the
  USDT→IRT conversion uses the same exchange's `USDT/IRT` best bid as the rate.
- **PR12 reconciler does not do fill accounting** (PR10): it conservatively flags
  filled/partial-fill cycles as `NEEDS_RECONCILE` rather than closing them. Exit
  from `NEEDS_RECONCILE` is operator-only (the operator path is a later PR). The
  recent-fills resolution path is unavailable until adapters expose it.
- **PR11 closes the buy→sell round trip.** Fills (buy and sell) are recorded as one
  aggregate row per status (from `GET_ORDER` aggregates); per-venue individual fills
  (via order-update streams / WebSocket) are a later refinement, as is fee conversion
  across non-quote assets for `realized_quote`. The operator exit from
  `NEEDS_RECONCILE` is still a later PR.
- **`health-monitor` runs public probes only** (private/authenticated health needs
  decrypted credentials — a later PR), so `private_status` stays `UNKNOWN` until then;
  the Monitor/Recorder logic is complete and tested with fake probes. WebSocket health
  is recorded if a WS probe is supplied but the binary wires none yet (WS is collector-
  side and partial). The collector keeps its own pre-PR14 health recorder; migrating it
  onto `health.Recorder` is a later cleanup.
- **`balance-sync` wires no clients yet** (authenticated balance reads need decrypted
  credentials — a later PR), so it boots and idles. The syncer logic is complete and
  tested with fake read-only clients; once credentials land, real `BalanceClient`s
  are injected with no logic change. Per-exchange timeout/concurrency are bounded; a
  failed read never wipes balances and a missing asset is never zeroed.
- **Sell management is polling-based:** the engine reprices/polls on a periodic pass
  (default 2s) reading the Binance reference from Redis; there is no steady-state
  WebSocket order-update path yet. A cycle whose reference price is missing/stale is
  left untouched that pass (never priced on stale data).
- **Gated integration tests share one MariaDB/Redis** and include a global open-cycle
  scan (the reconciler), so the gated suite must be run with **`go test -p 1 ./...`**
  (serial packages) to avoid cross-package contention. The default offline
  `go test ./...` (no `V3_TEST_*` env) stays fully parallel.
- **Concurrency:** the per-exchange limit is enforced across processes via
  `GET_LOCK` (no single-instance restriction needed). Distributed slot leasing
  isn't implemented, but the GET_LOCK approach is sufficient on one MariaDB.
- Credential encryption is still schema-only (PR2); no plaintext is exposed.
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

- **PR14 — read-only health by construction**: the Monitor only invokes caller-supplied
  `ProbeFunc`s and holds no order client; a reflection test asserts nothing it holds can
  place/cancel. Probes are public `GetMarkets` (and a read-only balance call for private
  when creds exist).
- **PR14 — public and private health are separate** (`public_status` vs
  `private_status`/`api_key_status`): a healthy public API never implies private health.
- **PR14 — fixed normalized vocabularies** for status (HEALTHY/DEGRADED/UNAVAILABLE/
  AUTH_FAILED/RATE_LIMITED/UNKNOWN) and error category (timeout/network/exchange_5xx/
  exchange_4xx/auth/rate_limit/unsupported/invalid_response/unknown); `health.Classify`
  maps errors; `context.Canceled` (shutdown) is not a health verdict.
- **PR14 — a transient failure never wipes** `last_success_at`/context; it increments
  `consecutive_failures` (reset to 0 on success) + counters. One exchange's failure is
  isolated (bounded concurrency + per-probe timeout).
- **PR14 — missing credentials → `private_status` UNKNOWN** (never unhealthy on absence);
  the binary runs public probes only and never panics; no secrets logged/stored
  (messages truncated; adapter errors pre-masked). Migration 014 added the normalized
  status + failure-tracking columns.
- **PR14 — `health.Recorder` is the canonical reusable recorder** for other components
  to adopt later; PR14 doesn't rewrite the collector's existing recorder.

- **PR13 — `balance-sync` is read-only by construction**: a narrow `BalanceClient`
  (only `Name`+`GetBalances`) is held, so no order-mutating call is reachable. It
  records balances only; it never touches cycles/orders/queue.
- **PR13 — hash-deduped history**: `wallet_balance_history` gets a row only when the
  `sha256(asset|available|locked|total)` content hash changes; `wallet_balances_current`
  is upserted every poll with a fresh `last_seen_at` (migration 013).
- **PR13 — a failed/timed-out balance read never wipes or zeros** the prior current
  balances; one exchange's failure is isolated from the others (bounded concurrency +
  per-exchange timeout).
- **PR13 — a missing asset is never zeroed/deleted**: only assets present in a
  response are touched; an absent asset's row survives and its `last_seen_at` goes
  stale (detectable by reconciliation/dashboard). Zero requires an explicit venue zero.
- **PR13 — balances are decimal end-to-end** into `DECIMAL(36,18)` (never float);
  `total` is derived as `available+locked` only when the venue omits it.
- **PR13 — the binary wires no clients yet** (credential decryption is later) and
  idles safely; no secrets are logged.

- **PR11 — sell quantity is the actual filled inventory** (`bought − sold`, floored to
  `step_size`), never the requested buy quantity. Partial buys sell their filled part
  (added `BUY_PARTIALLY_FILLED → SELL_REQUEST_QUEUED`).
- **PR11 — sell price** `floor(binanceRef × (1 − sell_offset_bps/10000), tick)`; offset
  and venue tick/step/min are DB config (loaded into `MarketConfig` from
  `exchange_markets`). Below-minimum sells are not placed.
- **PR11 — the resting sell schedules no auto-cancel** (unlike the buy IOC); fills are
  observed by a Manager-driven `sell_status` poll (one outstanding per sell). WS order
  updates deferred.
- **PR11 — repricing is cancel→replace, interval-gated** (`reprice_interval_seconds`,
  `last_reprice_at`), skipped while a sell place/cancel is `CLAIMED`/`IN_FLIGHT`; the
  cancel's final status is always read before reselling (cancel may race a fill);
  ambiguous cancel/place → `NEEDS_RECONCILE`, never re-sent.
- **PR11 — the symbol lock releases only on a full exit** (`SELL_FILLED → CLOSED`); a
  clean zero-fill buy (PR10) is the only other release. `realized_quote` nets fees
  only when they are denominated in the quote currency (other-asset fees stored raw).
- **PR11 — the sell Manager runs in the trade-engine** (it has the Redis reference
  price + config cache); it writes DB rows + queue requests only — the executor stays
  the sole mutating exchange caller. `RETRY_SCHEDULED retry_count==0` marks scheduled
  reprice/status steps (vs `>0` retries) — see §8.

- **PR10 — simulated IOC is modelled as queued/scheduled work**, not a worker sleep:
  after a place ack the cancel is enqueued with a future `next_retry_at`, and after
  the cancel the final `GET_ORDER` is likewise scheduled (`queue.EnqueueScheduled`).
  No executor goroutine is blocked for the wait.
- **PR10 — a missing order / status-fetch error is never proof of zero fill** →
  Ambiguous → NEEDS_RECONCILE (lock held). Only a settled `filled=0` is a proven
  zero-fill → CANCELLED (`SIMULATED_IOC_ZERO_FILL`), never FAILED. A definite *place*
  rejection (no exposure) is the distinct clean-fail path (`OnPlaceRejected`):
  order+cycle FAILED + lock released.
- **PR10 — partial fills continue with the filled quantity only**; the remainder is
  not inventory (PR11 sells `filled_quantity`). A partial with no usable price is
  ambiguous → NEEDS_RECONCILE.
- **PR10 — `actual_execution_mode` is mapped from the venue liquidity flag**
  (`OrderStatus.Liquidity`), defaulting to `UNKNOWN` — never inferred/guessed. Added
  `execution.OrderStatus.Liquidity` (additive contract field).
- **PR10 — one aggregate fill row per final status** with a deterministic
  `exchange_fill_id` (`final:<exchange_order_id>`), so repeated processing is
  idempotent via `UNIQUE(order_id, exchange_fill_id)`. Per-venue fills are deferred.
- **PR10 — the PLACE payload (`BuyIntentPayload`) moved to `internal/orders`** as the
  shared producer/consumer contract (buyflow produces it, the executor/processor
  consume it); JSON field names unchanged.
- **PR10 — the gated suite runs serially** (`-p 1`) because the reconciler's global
  open-cycle scan shares the test DB; offline tests stay parallel.

- **PR8 — spread basis is Iranian best ask vs Binance best bid** (owner-confirmed),
  the most conservative realizable comparison; fee-adjusted by buy (taker) + sell
  (maker) fees.
- **PR8 — IRT/IRR markets convert via the same exchange's `USDT/IRT` best bid** from
  Redis; missing/stale rate → no signal. The conversion rate and quote unit are
  stored on `comparison_events`/`signals` (migration 009) so units are never mixed
  silently. USDT markets compare directly (`reference_rate` NULL).
- **PR8 — the signal loop holds no exchange client by construction** (a reflection
  test asserts no engine field can `PlaceOrder`/`CancelOrder`); it never creates
  cycles/orders (PR9). Only `comparison_events`/`signals` are written.
- **PR8 — pending-intent scope is `exchange_market_id`** (one strategy today). PR8
  refreshes/removes an existing QUEUED buy request but never creates one;
  `SELECT … FOR UPDATE` + a `status='QUEUED'` guard make CLAIMED/IN_FLIGHT requests
  untouchable.
- **PR8 — `comparison_events` written synchronously** for now (low PR8 throughput);
  async batching deferred. Stale/missing/invalid data writes nothing (fail-safe).
- **PR8 — `MarketConfig` gained `ExchangeID`** so the engine can stamp
  `comparison_events.exchange_id` without a hot-path DB read.

- **PR12 — the reconciler holds a `ReadOnlyClient` interface** (no `PlaceOrder`/
  `CancelOrder` methods), so auto-send is impossible by construction; a
  panic-on-call fake + the interface design prove it in tests.
- **PR12 — "missing/unknown order" is never proof of no fill** → `NEEDS_RECONCILE`,
  never a clean close. SafeClose requires positive proof of zero exposure (all
  orders terminal, zero fills) and only then releases the lock, in the same tx.
- **PR12 — lock release is conservative:** only when the cycle is positively
  terminal/safe — never on age, process death, missing open order, Redis gap, or
  API failure.
- **PR12 — filled/partial cycles → `NEEDS_RECONCILE`** (fill accounting is PR10);
  the reconciler never closes a cycle with exposure.
- **PR12 (correction) — clean zero-fill closes to `CANCELLED`, not `FAILED`.** A
  zero-fill, zero-exposure attempt is a clean no-fill (reason
  `SIMULATED_IOC_ZERO_FILL`), not a trading failure. `FAILED` is reserved for real
  failures. The state machine gained `→CANCELLED` from `NEW`/`BUY_REQUEST_QUEUED`/
  `BUY_SUBMITTED` (in addition to `SIGNAL_DETECTED`/`CANCEL_PENDING`) so a buy-phase
  abandon can cleanly cancel; `BUY_FILLED→CANCELLED` stays illegal (inventory).
- **PR12 — `SUBMITTED→CANCELLED` is illegal** in the order machine; a clean
  exchange-cancel is only advanced from `CANCEL_PENDING` (the cancel we
  requested). An unexpected cancel from another state → `NEEDS_RECONCILE`.
- **PR12 — decisions are logged to `app_logs`** (no new schema, no secrets);
  state changes additionally produce state events.
- **PR12 — `symbollock`** gained read/release helpers; the transactional Acquire
  (acquire-lock + create-cycle + enqueue) remains PR9.

- **PR7 — per-exchange concurrency uses `GET_LOCK`** around count+claim on a
  pinned connection (+ `FOR UPDATE SKIP LOCKED`), so the limit holds across
  multiple executor processes without a single-instance restriction.
- **PR7 — live-execution is OFF by default** (`AllowLiveExecution=false`): a dev
  executor claims only read-only request types and the binary wires no real
  clients, so it cannot place a real order. There is intentionally NO direct-send
  method/CLI (asserted by a reflection test).
- **PR7 — conservative crash/ambiguous handling:** mutating requests are marked
  `IN_FLIGHT` (committed) before sending; an ambiguous outcome or a stuck
  `IN_FLIGHT` mutating row becomes `DEAD` + order `NEEDS_RECONCILE` and is NEVER
  blindly re-sent. Read-only requests are re-queued freely.
- **PR7 — completion is atomic with order state:** queue `SUCCEEDED` + the order
  transition share one transaction; if the transition fails the request is not
  marked succeeded (stays `IN_FLIGHT` for the sweeper).
- **PR7 — queue state stays in MariaDB, never Redis** (it is authoritative
  recovery state).

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
| PR6 | `pr6-config-system` | **accepted** | `internal/configstore`: DB-backed versioned trading config. `Snapshot` (MarketConfig merging exchange_markets flags + symbol_configs params, ExchangeConfig, fees, retention, active version), `Store.LoadSnapshot`/`ActiveVersion`, copy-on-write `Cache` + background `Run` reloader (non-blocking; keeps good config on reload failure), `ActivateVersion` + audited `UpdateMinSpreadBps` (version+audit in one tx, no secrets), validation (value sanity + enable-flag hierarchy), version-stamping helpers. Tests: sqlmock loaders/version/audit, cache COW/reload/concurrent-read, validation, gated MariaDB full-path. File-only bootstrap unchanged; no env config; not yet wired into a binary. |
| PR7 | `pr7-exchange-request-queue` | **accepted** | `internal/queue` (DB-backed priority queue): Enqueue (idempotency-rejected), cross-process-safe Claim (GET_LOCK + count + FOR UPDATE SKIP LOCKED; priority/next_retry_at/per-exchange-limit/enabled/type filters), MarkInFlight, MarkSucceeded/Failed/Dead, ScheduleRetry (capped backoff→DEAD), conservative SweepStuck (read-only requeue / mutating→DEAD+order NEEDS_RECONCILE). `internal/executor` (order-executor): claim+dispatch loop, read-only & mutating handlers, conservative ambiguous→DEAD+reconcile, atomic complete+order-transition (rollback-safe), `AllowLiveExecution` guard (default off), NO direct-send path. cmd/order-executor wired with no live clients. Tests: queue sqlmock + gated MariaDB (incl. concurrent claimers), executor classifiers + reflection no-send guard + gated end-to-end with fake clients. Closes the safety core; nothing trades yet. |
| PR12 | `pr12-startup-reconciler` | **accepted** | `internal/reconciler` (read-only; never auto-sends — holds a `ReadOnlyClient` with no Place/Cancel): `ReconcileStartup` + idempotent `RunPeriodic`; pure decision matrix (`decide.go`); capability-based known/unknown-exchange-order-id paths (unknown→never resend, positively-identify-or-NEEDS_RECONCILE); cycle decisions Continue/SafeClose/NEEDS_RECONCILE; **clean zero-fill safe-close → CANCELLED (NO_FILL) + lock release, NOT FAILED** (correction); missing/unknown order ≠ proof of no fill; decisions logged to app_logs; state via state machine. `internal/symbollock` read/release helpers (Acquire is PR9). cmd/reconciler wired (no clients). Tests: pure decide unit + gated MariaDB (decision matrix, safe-close+lock-release, ambiguous-keeps-lock, client-id attach, idempotent repeat, stuck-reporting, rollback, no-mutating-call guard). Completes the safety core (PR1–PR7 + PR12). |
| PR8 | `pr8-trade-engine-signal` | **accepted** | `internal/engine` (trade-engine signal loop): subscribe `market_events`; read Redis books/prices + configstore snapshot; **owner-defined spread implemented as planned** = (Binance best bid − Iranian best ask)/ask×10000, fee-adjusted (taker buy + maker sell); USDT direct / IRT-IRR convert via same-exchange `USDT/IRT` rate (missing/stale → no signal); freshness + enable-flag + config-v0 gating; write `comparison_events` (every computable comparison) + `signals` (passed), config-version stamped, quote_unit + reference_rate audited. **No order execution, no private exchange calls, no cycle creation, no order creation.** §2a pre-cycle pending-intent: update/remove existing **QUEUED** entry-buy request (FOR UPDATE + QUEUED guard; never touches CLAIMED/IN_FLIGHT; never creates). Migration 009 (audit columns); `MarketConfig.ExchangeID`. cmd/trade-engine wired (no private clients). Tests: offline spread/quote/targets/no-client + gated MariaDB+Redis (USDT signal, below-threshold, stale/missing data, disabled-for-signal, IRT conversion, fee-adjusted, intent update/remove/dedup, config-stamp). |
| PR9 | `pr9-cycle-creation-buy-enqueue` | **accepted** | `internal/buyflow` (+ `symbollock.Acquire`): first code that creates trading rows. On an accepted signal for a trading-enabled, fresh market it runs ONE transaction — insert cycle (config-stamped + signal context + execution mode) → acquire symbol lock (dup scope → `ErrSymbolLocked` → rollback, no orphan) → insert entry_buy order (`local_client_order_id`, limit, TIF NULL) → state machine cycle `NEW→SIGNAL_DETECTED→BUY_REQUEST_QUEUED` + order `NEW→REGISTERED→QUEUED` → enqueue `PLACE_ORDER` (deterministic idempotency key, full intent payload) → commit. Owner-defined maker-first/taker-fallback decision (`buyflow.Decide`, pure): maker limit below ask by `maker_price_offset_bps`, taker at ask after `maker_attempts_before_taker` maker attempts within `maker_signal_window_seconds`; persists intended mode/attempt/offset/ask. One shared attempt counter advances on create AND on refresh of the active scope (resets on window expiry). No-duplicate via the lock; the active cycle's still-QUEUED buy is **refreshed in place and re-decided** (so the SAME request escalates MAKER_FIRST→MAKER_RETRY→TAKER_FALLBACK without a duplicate); cycle-tied requests never deleted; CLAIMED/IN_FLIGHT never mutated. **Executes nothing** (no private client, no place/cancel/query, no fills, no lock release). Migration 010 (symbol_configs maker/taker cols + orders/cycles exec-mode cols); configstore loads the policy. Tests: offline Decide + gated (atomic create, rollbacks, dup-lock-blocks, maker→retry→taker across cycles, window reset, refresh-advances-attempt-and-escalates, refresh-window-expiry-resets, refresh-no-dup, CLAIMED/IN_FLIGHT untouched, idem-key unique, config stamp, flags/stale block, state-machine events, no private client). |
| PR10 | `pr10-order-fill-processing` | **accepted** | `internal/orders` (buy-side order/fill processing) + executor wiring. Simulated IOC as queued work (no worker sleeps): PLACE ack → `OnPlaceAck` (order QUEUED→SUBMITTED→ACKED, cycle →BUY_SUBMITTED, schedule CANCEL at `now+maker_wait`) → CANCEL ok/definite-reject → `OnCancelResult` (order →CANCEL_PENDING, schedule GET_ORDER) → `ProcessFinalStatus` (classify → fills + transitions + lock). Pure `Classify` (full/partial/zero/ambiguous); missing order ≠ zero fill; zero-fill → CANCELLED (`SIMULATED_IOC_ZERO_FILL`, lock released) not FAILED; partial → continue filled qty (lock held); full → BUY_FILLED (lock held); ambiguous (incl. ambiguous cancel/place) → order+cycle NEEDS_RECONCILE (lock held, never re-sent); definite place-rejection → `OnPlaceRejected` (FAILED + lock released). Fill accounting (filled/remaining/avg/quote/fee/fee_asset/`actual_execution_mode`/`fill_result`/`last_normalized_status`) + idempotent aggregate `fills` row (deterministic id). All state via `internal/state`; queue+state+fill+lock in one tx (never SUCCEEDED if state failed). Native IOC never forced (TIF empty). `queue.EnqueueScheduled`; `execution.OrderStatus.Liquidity`; migration 011; `BuyIntentPayload` moved to `internal/orders`. Tests (fake clients only): offline Classify matrix + gated (place→cancel→final scheduling, zero/partial/full, missing-not-zero, ambiguous-cancel→reconcile, place-rejected-clean, fee/avg, maker/taker, idempotent repeat, rollback) + executor end-to-end IOC loop. |
| PR11 | `pr11-sell-management` | **accepted** | `internal/sellflow` (exit sell create/reprice/Manager) + `internal/orders` sell processing + executor routing + engine driver. Sell on the ACTUAL filled inventory (`bought − sold`, step-floored), never the requested qty; partial buys sell their filled part (`BUY_PARTIALLY_FILLED→SELL_REQUEST_QUEUED`). Price `floor(binanceRef×(1−sell_offset_bps/10000), tick)`, min-order enforced; offset/tick/step/min are DB config (loaded into `MarketConfig`). `CreateSell` one tx (insert sell order → cycle→SELL_REQUEST_QUEUED + order NEW→REGISTERED→QUEUED → enqueue sell PLACE; rollback on failure; no-duplicate via active-sell guard). Resting place (`OnSellPlaceAck`, no auto-cancel) + Manager-driven `sell_status` poll (`ProcessSellStatus`): partial→SELL_PARTIALLY_FILLED (manage remainder), full→SELL_FILLED→CLOSED + PnL + lock release, ambiguous/missing→NEEDS_RECONCILE. Repricing cancel→replace, interval-gated (`reprice_interval_seconds`/`last_reprice_at`), skipped while a sell place/cancel is CLAIMED/IN_FLIGHT; cancel's final status always read before reselling; ambiguous→NEEDS_RECONCILE. Close writes exit accounting + `realized_quote` (fees netted only when quote-denominated; migration 012). All state via `internal/state`; queue+state+fill+lock atomic; engine never calls exchanges (executor only). Tests (fake clients): pure price/tick/step/min + gated sellflow (create full/partial, no-dup, below-min, tick-snap, rollback, reprice interval/in-flight/no-resting, Manager-creates-sell) + gated orders sell (place-ack-rests, partial-manages, full-closes+PnL, missing-ambiguous, idempotent, reprice-cancel partial/raced-full) + executor end-to-end sell loop. |
| PR13 | `pr13-balance-sync` | **accepted** | `internal/balance` + `cmd/balance-sync`: continuous read-only balance sync. Narrow `BalanceClient` (only `Name`+`GetBalances` — no place/cancel reachable). Per poll, per exchange/asset: content hash `sha256(asset\|available\|locked\|total)` over canonical decimals; `wallet_balance_history` row only when the hash changes (no dup spam); `wallet_balances_current` upserted every observation with fresh `last_seen_at` (migration 013). Decimal end-to-end into `DECIMAL(36,18)` (never float; 18-dp preserved); `total` derived as available+locked when omitted. Bounded concurrency + per-exchange timeout; one exchange's failure/timeout is isolated and NEVER wipes/zeros prior balances; a missing asset is never zeroed/deleted (its row survives, `last_seen_at` goes stale). Changes no cycles/orders/queue. Binary wires no clients yet (credential decryption later) and idles safely; no secrets logged. Tests (fake read-only clients): offline hash + read-only-interface guard + no-clients startup; gated (first-obs current+history, unchanged-no-dup, changed-avail/locked add history, missing-asset-not-zeroed, failure-isolation-keeps-previous, precision, timeout-keeps-previous, context-cancel-stops). |
| PR14 | `pr14-health-monitor` | **in review** | `internal/health` + `cmd/health-monitor`: read-only per-exchange health. Monitor invokes only caller-supplied read-only `ProbeFunc`s (public `GetMarkets`; private balance read when creds exist) — no place/cancel reachable (reflection guard); no cycle/order/queue writes. `Classify(err)` → normalized Status (HEALTHY/DEGRADED/UNAVAILABLE/AUTH_FAILED/RATE_LIMITED/UNKNOWN) + Category (timeout/network/exchange_5xx/exchange_4xx/auth/rate_limit/unsupported/invalid_response/unknown) from execution sentinels + NormalizedAPIError + ErrUnsupported + json errors; `context.Canceled` not recorded. `Recorder` upserts `exchange_health_current` (per-kind status, latency, last_success/failure, consecutive_failures reset-on-success, error/timeout/rate/auth counters, last_error_category/message) + appends `exchange_health_samples` (no FK, timestamp-indexed). Public/private tracked separately; auth error → api_key_status invalid; transient failure never wipes last_success; bounded concurrency + per-probe timeout isolate failures. Migration 014 (normalized status + failure-tracking cols). Binary runs public probes only (private UNKNOWN until creds); no secrets logged/stored. Tests (fake read-only probes): offline Classify matrix + read-only guard + no-targets startup; gated (healthy public, timeout/auth/rate/network/5xx/invalid classified+counted, private-auth→key-invalid, failure isolation, consecutive-then-reset, last-success preserved, context-cancel-stops). |
