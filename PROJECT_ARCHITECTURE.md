# v3TradeBot — Project Architecture

> **This document is part of the source code.** Every PR that adds, removes, or
> changes anything important (a table, a state transition, a binary, a Redis key,
> queue/config/recovery behaviour, a safety rule, a limitation, or a deferral)
> MUST update this file in the same PR. Outdated docs are treated as a bug.

Last updated: **PR2 correction — schema hardening (forward migration 025: signals FKs, operational indexes on comparison_events/signals, financial-value CHECK constraints). Built on the corrected PR1 (EnsureCurrent checksum/unknown-version enforcement).**

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

**Implementation placement:** signal evaluation + write `comparison_events`/`signals`
→ PR8 (signal-only); one-active-pending-intent via the **symbol lock** +
no-duplicate-queued-signal (refresh the unsent QUEUED buy) + maker/taker decision +
buy enqueue → **PR9** (all transactional); simulated-IOC execution + fill/status
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
- **PR2 correction (025):** schema hardening, delivered as a FORWARD ALTER migration
  (not an edit of 002/005/006 — applied migrations are immutable, per `EnsureCurrent`):
  FKs `signals.exchange_id → exchanges.id` and `signals.config_version →
  config_versions.id` (both NULLable; a NULL passes); operational indexes
  `comparison_events(exchange_id, canonical_symbol, created_at)` and
  `signals(exchange_id, canonical_symbol, created_at)` for "latest by exchange+symbol"
  queries; and DB-level financial CHECK constraints — `orders` (quantity / filled ≥ 0,
  filled ≤ quantity, limit_price / fee ≥ 0), `fills` (quantity / price / fee ≥ 0),
  `exchange_markets` (tick / step ≥ 0 — 0 is the documented "no snap" sentinel; min
  qty / min amount ≥ 0), and `wallet_balances_current` / `wallet_balance_history`
  (available / locked / total ≥ 0 — spot wallets are non-negative). NULL columns keep
  NULL semantics (a CHECK against NULL passes). No FK is added to a retention table.

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
  checksum and is a HARD STOP** — add a new `NNN` file instead. The checksum check
  is enforced at BOTH `migrate.Run` (before applying) AND at service startup via
  `migrate.EnsureCurrent`/`Status` (PR1 correction), so an edited applied migration
  fails fast even though services never call `Run`.
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
- **Fail-fast (PR1 + correction):** service binaries call `migrate.EnsureCurrent`
  on startup and refuse to run if **(a)** any migration is pending, **(b)** an
  already-applied migration's checksum differs from the embedded file
  (`ChecksumMismatchError` — schema tampered/edited), or **(c)** the DB has an
  applied version the binary does not know (`UnknownAppliedVersionError` — schema
  NEWER than the code, e.g. an old binary against a migrated DB). `Status` performs
  the same integrity verification and surfaces those errors; services never
  self-migrate.

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
- **WebSocket resilience (PR5 correction):** an unexpected stream close while the
  collector context is still active is **never** treated as a normal shutdown — it is
  logged + counted as a health failure (and on `WSFailureCount`), and the subscriber
  **reconnects with capped exponential backoff** (200 ms → 30 s), retrying for the whole
  lifetime of the context. A target is never silently abandoned; the only clean exit is
  context cancellation. During a sustained outage it additionally does a one-shot REST
  poll between attempts (sequential, no double-ingest) as a safety net while it keeps
  trying to restore the stream.
- **Publish-after-save ordering (PR5 correction):** a `market_event` is published **only
  after** both the order-book and price snapshots are written to Redis. If either save
  fails, the event is suppressed — consumers are never pointed at a snapshot that is not
  in the cache.
- **REST `received_at` (PR5 correction):** for poll fetches, `received_at` is stamped
  **after** a successful `GetOrderBook` (when the data is actually in hand), not before the
  request; latency is still measured from request start.
- Raw API calls are logged through the secret-masking IO logger (§12).
- Decoupled via interfaces (`MarketStore`, `HealthRecorder`) so it is unit-tested
  with fakes and never needs a live exchange/Redis/DB in unit tests.

## 7b. Trade-engine signal loop (implemented in PR8 — `internal/engine`)

The signal loop consumes `market_events`, reads the latest books/prices from Redis
and trading config from the configstore cache, computes the spread, and writes
`comparison_events` / `signals`. **Hard boundaries — PR8 is SIGNAL-ONLY:** it NEVER
calls an exchange (no private clients, no `PlaceOrder`/`CancelOrder` — a reflection
guard asserts no engine field can place/cancel), and its ONLY writes are
`comparison_events` and `signals`. **Even for a trading-enabled market with a passing
signal, PR8 creates no cycle, order, `exchange_request`, or symbol lock, and calls no
`buyflow`.**

Buy-cycle preparation is a SEPARATE capability gated behind `Config.PrepareBuyCycles`,
which the engine **leaves false by default** — that default is the safety gate, so the
engine library is signal-only unless a caller opts in. **In PR8** the `cmd/trade-engine`
binary left it `false` (the whole executable was signal-only). **PR9 enables it**
(`cmd/trade-engine` sets `PrepareBuyCycles: true`) and owns all cycle/order/exchange_request/
symbol_lock creation/refresh. Static invariant #7 now asserts the engine only ever reaches
buyflow *through* the `PrepareBuyCycles` gate (every `buyflow.Create*/Refresh*` call lives in
`internal/engine/engine.go` inside the flag-guarded `prepareBuy`); `TestSignalOnlyEvenWhenTradingEnabled`
covers the library default (flag off ⇒ no cycle/order/request/lock even for a passing,
trading-enabled signal). When enabled, a passing signal for a trading-enabled market prepares
the buy — cycle + symbol lock + order + QUEUED `PLACE_ORDER` request — in ONE transaction via
`internal/buyflow` (so a queue row is never created/updated/deleted in isolation; no orphan
state), and the originating signal is linked to the created cycle (`signals.cycle_id`). The
no-duplicate "refresh the existing QUEUED buy" path is `buyflow.RefreshActiveCycleBuy` (see
§10a). There are no standalone `UpdatePendingBuyRequest`/`RemovePendingBuyRequest` methods —
those never existed. The engine still NEVER calls an exchange; the order-executor is the only
sender.

**Fee lookup (PR6 model).** Fees come from `Snapshot.FeeFor(m.ExchangeID,
m.ExchangeMarketID)` — a market-specific override else THIS exchange's default,
never another exchange's default (no shared `Fees[0]`).

**Subscription resilience (PR8 correction).** The signal loop owns the
`market_events` subscription for the LIFETIME of the context. An unexpected channel
close (or a failed `SubscribeMarketEvents`) while ctx is active is **never** a silent
stop — it is logged and the subscription is re-established with capped exponential
backoff (200 ms → 30 s). `runSignalLoop` returns ONLY when ctx is cancelled
(returning a non-nil ctx error), so a Redis pub/sub drop can never make the engine
quietly exit.

**Event fan-out (`targets`).** On a reference-venue (Binance) tick, every Iranian
`enabled_for_signal` market on the same base asset is re-evaluated (its reference
moved). On an Iranian tick for the configured **quote-rate symbol** (`USDT/IRT`),
**every signal-enabled rial-quoted (IRT/IRR) market on the same exchange** is
re-evaluated — their Binance reference is derived as `Binance(USDT) × USDT/IRT`, so a
`Nobitex USDT/IRT` move refreshes `Nobitex BTC/IRT`, `ETH/IRT`, `SOL/IRT`, … (PR8
correction). USDT-quoted markets carry no such dependency. On any other Iranian tick,
only that market is evaluated. An unconfigured system (active config **version 0**)
produces no targets at all.

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

**No-duplicate pending buy intent (§2a, scope = `exchange_market_id`) — PR9, not PR8.**
PR8 never creates or mutates buy requests. When PR9's `buyflow` later handles a passing
signal for a trading-enabled market, the no-duplicate logic lives there: if the scope is
already locked it refreshes the existing **QUEUED** (not-yet-claimed) entry-buy request
**in the same transaction** as the cycle/lock state (`buyflow.RefreshActiveCycleBuy`),
guarded on `status='QUEUED'` so a `CLAIMED`/`IN_FLIGHT`/sent request is never touched.
Crucially this is one transaction across cycle + order + request + lock, so the queue
row is never updated/deleted in isolation (which could otherwise strand an open cycle
with no request to execute). There are no standalone `UpdatePendingBuyRequest`/
`RemovePendingBuyRequest` methods.

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
- **`MarkInFlight` is the pre-send boundary + is GUARDED (PR7 correction):** the
  executor commits `MarkInFlight` (`CLAIMED→IN_FLIGHT`) BEFORE sending a mutating
  request. The update is `WHERE id=? AND status='CLAIMED'` and checks `RowsAffected`:
  zero rows → `ErrRequestNotClaimed`, and the executor MUST NOT call
  `PlaceOrder`/`CancelOrder` (prevents a stale/duplicate send if a concurrent sweep
  requeued the row). `IN_FLIGHT` — not `CLAIMED` — is the "maybe sent" marker.
- **Crash recovery via `SweepStuck` (rule #6, PR7 correction):**
  - **Stale `CLAIMED`** (claimed but never reached `IN_FLIGHT` → NEVER sent) → reset to
    `QUEUED`, `claimed_by`/`claimed_at` cleared, re-claimable by another executor. Safe
    for any type (including mutating) because nothing was sent. Without this a crash
    between `Claim` and `MarkInFlight` would strand the row in `CLAIMED` forever (and
    hold a concurrency slot).
  - **Stale `IN_FLIGHT` read-only** → re-queued (idempotent).
  - **Stale `IN_FLIGHT` mutating** → `DEAD` + the owning order pushed to
    `NEEDS_RECONCILE` (NEVER blindly re-sent).
- **Guarded terminal transitions (PR7 correction):** `MarkSucceeded`/`MarkFailed`/
  `MarkDead` update `WHERE id=? AND status IN ('CLAIMED','IN_FLIGHT')` and check
  `RowsAffected`. A row already in a CONFLICTING terminal status → `ErrRequestNotActive`
  (the tx rolls back), so a late worker can never overwrite a newer status (e.g.
  `SUCCEEDED` clobbering a reconciliation `DEAD`). Re-applying the SAME status is an
  idempotent no-op (safe crash-recovery reprocessing — mirrors §9 strict replay).
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
  re-read and the outcome is classified as **replay**, **stale version**
  (`ErrStaleVersion`), **state mismatch** (`ErrStateMismatch`), or **missing row**
  (`ErrUnknownRow`). This is how crash/duplicate-event replays stay safe.
- **Strict replay (PR3 correction):** a no-op **replay** is accepted **only for the
  exact already-applied transition** — the row must be in the target state **at
  version `expected_version + 1`**, and (stronger guard) the recorded event at that
  version must be this same `from→to`. A transition that merely happens to share the
  target state at a **higher** version is a *stale* caller acting on old knowledge and
  is rejected with `ErrStaleVersion` — e.g. `SELL_REPRICE_PENDING→SELL_SUBMITTED` with
  `expected_version=10` against a row already at `SELL_SUBMITTED` version 20 fails, it
  is **not** treated as a replay. `Result.Replayed=true` is returned only in the exact
  case (and no second event is written).

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

**Signal → cycle link (audit).** The engine writes the `signals` row first (cycle_id
NULL); when a cycle is then created/refreshed for it, buyflow stamps `signals.cycle_id`
with that cycle **inside the same transaction** (`SignalContext.SignalID`), so the
signal → cycle audit trail is complete.

**Price/quantity validity + market-rule boundary.** buyflow **rejects a non-positive
limit price or quantity** (misconfigured offset/`buy_size`) — `CreateBuyCycle` returns
an error (nothing persisted) and `RefreshActiveCycleBuy` is a no-op — so an unsendable
intent is never enqueued. Venue **market-rule conformance** (`tick_size`, `step_size`,
`min_order_amount`, `min_order_quantity`) is NOT snapped in PR9: PR9 persists the
owner-decided price/qty; final tick/step/min validation is applied before the exchange
send, and a venue rejection of a non-conforming order is handled cleanly by the executor
(definite-rejection path → request `FAILED` + order `FAILED`/reconcile, never stuck
`QUEUED`, never re-sent — PR7). Non-positive values can never reach the exchange.

**No-duplicate / refresh / invalidation** (per §2a): if the scope is already locked,
PR9 does **not** create a second cycle; if that cycle's buy request is still **QUEUED
(unsent)** a newer valid signal **refreshes it in place** (`buyflow.RefreshActiveCycleBuy`).
The refresh is **fully guarded**: the `SELECT … FOR UPDATE` matches only when the WHOLE
execution state is queued/active — `symbol_lock.state='ACTIVE'` **and**
`cycle.state='BUY_REQUEST_QUEUED'` **and** `order.state='QUEUED'` **and**
`exchange_request.status='QUEUED'`; if any has moved on (request `CLAIMED`/`IN_FLIGHT`,
order/cycle advanced, lock released) no row matches and **nothing is refreshed** (a
claimer may already be sending it). It then updates the cycle, order, and request
**together in one tx so all three describe the same current intent** — including a **full
refresh of the cycle signal snapshot** (`signal_time`, `binance_price_at_signal`,
`iranian_price_at_signal`, `spread_bps`, `fee_adjusted_spread_bps`, `buy_size`,
`config_version`, mode/attempt), not just the order/payload, so the cycle audit is never
left stale. Each guarded UPDATE re-asserts its expected state in the `WHERE` and
**verifies `RowsAffected == 1`** (a 0-row update rolls the whole refresh back). Re-running
the maker/taker decision lets the SAME request escalate
`MAKER_FIRST → MAKER_RETRY → TAKER_FALLBACK` without a duplicate; a `CLAIMED`/`IN_FLIGHT`/sent
request is never touched and a cycle-tied request is never physically deleted. Invalidation
of a started cycle's buy is left to the PR10/PR11 lifecycle (the engine does not delete it).

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
- **Full** — `filled`, remaining 0, qty agrees, **AND a usable cost basis exists** (see
  the cost-basis rule below).
- **Partial** — `canceled`/`expired`/`partially_canceled` with a usable partial fill
  (positive filled qty **and** a usable cost basis).
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

**PLACE_ORDER routing is by the DB order role, never `payload.side` (PR10 correction #2).**
`executor.dispatchPlace` reads the **trusted** `orders.role` (`entry_buy` → buy handler,
`exit_sell` → sell handler); the payload's `"side"` text is never used to pick the handler.
So a payload that lies about its side cannot bypass the intended handler/validation: a buy DB
order always routes to the buy handler, whose `Validate()` then rejects `side != buy`. (The
former `PayloadSide` helper was removed.) A request with no `order_id`/`cycle_id`, or whose
order row cannot be read/routed, is a request-only failure.

**Pre-send payload validation + clean rejection (`BuyIntentPayload.Validate`, PR10 corrections
#1/#5).** In the buy handler, before marking IN_FLIGHT or calling `PlaceOrder`, the payload
must both **decode** and **validate**: `simulated_ioc=true`, `side=buy`, `order_type=limit`,
`intended_price > 0`, `intended_quantity > 0`, `local_client_order_id` non-empty. (Without
this, `decimalOrZero` would silently turn a bad price/qty string into a **0-value order**.) An
**undecodable payload** OR a validation failure resolves cleanly via `OnPlaceRejected` (request
FAILED, order + cycle FAILED, lock released) — it is NOT merely a request-only failure, so the
order/cycle/lock are never left stuck; nothing was placed, so there is no exposure. The sell
handler has the mirror `SellIntentPayload.Validate`; a bad/undecodable sell (which has existing
inventory) is instead marked request FAILED with order + cycle → NEEDS_RECONCILE, **lock
held** (never sent, never stranded).

**Empty `exchange_order_id` on ack (PR10 correction #6).** If `PlaceOrder` succeeds but the
ack carries no usable `ExchangeOrderID`, `OnPlaceAck` must NOT schedule a blind
`CANCEL_ORDER("")`/`GET_ORDER("")`. The order was placed (exposure exists) but is
untrackable by us, so the PLACE request is marked SUCCEEDED, order + cycle go to
**NEEDS_RECONCILE**, and the **symbol lock is held** for the reconciler to resolve. (Lookup/
cancel by `client_order_id` is not part of the current adapter contract, so we never rely on
an empty exchange id.)

**Full/partial fill needs a usable cost basis (PR10 correction #7).** A fill is only
classified Full/Partial (and a `fills` row written) when there is a usable average price:
the venue's `AvgPrice` if positive, else derived as **`ExecutedQuote / FilledQty`** when
both are positive (`usableAvgPrice`). A full fill with a positive filled qty but **no
`AvgPrice` and no `ExecutedQuote`** has no cost basis for accounting / the sell leg → it is
**Ambiguous → NEEDS_RECONCILE, lock held**, and **no fill row is recorded with a zero/invalid
price**. When `ExecutedQuote > 0`, the derived average is stored on the order and the fill.

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
treats the exact already-applied transition — target state at `expected_version + 1`
— as a replay no-op; see §9 strict replay). A transient (retryable) status-fetch
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

**Resting place + fill polling.** A sell `PLACE_ORDER` is routed by the executor by the
**DB order role** (`exit_sell` → sell handler; NEVER the untrusted `payload.side` — PR11
#2/§10b), and its payload is **validated before send** (`SellIntentPayload.Validate`:
`side=sell`, `order_type=limit`, `price>0`, `quantity>0`, non-empty client id — PR11 #3).
An undecodable/invalid sell is NOT sent: request `FAILED`, order+cycle `NEEDS_RECONCILE`,
**lock held** (inventory exists — conservative, never failed+released like a buy). A
**definite venue rejection of a sell** (insufficient balance, bad request, auth) is likewise
NOT the buy path: `OnSellPlaceRejected` marks the request `FAILED` and order+cycle
`NEEDS_RECONCILE` with the **lock HELD** — because the buy leg already acquired inventory that
may still be held, so the scope must not be freed (a buy rejection releases the lock only
because no inventory was acquired). **Executor as the final boundary:** before ANY sell
`CancelOrder`/`GetOrder` (and buy cancel/final-status) the executor refuses an empty
`exchange_order_id` — it never calls `CancelOrder("")`/`GetOrder("")`; the request is
terminal (`FAILED`/`DEAD`) and order+cycle go to `NEEDS_RECONCILE` with the **lock held**. A valid
sell goes to `OnSellPlaceAck`: order `→ACKED`, cycle `→SELL_SUBMITTED`, and — unlike the buy
IOC — **no auto-cancel** is scheduled (it rests). **If the sell ack has no usable
`ExchangeOrderID`** (PR11 #4), the resting sell is untrackable, so order+cycle go to
`NEEDS_RECONCILE` with the **lock held** — never a blind `GetOrder("")`/`CancelOrder("")`
(the `ensurePoll`/`RepriceSell` paths also refuse an empty id). The Manager keeps at most
one outstanding `sell_status` `GET_ORDER` poll per resting sell; the executor runs it
through `ProcessSellStatus`, which records fills and drives the cycle on the **cumulative**
sold-vs-bought quantity:
- partial (resting) → order `PARTIALLY_FILLED`, cycle `SELL_PARTIALLY_FILLED` (keep
  managing the remainder — never sell more than the held inventory);
- full (cumulative sold ≥ bought) → close (below);
- a fill with **no usable cost basis** (neither `AvgPrice` nor derivable `ExecutedQuote` —
  PR11 #5), or an ambiguous / fetch error / missing order → order+cycle `NEEDS_RECONCILE`
  (lock held); no fill row is recorded with a zero/invalid price. When `ExecutedQuote>0`
  the average is derived (`ExecutedQuote / FilledQty`).

**Repricing (cancel → replace, interval-gated).** When the reference moves, the
Manager reprices — but only when `reprice_interval_seconds` has elapsed
(`last_reprice_at`), and **never** while a sell place/cancel is `CLAIMED`/`IN_FLIGHT`.
`RepriceSell` cancels the resting sell (cycle `→SELL_REPRICE_PENDING`, order
`→CANCEL_PENDING`, enqueue a `sell_reprice` `CANCEL_ORDER`) — but if the resting sell's
`exchange_order_id` is **empty** it does NOT enqueue a blind `CANCEL_ORDER("")`; instead
order+cycle go to `NEEDS_RECONCILE`, **lock held** (PR11 #7). `OnSellCancelResult`
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
stored raw, not silently folded in). **A cycle is closed ONLY when the close accounting
is complete and valid** (PR11 #6): `closeCycleWithPnL`/`writeCloseAccounting` check every
query error (no ignored `_ = Scan(...)`) and require positive buy filled qty + buy cost
basis, positive sell filled qty + sell proceeds, and sold-vs-bought within tolerance
(`ErrIncompleteCloseAccounting`). If the accounting is missing/zero/inconsistent, the
automatic path does NOT close/PnL — it diverts the cycle to `NEEDS_RECONCILE` with the
**lock held** (the operator path returns the error). Everything (queue status, order/cycle
transitions, fills, lock release) commits in one transaction; repeated processing is
idempotent.

**What PR11 does NOT do (→ later):** steady-state WebSocket order updates (polling is
the supported path); per-venue individual fills (one aggregate fill row per status);
multi-leg / cross-asset fee conversion for `realized_quote`; the operator exit from
`NEEDS_RECONCILE`.

## 10d. Ambiguous mutating-outcome handling & order-result reading (order-lifecycle safety)

The whole point of the state machine + queue + reconciler is to survive **ambiguous
exchange outcomes**. Iranian venues have **no native IOC**, so the buy is a *simulated
IOC* (place → wait → cancel → read final status), and **a fill can materialise after our
HTTP call returns or times out**. The cardinal rule everywhere:

> **`PlaceOrder` / `CancelOrder` are AMBIGUOUS unless the exchange response is definitive.
> An ambiguous mutating call is NEVER blindly retried; the symbol lock stays HELD while
> ambiguity exists; reconciliation owns the final decision.**

**Outcome classification (`executor.isDefiniteRejection`, conservative).** Only clear
client-side rejections are "definite": `ErrInsufficientBalance`, `ErrAuthFailed`, or a
normalized `CatBadRequest`/`CatAuth`/`CatInsufficientBalance`. **Timeout, network reset,
5xx, and any unknown error are AMBIGUOUS** (not "the order was not created").

**Ambiguous `PlaceOrder`.** `handlePlace`/`handleSellPlace`: on a definite rejection →
clean fail (buy: `OnPlaceRejected` = FAILED + lock released; sell: FAILED + order/cycle
NEEDS_RECONCILE, lock held). On an **ambiguous** error → `deadReconcile`: the request is
`DEAD`, the order **and** cycle go to `NEEDS_RECONCILE`, the lock is **held**, and the
mutating request is **never re-sent**. The order may exist on the venue even though our
request failed — so before any *future* placement the **reconciler** looks it up (below),
never a blind re-`PlaceOrder`. `MarkInFlight` is committed **before** the send, so a crash
mid-send leaves a recoverable `IN_FLIGHT` marker.

**Ambiguous `CancelOrder`.** `handleCancel`/`handleSellCancel`: a clean cancel **or** a
definite rejection (e.g. "already gone/filled") both resolve via the **authoritative final
`GET_ORDER`** (the cancel may have raced a fill — we never assume terminal from a cancel
ack). Only an **ambiguous** cancel (timeout/network) → `deadReconcile` → `NEEDS_RECONCILE`,
lock held; the reconciler then reads the real status. (This mirrors the reference system,
where cancel returns 200/success *before* the order leaves "open", so a status re-read is
mandatory.)

**No blind retry of mutating requests (queue).** `SweepStuck`: a stale **`CLAIMED`**
request (claimed but never `IN_FLIGHT` → never sent) is safely re-queued even if mutating;
a stale **`IN_FLIGHT`** read-only request is rescheduled; a stale **`IN_FLIGHT` mutating**
request is `deadMutatingStuck` = `DEAD` + order `NEEDS_RECONCILE`, **never re-sent**.
`ScheduleRetry` refuses a mutating request outright (dead-letters it) as defense-in-depth.

**Reconciliation owns the final decision — lookup by exchange id, then client id, never
re-send (`internal/reconciler`).** For an order with ambiguity, the reconciler fetches the
real state read-only:
- `GetOrder(exchange_order_id)` when it is known;
- else, **for venues that accept a client order id** (`Capabilities.ClientOrderID &&
  FetchByOrderID`), `GetOrder(local_client_order_id)` — **lookup by `client_order_id`** —
  and it back-fills the discovered `exchange_order_id`;
- else (cannot positively identify) → `NEEDS_RECONCILE`, **not** a re-send.
Its decision table is conservative: open/partial → *Continue* (resume from persisted state,
idempotent via the queue); filled-and-clean → advance; canceled/expired with **zero** fill
→ terminal; **any fill it cannot fully account for, a missing order, or a fetch error →
`NEEDS_RECONCILE` with the lock held** (a missing order is **never** proof of zero fill).
The reconciler holds a `ReadOnlyClient` that structurally lacks `PlaceOrder`/`CancelOrder`,
so it can never mutate.

**Order-result reading is capability-aware: private WS *or* REST polling → one normalized
path.** `exchanges.Capabilities.OrderUpdatesWS` declares whether a venue has a private
order-update WebSocket. `execution.NormalizedOrderEvent` (built by `EventFromStatus` for a
REST `GetOrder` result and `EventFromAck` for a place ack) is the **single normalized shape**
both a WS stream and REST polling converge on, so the same fill/status processing
(`ProcessFinalStatus`/`ProcessSellStatus`) serves either source. **Every Iranian adapter in
this system is REST-poll-only today** (`SubscribeOrderUpdates` returns `Unsupported`;
`OrderUpdatesWS=false`) — order status comes from the scheduled `GET_ORDER` follow-ups —
so no WS consumer is wired (wiring a dead stream would be misleading). The reference system
shows the intended split for venues that *do* have it: **Nobitex & Bitpin** race a private
order WS (Centrifugo) against REST `GetOrder` and **fall back to polling** on WS timeout;
**Wallex is REST-only**. When such a venue is added here, its adapter sets `OrderUpdatesWS`
and a small consumer feeds `SubscribeOrderUpdates` → `NormalizedOrderEvent` into the *same*
processing path — REST polling remains the always-available fallback; the system never relies
on WS alone.

**Wallet snapshots, settlement lag & the inventory source of truth.** Matched quantity comes
from **order status + recorded fills**, never a wallet snapshot: the sell sizes and closes on
`orders.filled_quantity` (`bought − sold`), and PnL from `quote_spent`/proceeds — so a wallet
balance that lags after a cancel/partial fill can never mis-size the next leg. Wallet
snapshots are **confirmation**, not the primary source: `balance-sync` (§11a) periodically
records `wallet_balances_current` + `wallet_balance_history` (per `exchange_id`, timestamped,
hash-deduped), and the operator-reconcile tool runs an **advisory balance cross-check**
(exchange-reported base balance vs the resolution's implied held quantity). Because all
venues report balances over REST only (no balance WebSocket — same as the reference), a
snapshot can trail the order state briefly; the lock stays **held** through the sell, so the
system never releases a scope on unsettled inventory. (The reference additionally gates each
live leg on a ~5s balance-freshness window and refreshes balances after each trade — a
follow-up here.)

**Rate-limit-aware, per-exchange polling.** `balance-sync` polls **each exchange on its own
cadence** (`Config.IntervalFor(code)`, floored by `Config.MinInterval` so a misconfig can
never over-poll); an exchange not yet due is skipped, so a rate-limited venue is polled less
often without starving the others. Adapter-level rate limits surface as
`execution.ErrRateLimited`/`CatRateLimit`, which the executor treats as retryable-with-backoff
for read-only calls and never as a definite outcome for a mutating call. (The reference uses
per-endpoint token spacing for Nobitex/Wallex and a shared 60-req/min budget for Bitpin, and
honors server-issued 429 back-off — the model this system's per-exchange cadence + rate-limit
classification is built to accommodate.)

**Lock behavior summary (while an outcome or settlement is unknown).**

| Situation | Order/cycle | Symbol lock |
|---|---|---|
| Definite `PlaceOrder` success | proceed to scheduled next step | held |
| Definite buy rejection (never placed) | FAILED (`OnPlaceRejected`) | **released** (no exposure) |
| Definite **sell** rejection (`OnSellPlaceRejected`) / bad sell payload | request FAILED, order/cycle NEEDS_RECONCILE | **held** (inventory unresolved) |
| Sell follow-up (`CANCEL_ORDER`/`GET_ORDER`) with empty `exchange_order_id` | not sent; request FAILED, order/cycle NEEDS_RECONCILE | **held** |
| **Ambiguous** `PlaceOrder` (timeout/network) | request DEAD, order+cycle NEEDS_RECONCILE, never re-sent | **held** |
| **Ambiguous** `CancelOrder` | NEEDS_RECONCILE (reconciler reads real status) | **held** |
| Cancel clean/definite | resolve via authoritative `GET_ORDER` | per final status |
| Proven zero fill after cancel | CANCELLED | **released** |
| Partial / full fill | continue / close on valid cost basis | **held** until full exit closes |
| Fill with no usable cost basis / missing order | NEEDS_RECONCILE | **held** |

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
  from `CANCEL_PENDING`, the cancel we requested); `EXPIRED`(zero fill)→advance;
  **`REJECTED`→`NEEDS_RECONCILE` (PR12 #2)** — a rejection is an **execution anomaly, NOT a
  clean zero-fill cancel**; it must never advance to a terminal state that could safe-close
  the cycle / release the lock (for a sell the buy leg may hold inventory; for a buy a silent
  clean-close would hide the rejection), so reconciliation/operator decides the side-appropriate
  next action; `OPEN`/`NEW`/`PARTIALLY_FILLED`→Continue (still working);
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
**Terminal-order classification before safe-close (PR12 #2).** A terminal order is
only a *clean* zero-fill if it is `CANCELLED`/`EXPIRED` with zero fill — an
**already-stored `REJECTED` or `FAILED`** order (even with zero fill) is an execution
anomaly and forces the cycle to `NEEDS_RECONCILE` (lock held), never a safe-close.
This covers a `REJECTED` order persisted in the DB from a prior run — not just a
`REJECTED` status observed live — and is especially important for a sell (the buy leg
may hold inventory).

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
zero fills); "missing from open orders" alone is never such proof. **Two additional
guards inside the safe-close transaction (PR12):**
- **No active exchange_request (#3):** `safeClose` re-reads, in the SAME tx, the count of
  `exchange_requests` for the cycle in `QUEUED`/`CLAIMED`/`IN_FLIGHT`/`RETRY_SCHEDULED`; if any
  is active it does **not** close and does **not** release the lock (the executor could still
  send that request) — the cycle is flagged `NEEDS_RECONCILE` with a clear logged reason. This
  is atomic with the close, so a request enqueued concurrently cannot slip past.
- **Illegal transitions are never silently skipped (#4):** `applyOrderOutcome` never returns
  "success" for a transition the state machine forbids — it **diverts the order to
  `NEEDS_RECONCILE`** (and the cycle follows), and an apply/tx failure is logged and treated as
  needs-reconcile. So the reconciliation report never claims an advance that did not happen.

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

**Per-exchange, rate-limit-aware cadence (PR11 lifecycle-safety follow-up).** Each
exchange is polled on **its own interval** — `Config.IntervalFor(code)` (0 → the default
`Interval`), floored by `Config.MinInterval` so a misconfiguration can never over-poll. The
run loop wakes at the finest effective interval and **skips any exchange not yet due**
(tracked per exchange), so a rate-limited venue is polled *less often* without slowing the
others (mirrors the reference's per-exchange `balance_poll_interval` + skip-not-due). All
venues are REST-only (no balance WebSocket). These snapshots feed reconciliation and the
advisory balance cross-check (§10d) — they are inventory **confirmation**, never the primary
matched-quantity source (order fills are).

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
- **Bearer/JWT tokens masked (PR4 correction):** the sensitive-key set also covers
  `access`, `refresh`, `accessToken`, `refreshToken`, `jwt`, `bearer` — so Bitpin's auth
  response `{"access":…,"refresh":…}` and equivalent token fields never reach
  `api_call_logs.response_body` in the clear.
- **Error text is masked too (PR4 correction):** the IO logger routes a transport error
  through `MaskErrorText` before storing it in `api_call_logs.error` — Go's `*url.Error`
  embeds the full request URL (with signed query params), and net errors can carry
  `key=value` secrets, so a raw `err.Error()` is never stored.
- **No money value passes through `float64` (PR4 correction):** Exir order-book
  prices/quantities are decoded as `json.Number` and parsed with `decimal.NewFromString`
  (never `NewFromFloat`); Rial→Toman multipliers are exact `decimal.RequireFromString("0.1")`
  literals. Price/quantity/balance/fee are decimal end-to-end.
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
- **Fees scoped per exchange (PR6 correction)** — defaults live in
  `DefaultFeesByExchangeID` (keyed by `exchange_id`) and market-specific overrides in
  `FeesByMarketID` (keyed by `exchange_market_id`). A single map keyed by
  `exchange_market_id` with `0`="default" was unsafe: every exchange's default
  collided at key `0` (last write wins), so profit/decision math could use the wrong
  venue's fee. `Snapshot.FeeFor(exchangeID, exchangeMarketID)` resolves a market
  override first, then THIS exchange's default — **never** another exchange's default.
- **Single active version enforced (PR6 correction)** — `ActiveVersion` no longer does
  `ORDER BY id DESC LIMIT 1`. Zero active → `ErrNoActiveVersion`; exactly one → its id;
  more than one → `ErrMultipleActiveVersions` (the system refuses to silently pick one,
  since that could run trading on the wrong config). The activation path keeps the
  invariant (supersede-then-insert in one tx).
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
  **Write-path validation happens BEFORE the transaction (PR6 correction):** an
  invalid value (e.g. a negative `min_spread_bps`) is rejected up front, so it
  activates no version, mutates no `symbol_config`, and writes no audit row.
- **Validation** (rule #6) — `ValidateMarket`/`ValidateExchange`/`ValidateSnapshot`
  check value sanity (non-negative spreads/intervals/retries, positive timeouts/
  concurrency, positive `buy_size` when trading is enabled, valid `buy_size_unit`)
  and the **enable-flag hierarchy** `trading ⊆ signal ⊆ collection` (so a symbol
  can't be half-enabled by incomplete config). Referential integrity is enforced
  by schema FKs.

The dashboard EDIT forms (creating versions/audit via this layer) are PR17; the
trade-engine consuming the cache is PR8. Each cycle stores the `config_version`
(and regime config version) used when its signal was created.

## 14. Dashboard (read-only views implemented in PR16 — `internal/dashboard`; config editing is PR17)

The `dashboard` binary serves the operator views: an HTTP read API for the initial
load plus a WebSocket for live updates. **PR16 is strictly read-only.** It runs as its
own binary (restarting it never affects collector/trade-engine/order-executor/
reconciler/balance-sync/health-monitor/regime), and it **cannot trade by
construction**: the `Server` holds **only** a `*sql.DB` (no exchange client, no queue;
a reflection test asserts this), and **every route is GET-only** — any POST/PUT/DELETE/
PATCH (e.g. an attempt to edit config) is `405`, because there is no mutating route at
all. It never places/cancels orders, creates cycles/orders, mutates the queue, or
edits config.

**Endpoints (all GET, read-only SELECTs, JSON):** `/api/cycles/open`,
`/api/cycles/closed`, `/api/cycles/{id}` (detail), `/api/orders`, `/api/fills`,
`/api/requests`, `/api/signals`, `/api/comparisons`, `/api/balances`, `/api/health`,
`/api/regime`, `/api/logs` (app_logs / reconciler decisions), `/api/api-logs` (masked),
`/api/config` (read-only snapshot), `/ws` (live), `/` (index), `/healthz`. List
endpoints take `?limit=` (defaulted + hard-capped). Missing data returns an empty
array (never a panic).

**WebSocket live updates.** `/ws` pushes a SAFE periodic snapshot (open cycles, health,
regime, balances) every `WSInterval`; it only SELECTs and sends — it takes **no**
commands from the socket (live updates never control trading). The loop ends on client
disconnect or server shutdown.

**No secrets.** The dashboard never reads the credentials table. The `api-logs` view
re-masks (defence in depth) the already-masked-at-storage headers/bodies/url — the
values of sensitive keys (authorization/api-key/secret/signature/token/…) are redacted
so no secret reaches the browser.

**Cycle detail (`/api/cycles/{id}`)** composes the cycle row + its orders, fills,
exchange requests, cycle/order state events, symbol locks, and related app_logs — so it
shows signal context (time, prices, spread, fee-adjusted spread, config version), the
buy/sell orders + fills + fees + realized quote + lock status, and the queue requests.

**Maker/taker visibility.** The order rows carry `intended_execution_mode` /
`actual_execution_mode` / `maker_attempt_number` / `maker_offset_bps` / `fill_result`,
and the requests show the simulated-IOC place→cancel→status path (request types +
purposes).

**Queue display rule.** The `requests` view adds a computed `step_kind` that
disambiguates `RETRY_SCHEDULED`: `retry_count == 0` → `scheduled_next_step` (a planned
simulated-IOC/reprice step), `retry_count > 0` → `retry` (an actual retry) — so a
planned step is never shown as a failed retry.

**Fee display.** Cycle detail includes a `fee_note` stating that `realized_quote`
includes quote-denominated fees only; fees paid in another asset are shown raw and not
netted.

**Health display.** `/api/health` shows `public_status` / `private_status` /
`api_key_status` / `ws_status`, last success/failure, `consecutive_failures`, latency,
`last_error_category`, and the safe `last_error_message`.

**Balance display.** `/api/balances` shows available/locked/total, `last_seen_at`, and
a computed `stale` flag (last_seen_at older than the configured age) — a missing asset
keeps its last value and is simply flagged stale, never shown as zero.

**Regime display.** `/api/regime` shows direction, level, confidence, basket score,
per-timeframe scores, per-symbol contributions, stale_reason, and config version
(history via `/api/...` history queries later).

**Auth.** An auth layer is a documented placeholder (the WS upgrader currently accepts
any origin for local operator use); real auth lands with the config-editing PR.

## 14a. Dashboard config editing (implemented in PR17 — `internal/dashboard` + `internal/configstore`/`internal/regime` admin)

PR17 adds **authenticated, authorized, versioned, audited, validated** editing of the
DB-backed operational config. It still **does not trade** — there is no place/cancel/
cycle/order/queue/credential mutation route; only config rows change, through the
versioned-write path.

**Authentication (required before any mutation).** Every mutating route is wrapped by
`requireConfigOperator`: it needs an `Authorization: Bearer <token>` whose **SHA-256
hash** matches an enabled `dashboard_tokens` row (migration 017 — only the hash is
stored, never the plaintext). No token / bad token → **401**. Read views stay open
(local, read-only). Tokens are provisioned out-of-band (an admin inserts a hashed
token); a token-management UI is later.

**Authorization (roles).** `dashboard_tokens.role` ∈ `viewer` | `config_operator` |
`credential_operator` | `admin`. Config editing requires `config_operator` or `admin`;
a `viewer` is **403**. Credential and emergency-operator roles exist for later tooling
— no dashboard user is treated as fully trusted.

**Versioned write flow (no silent changes).** Each edit runs in ONE transaction:
activate a new `config_version` (superseding the prior active one), update only the
provided fields, and write a `config_change_audit` row **per changed field**
(config_version, entity_type, entity_id, field, old_value, new_value, `changed_by` =
the **authenticated** operator, reason from the request, timestamp). The new
`config_version` is stamped on the edited row where the table has the column
(symbol_configs/exchange_configs/regime baskets); `exchange_markets` has no such column
so the version lives on `config_versions` + the audit row. An edit with no real change
→ 400 (`ErrNoChanges`).

**Validation before write (→ 400).** `min_spread_bps ≥ 0`; `buy_size > 0`;
`buy_size_unit ∈ {base,quote}`; `sell_offset_bps ≥ 0`; `reprice_interval_seconds ≥ 0`;
`order_timeout_ms > 0`; retries ≥ 0; `maker_attempts_before_taker ≥ 0`;
`maker_signal_window_seconds > 0` (esp. when maker-first is enabled);
`maker_wait_before_cancel_ms ≥ 0`; `max_taker_slippage_bps ≥ 0`; `taker_price_mode =
ASK`; exchange `max_concurrent_requests > 0`, `request_timeout_ms > 0`; fees
non-negative; regime thresholds ordered `0 ≤ neutral ≤ moderate ≤ strong`; basket
update interval > 0; symbol/timeframe weights > 0.

**Enable-flag hierarchy (enforced).** `enabled_for_trading ⊆ enabled_for_signal ⊆
enabled_for_collection` — enabling trading without signal, or signal without
collection, is rejected (400). `enabled_for_sell_manage` is **independent** so existing
cycles keep being managed even when new-cycle trading is turned off (disabling trading
never stops sell management of open cycles).

**Editable surfaces.** Symbol config (`POST /api/config/symbol/{em}`) — spread/size/
sell-offset/reprice/maker-taker/IOC-wait/slippage/timeouts; market flags (`/market/{em}/
flags`); exchange config (`/exchange/{ex}`) — concurrency/timeout/retries/backoff/rate;
fees (`/fee`) — maker/taker per exchange or market default; regime baskets
(`/regime/basket/{id}` + `/symbol` + `/timeframe`) — name/enabled/thresholds/interval/
symbols+weights/timeframes+weights. Audit history at `GET /api/audit`.

**Credential editing — DEFERRED to a dedicated PR.** It is the riskiest surface
(encryption, key versioning, never-return/never-log plaintext), so PR17 ships **no**
credential mutation route (the auth boundary trivially covers "no unauthenticated
credential editing" — there is none). The `credential_operator` role is reserved for it.

**Hot reload (no restart for normal edits).** Services pick edits up through their
periodic reloads: the trade-engine's `configstore.Cache` reloads on its interval, and
the regime calculator re-reads baskets each pass — so symbol/exchange/fee/flag/regime
edits apply without restarting any service. (No edited setting requires a restart.)

**Open-cycle safety.** Active cycles keep their **stamped `config_version`** and are
never rewritten; new cycles use the new active version. Current sell management reads
the live `MarketConfig` (so e.g. a `sell_offset_bps`/`reprice_interval` change affects
open cycles' repricing immediately) while the cycle retains its config_version stamp for
audit; per-cycle fully-stamped sell parameters are a documented future refinement.

**WebSocket origin.** `AllowedWSOrigins` config tightens the WS `Origin` check to an
allowlist (empty = permissive, only for local read-only use). The WS still carries **no
commands**, so it can't affect trading regardless; config editing is HTTP + token, not
WS.

## 15. Market regime (implemented in PR15 — `internal/regime`)

The regime calculator scores **DB-configurable baskets** of Binance symbols into a
normalized market regime. It reads Binance prices **only from the Redis market-data
cache** (it never calls Binance — no REST/WS, no clients), reads basket config from
MariaDB, and writes regime current/history to MariaDB. It makes **no** trading
decisions and never touches cycles/orders/queue/locks/trading-config (a later
trade-engine PR may consume the regime; PR15 only computes + stores it). Migration 015
added the basket/symbol/timeframe/current/history tables.

**Config model (nothing hardcoded).** `market_regime_baskets` (name, enabled,
`update_interval_seconds`, `neutral_band_bps`/`moderate_threshold_bps`/
`strong_threshold_bps`, `config_version`) + `market_regime_basket_symbols`
(binance_symbol, weight, enabled) + `market_regime_timeframes` (label, seconds,
weight). Baskets are configured via DB (dashboard later); the calculator computes
nothing until a basket is configured.

**Calculation (multi-timeframe momentum; `regime.Calculate`, pure + unit-tested).**
The calculator samples each basket symbol's current Binance price from Redis into a
rolling per-symbol **time series** (deduped by the venue observation time). For each
timeframe T it finds the observation ~T ago and computes the symbol's bps change
`(current − refT)/refT × 10000`; these are weighted across symbols (per symbol weight)
→ per-timeframe score, then across timeframes (per timeframe weight) → the basket
**score (bps)**. `direction` ∈ `BULLISH/BEARISH/NEUTRAL/UNKNOWN` (NEUTRAL inside the
neutral band); `level` ∈ `STRONG/MODERATE/WEAK/FLAT/UNKNOWN` by |score| vs thresholds;
`confidence` = (fresh symbols / total) × (timeframes with data / total), 0..1. Output
also carries per-timeframe scores + per-symbol contributions + config version.

**Stale-data policy (never fabricate).** Redis is input only. A symbol whose freshest
sample is older than `MaxAge` (or missing) is **excluded** (lowering confidence); a
timeframe the series can't yet span is skipped. If **no** symbol has fresh data, or no
timeframe can be computed, the regime is **`UNKNOWN`** with a `stale_reason` — stale
data is never treated as valid. On a Redis miss/error the calculator records nothing
that tick (no crash, no fabricated momentum); the series ages out and the regime
degrades to `UNKNOWN`. After a restart the in-memory series is empty, so the regime is
`UNKNOWN` until it warms up to span the timeframes.

**Current + history.** `market_regime_current` is upserted per basket (idempotent for a
genuinely identical regime); `market_regime_history` gets a row whenever the regime
**changes**, where "changed" is a **full-field content hash** (`state_hash`, migration
016) over direction, level, confidence, score, per-timeframe scores, per-symbol
contributions, stale_reason, and config_version — **not** just the direction/level
label. So `BULLISH/STRONG @ confidence 0.35 → 0.90` records a new history row, and an
`UNKNOWN` whose `stale_reason` changes records one too (the dashboard sees the full
evolution, and a stale-data `UNKNOWN` is never shown as a still-valid regime). Both are
config-version-stamped; history is high-volume, timestamp-indexed, no FK on the write
path (retention friendly).

**Wiring.** The trade-engine hosts the calculator on its own cadence (it already has
the Redis client); the engine implements `regime.PriceSource` by reading
`price:{binance}:{symbol}` (mid when both sides present, else best bid) with the venue
observation time. Per-basket `update_interval_seconds` gates recomputation.

**Later (not PR15).** The trade-engine consuming the regime to influence configurable
parameters (accept/reject, size, spread, sell offset, repricing) and a per-cycle regime
snapshot at signal time are deferred — PR15 only computes and stores the regime.

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
- **Retention** (implemented in PR18 — `internal/retention` + `cmd/retention-worker`):
  see §16a.
- **Partitioning decision (PR2, MariaDB):** a partitioned InnoDB table requires
  every unique/primary key to include the partition column, which complicates the
  `AUTO_INCREMENT` PKs here. **PR2 therefore creates all high-volume tables
  NON-PARTITIONED** with a strong `created_at` index (and no foreign keys), so
  retention is a simple batched `DELETE ... WHERE created_at < ? LIMIT N`.
  Partitioning may be introduced later if volume demands it; because the
  `created_at` index already exists, that change is additive, not breaking.

## 16a. Retention worker (implemented in PR18 — `internal/retention`)

The `retention-worker` deletes OLD rows from high-volume OPERATIONAL tables only, in
bounded batches, driven entirely by DB config. It makes no exchange calls and uses no
Redis (holds only a DB handle).

**Never deletes permanent trading records.** Retention can only target a fixed
**whitelist** of high-volume tables, each mapped to its timestamp column:
`api_call_logs`, `comparison_events`, `exchange_health_samples`, `app_logs`,
`wallet_balance_history`, `market_regime_history` (all `created_at`). PERMANENT tables —
`cycles`, `orders`, `fills`, `signals`, `symbol_locks`, `exchange_requests` — are
**absent from the whitelist**, so they can **never** be deleted by retention even if a
`retention_settings` row names them (the worker iterates the whitelist, not the config).
A table is only eligible if its timestamp column is whitelisted (no time column → never
targeted), and these tables have no FKs (cheap deletes).

**Config-driven (nothing hardcoded).** Per-table `retention_settings`: `enabled`,
`retention_days`, plus `batch_size` / `max_batches_per_run` / `pause_ms` (migration
018). A missing row, or `retention_days <= 0`, means **not configured → do nothing**
(a deletion window is never guessed); `enabled=0` → skip.

**Batched deletes (never one huge delete).** Per table: `cutoff = now − retention_days`;
then `DELETE … WHERE <ts> < cutoff LIMIT batch_size` repeated up to
`max_batches_per_run`, stopping early on a short batch (no more old rows), with an
optional `pause_ms` between batches — avoiding long locks / replication pain.

**Dry-run.** A dry-run (the `-dry-run` command-line flag — an execution mode, never a
runtime env var) reports per table the cutoff, configured batch size, and **estimated
rows** (`COUNT(*) WHERE <ts> < cutoff`) and deletes **nothing**.

**Single-run advisory lock.** The whole run holds `GET_LOCK('v3tradebot_retention')` on
a pinned connection; if it can't be acquired (another worker is active) the run exits
cleanly (`LockAcquired=false`) and deletes nothing.

**Failure behaviour.** One table's error is recorded in its result and the run
continues to the others (or stops if `StopOnError`); batches are bounded (no infinite
retry), and a cancelled context stops the loop cleanly between batches/tables.

**Observability.** Every run writes a summary to `app_logs`
(`source_binary='retention-worker'`, no secrets): start/finish, duration, dry-run flag,
lock-acquired, and per-table {cutoff, configured/enabled, deleted count, batches,
estimated (dry-run), skipped reason, error}.

**Cadence.** The binary runs once on startup then every 6h; `RunOnce` self-guards with
the advisory lock so overlapping schedules across processes are safe. Dry-run is the
`-dry-run` flag (no runtime environment variables are used for service behaviour
anywhere in the system).

## 16b. Dry-run trading mode (implemented in PR19 — `internal/simexec`)

Dry-run runs the **full** trading lifecycle — signal → cycle → symbol lock → buy order
→ queue enqueue → executor claim → simulated buy fill → sell creation → simulated sell
status → cycle close → reconcile — through the **real** queue/executor/order-processing/
sellflow boundaries, but against a **simulated exchange client** so **no real
`PlaceOrder`/`CancelOrder` ever reaches an exchange** and there is zero real exposure.

**Activation (config-driven, safe by default — never a runtime env var).** The
bootstrap `[execution] mode` selects `off` (default) | `dry_run` | `live`:
- **`off` (default)**: the order-executor wires **no** clients and `AllowLiveExecution=
  false` → no order, real or simulated, can be sent. Dry-run/live must be **explicit**.
- **`dry_run`**: the order-executor wires `simexec` clients for the enabled exchanges and
  `AllowLiveExecution=true`; the trade-engine stamps created cycles `dry_run=1`.
- **`live`**: real private clients (needs decrypted credentials — a later PR; until then
  it falls back to safe `off` with a warning).

**Simulated client (`simexec`, no I/O).** It implements `exchanges.PrivateClient` with
**no network code at all**, recording each placed order so `GetOrder` returns a
consistent status per a configurable **scenario**: `full_fill`, `partial_fill` (half,
remainder cancelled), `zero_fill`, `ambiguous` (GetOrder unknown → NEEDS_RECONCILE),
`rejected` (definite place rejection), `place_timeout` (ambiguous ack), `cancel_race`
(cancel raced a full fill). The dry-run binary default is `full_fill`.

**Does not bypass the architecture.** Dry-run does **not** mark cycles closed from the
engine — every transition goes through the same queue → executor → `internal/orders`
boundaries (the simulated-IOC place→cancel→status flow for the buy, and the resting
sell place→poll for the sell). The engine only sets the `dry_run` marker.

**Dashboard labelling.** Cycles carry `dry_run`; the orders/requests/fills views surface
it via a join to the cycle, so the dashboard clearly shows `DRY_RUN`.

**Reconciler safety.** The reconciler loads `cycles.dry_run` (migration 019) and emits a
`dry_run_cycle` identification decision, so a simulated dry-run order is never confused
with a real exchange order; dry-run cycles are only ever verified against the simulated
client (or skipped when no client is wired).

## 16c. Limited live execution (implemented in PR20 — `internal/live`)

PR20 is a **safety PR**, not a wiring PR: it enables real live orders **only** under
strict, explicit caps + a global kill switch + per-exchange/per-symbol live flags +
credential availability + an audit trail, with the final gate **inside the
order-executor** (never relying on the engine alone). Safe by default at every layer.

**Split.** The real **credential decryption + real-adapter wiring is deferred to PR20a**
(a dedicated credential PR — encrypted-credential loading, in-memory decryption, key
versioning, masking). Until then live mode wires **no real client** and
`AllowLiveExecution` stays false, so nothing is sent. PR20 delivers and fully tests the
live **safety machinery** with a fake (no-network) client.

**Activation rules.** Bootstrap `[execution] mode` must be **explicitly** `live` (default
`off` is safe; `dry_run` keeps using `simexec`). No default live behaviour, no runtime
env var. Beyond the mode, live trading does **not start** unless the DB controls are
configured (see caps) and the kill switch is disengaged.

**Cap model (`live_controls` singleton + per-scope flags; migration 020).** The
`live_controls` row holds the global caps; **every cap is required** — if any is missing
(`Configured()` false) live is denied. Caps: `max_open_cycles`, `max_daily_orders`,
`max_daily_quote`, `max_order_notional`, `max_base_qty`, `max_consecutive_failures`,
`max_unresolved_reconcile`. Scope is opt-in via `exchanges.live_enabled` and
`exchange_markets.live_enabled` (both default 0). The kill switch (`live_controls.
kill_switch`) **defaults engaged (1)**.

**Kill switch.** When engaged: no new buy cycle may start (engine `AllowNewBuyCycle`
denies) and no new **buy** PLACE may be sent (executor gate denies). Risk-reducing paths
continue: **sell** PLACE (exiting existing inventory), CANCEL, and GET_ORDER/status are
still allowed so open cycles are safely managed.

**Executor-side live guard (the load-bearing gate).** The final live check is in
`order-executor`, immediately before each mutating send (`liveGatePlace` /
`liveGateCancel`). Before a real PLACE/CANCEL the `live.Guard` verifies: mode is `live`,
`AllowLiveExecution` true, request **not** dry-run, exchange + symbol live-enabled, caps
pass (notional/qty/open-cycles/daily-orders/daily-quote/consecutive-failures/
unresolved-reconcile), active credentials exist, kill switch off (for buys), and the
order/cycle state is still valid. A denial **fails the request without sending** and is
audited. The engine performs a first `AllowNewBuyCycle` check; the executor re-checks —
belt and suspenders.

**No blind resend (unchanged).** All prior safety holds in live: `MarkInFlight` commits
before the send; an ambiguous mutating result → order/cycle `NEEDS_RECONCILE`, request
`DEAD` (never re-sent); a missing order is not proof of zero fill; an `IN_FLIGHT`
mutating timeout is never blindly retried.

**First live phase is tiny.** The intended first rollout is one exchange, one symbol,
very small `max_order_notional`/`max_base_qty`, `max_open_cycles=1` — enforced purely by
the configured caps + the single `live_enabled` exchange/symbol; the dashboard keeps the
dry-run comparison and a live warning visible. Broad multi-exchange live is **not**
enabled here.

**Audit (`live_audit`; migration 020).** Every live mutating decision (allow or deny) is
persisted: exchange, symbol/market, cycle, order, request id, action/side, notional,
decision, reason, execution mode, config version, timestamp. No secrets.

**Dashboard live visibility (`GET /api/live`).** Read-only: execution mode (`LIVE`),
kill-switch state, the caps, live-enabled exchanges + symbols, today's order count + open
cycles (remaining allowance), credential **status only** (never key material),
unresolved-reconcile count, and the last live allow/deny from the audit.

**What remains after PR20.** PR20a — real credential decryption + real private-client
wiring (so live mode actually sends, gated by this same guard). Until PR20a, `live` mode
is a fully-tested safety harness with no real client.

## 16d. Credential decryption & real private-client wiring (PR20a — `internal/secrets`, `internal/credentials`)

PR20a makes `live` mode actually able to send by loading the encrypted exchange
credentials, decrypting them **only in memory**, and constructing real private clients
through the existing factory — all still gated by the unchanged PR20 `live.Guard`. It
weakens no PR20 guard.

**Encryption format (`internal/secrets`).** AES-256-GCM, stored layout
`nonce || ciphertext || tag` (12-byte GCM nonce prepended; 16-byte tag appended by
`Seal`). The AES-256 key is `SHA-256(master_key_bytes)`, so the operator's master-key
string need not be exactly 32 bytes. No second/incompatible format is introduced. The
only supported `encryption_algorithm` is `AES-256-GCM` (empty = that default); any other
value is treated as unusable. `Encrypt`/`Decrypt` are symmetric (Encrypt is used by tests
and future provisioning tooling).

**Master key handling.** The key comes from the bootstrap config file
(`[security] master_key`) — never a runtime env var. An **empty** master key →
`NewCipher`/`NewProvider` return `ErrNoMasterKey`, so every service disables credential
loading safely (no clients, no live execution, no panic). An invalid key simply fails to
decrypt → the credential is marked unusable. A decryption failure never panics a service.

**Credential loading (`credentials.Provider`, an `exchanges.CredentialProvider`).** For an
exchange code it selects the single clearly-chosen credential — `enabled=1 AND
status='active'`, **highest `key_version`** (then newest id) — and decrypts api_key /
api_secret / passphrase in memory. Disabled, non-active, and lower-version rows are
ignored. On unsupported algorithm or decryption failure it best-effort marks the row
`status='error'` with a **non-secret** note (`last_auth_error`) and returns an error that
carries no plaintext. Plaintext is never written back, never logged, never returned by the
dashboard, never placed in an error.

**Real private-client construction (`credentials.Builder`).** `BuildPrivate(code)` builds
through `exchanges.NewPrivateClient` (the factory) with the Provider injected as
`Creds`, and the per-exchange symbol map loaded from `exchange_markets`. It builds **only**
when an active credential exists; an unsupported exchange (no private adapter) yields no
client. No exchange-specific construction is hardcoded in the executor. Construction does
no network I/O (adapters connect lazily), so wiring at boot is safe.

**Executor integration.** In `live` mode the order-executor builds real clients for
live-enabled exchanges that have an active credential and sets `AllowLiveExecution=true`;
the PR20 `live.Guard` still runs immediately before every PLACE/CANCEL (mode/
AllowLiveExecution/live-flags/caps/credentials/kill-switch/not-dry-run/state). A missing/
invalid master key → no clients, `AllowLiveExecution` stays false, nothing is sent.

**Read-only services (narrowed interfaces).** `balance-sync`, the `health-monitor` private
probe, and the `reconciler` build credentialed clients via the same `Builder` but hold
them through **narrowed** interfaces — `balance.BalanceClient` (Name+GetBalances),
`reconciler.ReadOnlyClient` (no Place/Cancel), `credentials.BalanceReader` (GetBalances
only). `PlaceOrder`/`CancelOrder` are unreachable from these services by construction
(compile-time + reflection guards).

**Credential validation (`Provider.Validate`).** The ONLY validation is a read-only
balance read via the `BalanceReader` interface — it can never place or cancel. Success
stamps `status='active'` + `last_checked_at`; failure stamps `status='invalid'` with a
non-secret note. The health private probe uses `Validate`, so private health reflects the
credential state without any mutating call.

**Dashboard.** `GET /api/credentials` (and the credential block in `GET /api/live`) expose
**status only** — exchange, label, status, enabled, key_version, encryption_algorithm,
last_checked_at, and the non-secret last_auth_error. They never select the encrypted blobs
and never decrypt: no API key, secret, token, passphrase, plaintext, or ciphertext.

**Failure behaviour.** A credential that fails to load/decrypt is marked unusable →
the guard's `credentialsAvailable` (status='active') then denies live sends for that
exchange; private health shows the failure; balance-sync/reconciler skip it. No cycle is
corrupted and no order is blindly retried (the PR7/PR10 conservative paths are unchanged).

**Rotation readiness.** `key_version` is stored and honoured (highest active selected).
Rotation = insert a higher-version active credential, then disable the old one; disabled/
old credentials are ignored. Exactly one active credential is selected deterministically.

**What remains after PR20a.** A credential **provisioning/rotation UI + an encrypt-and-
insert tool** (credentials are inserted out-of-band today; `secrets.Encrypt` is the
building block). The first real live rollout is still gated tiny by the PR20 caps. The
operator exit from `NEEDS_RECONCILE` remains a dedicated future PR.

## 16e. Operator resolution for NEEDS_RECONCILE (PR21 — `internal/opreconcile`)

`NEEDS_RECONCILE` is a holding state with **no automatic exit** (PR3). PR21 adds the ONLY
exit: an authenticated operator's explicit, validated, audited resolution. It is a LOCAL
tool — it contacts **no exchange** (the `opreconcile.Resolver` holds only a DB handle; a
reflection guard asserts no Place/Cancel surface), there is **no blind/one-click close**,
and every state change goes through `internal/state`.

**Operator-only state-machine exit.** `internal/state` gains `ApplyCycleResolution` /
`ApplyOrderResolution` — separate from the normal trading map so the engine/executor can
never exit `NEEDS_RECONCILE`. They require `From=NEEDS_RECONCILE` and a `To` in an explicit
whitelist (cycle: BUY_FILLED / BUY_PARTIALLY_FILLED / SELL_PARTIALLY_FILLED / SELL_FILLED /
CANCELLED / FAILED / CLOSED; order: FILLED / PARTIALLY_FILLED / CANCELLED / FAILED), with
the same version-guarded CAS + event-row write as any transition. An illegal target is
rejected (`ErrNotReconcileResolution`).

**Auth + roles.** List/detail/audit are read-only; **preview/apply require a
`reconcile_operator` or `admin` bearer token** (`requireReconcileOperator`: 401 no/invalid
token, 403 insufficient role). A viewer / config-only user cannot resolve. The
authenticated operator name is recorded in the audit (never taken from the request body).

**No blind resolution — preview then apply.** `GET /api/reconcile/{id}` shows the full
context an operator must see first: cycle, exchange + symbol, every order (local + exchange
order id + last state), fills, queue requests, state events, locks, recent logs, the
`NEEDS_RECONCILE` reason (last entering event), a balance snapshot for the base asset, prior
resolutions, and the available actions. `POST …/preview` returns the **exact** proposed
state changes + warnings and writes nothing; `POST …/apply` re-validates in one tx and
applies. A `reason` is mandatory on apply.

**Resolution actions.** `cancel_zero_exposure` (no exposure → cancel cycle+orders, release
lock), `attach_exchange_order_id` (set the id, no state change), `mark_buy_filled` (record
fill → BUY_FILLED, lock held — sell resumes), `mark_buy_zero_filled` (→ CANCELLED, release
lock), `mark_sell_filled` (record full-exit fill → CLOSED + PnL accounting, release lock),
`mark_sell_partially_filled` (→ SELL_PARTIALLY_FILLED, lock held), `mark_order_cancelled_zero_fill`
(cancel one order, cycle stays NEEDS_RECONCILE), `keep_needs_reconcile` (audit only),
`mark_failed` (see below). Each validates its required fields.

**`mark_failed` safety (terminal-state guard).** `FAILED` is terminal, and a FAILED cycle
with unresolved exposure can hide risk from automated management — so moving OUT of
NEEDS_RECONCILE via `mark_failed` is gated on exposure (a warning alone is not enough):
- **Proven zero exposure** (net = 0): allowed, but the preview warns to prefer
  `cancel_zero_exposure`; lock released.
- **Open/unknown exposure, no confirmation**: **REFUSED** — the cycle is kept in
  NEEDS_RECONCILE, the lock stays held, a warning is returned, and the refusal is still
  audited (`external_resolution_confirmed=0`). It does **not** move to FAILED.
- **Open/unknown exposure, forced**: only when the operator sets
  `external_resolution_confirmed=true` **and** supplies a strong `external_resolution_reason`
  (mandatory) confirming the exposure was handled outside the system → FAILED, lock released
  (so the symbol is not blocked forever), audited with `external_resolution_confirmed=1` and
  the external reason folded into the audit reason. The before snapshot records the open
  exposure.

**Lock safety.** A symbol lock is released **only** when the resolution proves no remaining
exposure (net inventory = 0) or a full safe exit — never because a button was clicked. A
`mark_failed` (or any action) with open exposure keeps the lock and surfaces a warning.

**Fill safety.** Operator-supplied fills validate side (matches the order role), positive
quantity + price, non-negative fee, a fee asset when a fee is given, and a fill id (for
dedup). They reject oversell (buy: filled+qty ≤ ordered; sell: total sold+qty ≤ bought) and
a duplicate fill id (the `(order_id, exchange_fill_id)` unique key), update the order's
cumulative `filled_quantity`/`avg_fill_price`/`quote_spent`/`fee`, and on a closing sell run
the shared PnL accounting (`orders.ResolveCloseFromReconcile`).

**Balance cross-check (advisory).** When `wallet_balances_current` has the base asset, the
preview/apply compares the resolution's implied exposure to the balance and adds a
**warning** if they materially disagree (>1%). It never auto-blocks.

**No exchange mutation.** PR21 places/cancels nothing. The only allowed exchange activity
elsewhere is the existing read-only balance/health/reconcile reads (PR20a); the resolution
tool itself does no exchange I/O.

**Audit (`reconcile_resolutions`; migration 021).** Every applied resolution writes an
immutable row: operator, timestamp, cycle id, order id, action, old/new cycle + order
states, reason, supplied fill JSON, before/after snapshots, and whether the lock was
released. Preview writes nothing.

**What remains after PR21.** A richer operator UI (this PR ships the JSON API + a static
action catalog), and optionally a read-only exchange status fetch button surfaced in the
detail view (the data path exists via PR20a read-only clients; PR21 does not wire it).

## 16f. Credential provisioning & rotation (PR22 — `internal/credentials.Provisioner`)

PR22 adds the operator path to **create, rotate, disable, and validate** exchange
credentials. Plaintext exists only in memory; only ciphertext is ever stored; every
operation is authorized + audited. It builds on PR20a's `internal/secrets` (same format)
and is consumed by PR20a's `Provider` unchanged.

**No plaintext storage.** `Provisioner.Create`/`Rotate` accept plaintext, encrypt each
secret field **immediately** with `secrets.Cipher` (AES-256-GCM, `nonce ‖ ciphertext ‖
tag`, key = SHA-256(master key) — identical to PR20a), and write **only** ciphertext
(VARBINARY) to `exchange_credentials`. Plaintext is never persisted, never logged (verified
by a log-capture test), never returned (the API responds with only the new id + status),
and never written to the audit. An empty secret field stores a NULL blob.

**Master key.** From the bootstrap config file only (`[security] master_key`) — never a
runtime env var. An empty key → `NewProvisioner` returns `ErrNoMasterKey`, so the dashboard
leaves the provisioner nil and the create/rotate/disable endpoints respond safe-disabled
(503). A wrong master key cannot decrypt later (PR20a `ErrDecrypt`), proven by test.

**Create.** `exchange_code`, `label`, `key_version` (default 1), `algorithm` (default +
only `AES-256-GCM`), `enabled`, `status` (validated enum, default `active`), `api_key`,
`api_secret`, optional `passphrase`, and a mandatory `reason`. A duplicate `(exchange,
label)` → 400. One row inserted + one audit row, in a transaction.

**Rotation (no ambiguity).** `Rotate` inserts a NEW active credential at `key_version =
(current max active)+1` (derived unique label `…#vN`) and, in the **same transaction**,
disables **every** previously-active credential for the exchange (`enabled=0,
status='disabled'`) — so exactly one active credential ever exists. Both the new credential
and each disabled one are audited. PR20a's `Provider` then deterministically resolves to the
new secret.

**Disable.** Sets `enabled=0, status='disabled'`, KEEPING the row (and its history) — secret
rows are not deleted by default. PR20a's `Provider` immediately ignores it (proven by test).
Audited.

**Validation (read-only).** `Provisioner.Validate` does a single read-only balance read via
the narrow `BalanceReader` interface (it CANNOT place/cancel — no such method exists),
records `status` (`active`/`invalid`) + `last_checked_at`, and audits the check. No mutating
exchange call is reachable.

**Authorization.** Create/rotate/disable require a `credential_operator` or `admin` bearer
token (`requireCredentialOperator`: 401/403). `viewer`, `config_operator`, and
`reconcile_operator` cannot edit credentials (separation of duties). The credential audit +
the PR20a `GET /api/credentials` (status only) are read-only.

**Dashboard never shows secrets.** `GET /api/credentials` shows exchange/label/status/
enabled/key_version/algorithm/last_checked/last_error only; `GET /api/credentials/audit`
shows the operation history (operator/action/old+new status/old+new key_version/reason).
Neither selects the encrypted blobs, key material, or plaintext.

**Audit (`credential_audit`; migration 022).** Each operation records exchange, credential
id, operator, action (`create`/`rotate_new`/`rotate_disable_old`/`disable`/`validate`),
old/new status, old/new key_version, reason, and timestamp — **no secret material**.

**What remains after PR22.** A bespoke credential UI (this PR ships the JSON API) and an
optional offline encrypt-and-insert CLI for first-token bootstrap (the same `Provisioner`
would back it). HSM/KMS-backed master keys remain future work.

## 16g. Live preflight & canary rollout (PR23 — `internal/preflight`)

PR23 adds a strict, read-only **readiness checklist** plus an explicit operator
**acknowledgement** that the live guard enforces — so live trading cannot start by accident
even when credentials, caps, and live controls all exist. Preflight does **not** replace
the PR20 executor guard; both are mandatory.

**Read-only checker (`preflight.Checker`).** Holds ONLY a DB handle (reflection guard: no
place/cancel/balance/order method) — it never contacts an exchange and never mutates trading
state. `GET /api/live/preflight` runs it for the target exchange/market (defaults to the
configured canary scope) and returns a `Report` with a per-check pass/fail/warn list, the
failing/warning names, overall `ready`, and a `config_hash`.

**Checks** (all DB-only, read-only): execution mode is live (the executor derives
AllowLiveExecution from it); kill-switch state known + disengaged; exchange + symbol
live-enabled; caps configured + sane (+ canary expects `max_open_cycles=1`); credential
exists/enabled/active/validated and validation **fresh** (within
`credential_validation_max_age_minutes`); private health ok (or a WARN accepted when
`health_required=0`); balance-sync data recent; market data fresh (a recent
`comparison_event` — which the engine writes only when **both** Binance + Iranian books are
fresh — so it covers Binance-reference + Iranian-market freshness); unresolved
NEEDS_RECONCILE within cap; no stuck IN_FLIGHT mutating request; no stale (expired ACTIVE)
symbol lock; no DEAD mutating request on a real cycle; a recent successful **dry-run**
(CLOSED) for the same exchange/symbol; the auth path (an enabled dashboard token exists);
and the audit path (`live_audit` present). Freshness windows live in `live_controls`
(migration 023; NULL → a safe built-in default).

**Preflight config hash + acknowledgement.** `ConfigHash` hashes the **config-relevant**
inputs (mode, caps, live flags, canary scope, credential identity, freshness windows,
ack requirement) — deliberately EXCLUDING ephemeral values (market freshness, balances, the
kill switch), so a market tick does not invalidate an ack but a config change does. `POST
/api/live/acknowledge` (admin only) re-runs preflight, refuses unless `ready` (HTTP 409 with
the failing checks), then records a `live_acknowledgements` row bound to the current
`config_hash` (deactivating any prior ack) with operator, exchange, symbol, credential id,
caps snapshot, and reason.

**Canary gate in the guard (the teeth).** When `live_controls.require_canary_ack=1`
(default), the PR20 guard's live-BUY path additionally requires ALL of: the order is within
the configured canary exchange/symbol scope; an ACTIVE acknowledgement exists whose
`preflight_hash` still equals the freshly-recomputed `ConfigHash`; the acknowledgement has
**not expired** (`canary_ack_max_age_minutes`); and a **dynamic re-check** of the
time-sensitive safety conditions passes. A missing ack, an out-of-scope market, a stale ack
(config changed), an expired ack, or any failing dynamic condition DENIES the buy (audited).
Combined with `max_open_cycles=1` and the single canary exchange/symbol, this is the
one-exchange / one-symbol / one-cycle / one-buy-at-a-time canary. Sells/cancels are
unaffected (risk-reducing).

**Why a config hash is not enough (the correction).** Some readiness conditions rot with
time even when config does not change. So beyond the config-hash match, the guard re-checks
— immediately before every live buy — the **dynamic** subset via `preflight.DynamicRecheck`:
credential validation still fresh, market data still fresh, balance data still fresh,
unresolved NEEDS_RECONCILE still within cap, no stuck IN_FLIGHT mutating request, no
dangerous (DEAD) queue state (the kill switch is re-checked earlier in the buy path, and the
ack-expiry handles staleness of the sign-off itself). The acknowledgement therefore ages out
(`canary_ack_max_age_minutes`, default 30) AND the live state is re-verified per buy — a
stale credential/market/balance, a new reconcile/stuck-request, or a re-engaged kill switch
all deny the next live buy. Risk-reducing sell/cancel/status paths are untouched.

**No exchange mutation.** Preflight reads DB state only; it places/cancels nothing and runs
no real exchange call. (The read-only credential validation that feeds the freshness check
is PR20a/PR22's balance read, recorded separately.)

**Dashboard visibility.** `GET /api/live/preflight` (status, failed checks, warnings,
credential-validation age, market/balance freshness, dry-run last success, unresolved
reconcile, kill switch) + `GET /api/live/acknowledgements` (canary ack status, read-only).

**What remains after PR23.** Wiring the canary scope/freshness windows through the config
dashboard UI (set via SQL/admin today), and an "accept this warning" workflow for the
health WARN. The first real live order is still gated by both the acknowledgement and the
PR20 guard.

## 16h. First real canary live-run instrumentation (PR24 — `internal/live` sessions)

PR24 makes the first real live order **observable, correlatable, and stoppable** — without
broadening scope. It is still one exchange / one symbol / one open cycle / tiny notional,
gated by the PR23 acknowledgement; PR24 adds an explicit **run session** the operator
starts and stops.

**Live run session (`live_run_sessions`; migration 024).** A session records operator,
exchange, symbol, credential id, the preflight hash + acknowledgement id it was started
under, a caps snapshot, status (`ACTIVE`/`STOPPED`), start/stop times + reasons, and the
first-order checklist. `StartSession` verifies — atomically before inserting — that the
scope is exactly the configured canary, that a current (hash-matching, non-expired)
acknowledgement exists, that the dynamic safety re-check still passes, and that no session
is already active. It contacts no exchange.

**The session is a buy gate (stop = immediate block).** The PR20/23 guard's live-BUY path
now also requires an **ACTIVE** session for the scope (added to `canaryAckOK`, after the
ack/expiry/dynamic checks). `StopSession` flips it to `STOPPED` (with stop time + reason),
so **new buys are blocked immediately**; risk-reducing **sell / cancel / status** paths
never reach the session gate and stay available. This is a per-run, audited complement to
the global kill switch.

**Start/stop/view endpoints.** `POST /api/live/session/start` and `…/stop` require **admin**;
start first re-runs preflight (must be `ready`, else 409) then `StartSession` (400 on
scope/ack/readiness problems, 409 if already active). `GET /api/live/session` (read-only)
shows the current session + order count, quote used, open-cycle count, last order, last
live deny, kill switch, execution mode, and the acknowledgement status (incl. `expired`).

**Live audit correlation.** Every `live_audit` row (allow or deny) is now tagged with the
active `live_session_id`, the `acknowledgement_id`, and the current `preflight_hash` for the
scope (migration 024 columns; populated best-effort by the guard's audit writer).

**First-order checklist.** Immediately before the FIRST real buy of a session is sent, the
executor calls `Guard.RecordFirstOrderChecklist`, which writes a one-time JSON snapshot onto
the session: mode, exchange, symbol, caps **remaining**, credential **status**,
acknowledgement status, session status, kill switch, and the request/order/cycle ids — **no
secrets** (credential status only). It is idempotent (only the first buy writes it).

**What remains after PR24.** A live operator console UI (this PR ships the JSON API), and the
actual first venue order. PR20a's real private-client wiring is **already complete** — the
remaining prerequisites for a real order are purely operational: a real master key
configured, a real encrypted credential provisioned + recently validated, live config
enabled, caps configured, the kill switch intentionally disengaged, preflight passing, an
active (non-expired) acknowledgement, an active canary session, and an operator watching the
dashboard/live audit. Broadening beyond the single canary scope is explicitly out of scope.

## 16i. Real canary execution: runbook & production hardening (PR25)

PR25 prepares the system for the **first real venue canary order** with an explicit operator
runbook, startup safety reporting, operator warnings, emergency-stop behavior, and a per-run
audit export. It does **not** broaden live scope (still one exchange / one symbol / one open
cycle / tiny notional, gated by preflight + acknowledgement + active session).

**Operator runbook (`RUNBOOK.md`).** Step-by-step for the first live run: provision a
credential → validate (read-only) → configure caps + canary scope → run a dry-run → run
preflight → acknowledge → disengage the kill switch → start the session → watch the first
order → stop the session → engage the kill switch → inspect/export the live audit → resolve
`NEEDS_RECONCILE`. Includes the emergency-stop behavior table by cycle state.

**Startup safety summary (`live.BuildSafetySummary`).** Each live-capable binary
(`order-executor`, `trade-engine`) logs a one-line, **secret-free** startup snapshot:
execution mode, live enabled, kill switch, canary exchange + symbol, caps configured,
credential **status**, active session, and **whether new live buys are currently allowed**
(the full guard verdict for the canary scope). Read-only; writes nothing.

**Recent dry-run precedes the first live buy.** `StartSession` now also requires a recent
successful dry-run (CLOSED) for the exchange/symbol (`preflight.RecentDryRunOK`), in addition
to the preflight readiness it already implied — so a live buy is never the first time that
exchange/symbol path runs. Visible in preflight (`recent_dry_run_success`) + warnings.

**Operator warnings (`GET /api/live/warnings`).** Read-only, severity-tagged warnings for
live-danger states: live mode enabled, kill switch disengaged, session active, first order
pending vs **sent**, unresolved `NEEDS_RECONCILE`, and stale balance / market / credential
validation.

**Emergency stop.** Stopping the session (scope-local) or engaging the kill switch (global)
blocks new buys **immediately** while risk-reducing **sell / cancel / status** paths keep
working (they never pass the canary buy gate). Neither recalls an already-sent request; the
executor/reconciler continue their conservative handling (ambiguous → `NEEDS_RECONCILE`,
never blind-resent). The per-cycle-state matrix is documented + tested.

**Live audit export (`GET /api/live/session/export`).** Read-only, **secret-free** bundle for
a session (the active one or `?session_id=`): the session, preflight hash, acknowledgement,
caps, requests, orders, allow **decisions**, **denials**, the first-order checklist, and the
stop reason — using the PR24 `live_audit` session correlation.

**What remains after PR25.** A bespoke live operator console UI (all of the above ship as
JSON APIs + a runbook), and the actual first venue order — which now needs only the
operational pre-reqs in `RUNBOOK.md` (real master key, provisioned + validated credential,
live config + caps, kill switch deliberately off, passing preflight, active non-expired
acknowledgement, active session, operator watching). Rule #3 keeps tests venue-free.

## 16j. Critical pre-deploy code audit (PR26 — `scripts/check-critical-invariants.sh`, `internal/audit`)

PR26 is a **read-only safety audit**, not a deployment: no real order is sent, no API key is
needed, no live scope is broadened. It verifies the system's hard invariants across every
safety layer, encodes them as enforced static checks, fills test gaps, and applies two small
hardening fixes. The audit was run partly via independent read-only sub-audits of the
highest-risk areas (queue/retry, the order-send/commit/crash path, decimal+masking).

**Result: no critical bug found.** Every audited invariant HOLDS — the queue claim SQL is
correctly parenthesized (`… AND (status='QUEUED' OR (status='RETRY_SCHEDULED' AND
next_retry_at<=NOW(6)))`, with request-type + enabled-exchange + concurrency filters);
`MarkInFlight` commits before any mutating send and the post-response update is one atomic
transaction; ambiguous PLACE/CANCEL never re-send and a missing order is never zero-fill;
stuck mutating `IN_FLIGHT` → `DEAD` + order `NEEDS_RECONCILE`; money/price/qty/fee use
`decimal` throughout; secrets are masked before logging (including bitpin's `secret_key`
body field, via the `secret` substring rule); and the mutating exchange surface is confined
to the order-executor (engine/reconciler/balance-sync/health/dashboard/preflight hold only
read-only or no clients).

**Static invariant checks (enforced, area 18).** `scripts/check-critical-invariants.sh`
(CI/manual) + `internal/audit/invariants_test.go` (runs in `go test ./...`) scan the source
and FAIL on: a runtime `V3_*` env var; `PlaceOrder`/`CancelOrder` outside the
executor/adapters; a direct `UPDATE cycles|orders SET … state` outside `internal/state`; a
dashboard reference to an encrypted credential blob column; a secret-named field passed to a
logger; or a read-only service main referencing `PrivateClient`. All currently pass.

**Hardening fixes applied (no bug, defense-in-depth + precision):**
1. `queue.ScheduleRetry` now refuses a MUTATING request: it dead-letters it (`DEAD`) and
   pushes its order to `NEEDS_RECONCILE` instead of ever rescheduling — so even a future
   caller mistake can never blindly re-send a place/cancel. (All four current callers pass
   read-only requests; the guard is belt-and-suspenders.)
2. `exir` order books are decoded as `json.Number` and parsed with `decimal.NewFromString`
   (was `[]float64` → `NewFromFloat`) so reference prices/quantities convert EXACTLY, never
   via a lossy float round-trip — matching the "no float for price" rule (exir is a
   public/reference-only adapter, not a canary venue).

**Focused tests added.** state/boundary static tests (above); `queue` mutating-retry guard;
`exchanges` mask coverage for every adapter secret field name (incl. `secret_key`);
`dashboard` `asInt` driver-type parsing (the PR25 `[]byte`/`float64` id bug — area 16). The
existing suites already cover the deeper invariants the audit re-verified (atomic create,
ambiguous→reconcile, classify matrix, oversell/duplicate-fill, lock-release-on-proof, live
guard deny matrix, ack/session gating, no-secrets).

**Explicit crash / rollback fault-injection tests (correction).** Two TEST-ONLY fault seams
were added — `executor.Config.faultAfterSend` (forces a post-send completion tx to roll back
*after* the fake client's send) and `opreconcile.Resolver.faultBeforeCommit` (forces an apply
rollback before commit) — both unexported (nil in production). The fake private client now
counts sends. Six venue-free recovery tests prove the conservative behavior: (1) crash after
commit before send → committed request recoverable, claimed + sent exactly once, no
duplicate; (2) crash after `MarkInFlight` before response → stuck mutating `IN_FLIGHT` →
sweeper `DEAD` + order `NEEDS_RECONCILE`, lock held, 0 sends; (3) `PlaceOrder` succeeds but
completion rolls back → request `IN_FLIGHT`, order/cycle unchanged, then sweeper `DEAD` +
`NEEDS_RECONCILE` with NO second send + lock held; (4) `CancelOrder` succeeds but completion
rolls back → not blindly retried, order `NEEDS_RECONCILE`, lock NOT released; (5) crash with a
reprice cancel `IN_FLIGHT` → no replacement sell created, order `NEEDS_RECONCILE`, no oversell,
lock held; (6) crash during operator-reconcile apply → full rollback, no partial state/audit,
lock not released, then a clean apply commits state+audit atomically.

**Remaining risks (documented, not blocking).** The crash-recovery paths are now explicitly
fault-injection-tested (above). The first real venue order remains an operator action gated by
the full PR20–PR25 stack; an actual end-to-end run against a live venue is out of scope for a
venue-free audit (rule #3).

## 16k. Deploy packaging & local/staging dry-run readiness (PR27 — `Dockerfile`, `docker-compose.yml`, `DEPLOY.md`)

PR27 packages the system for **local/staging** with **no live trading and no real
credentials**. It adds no trading behaviour; it is packaging + docs + safe-default
verification.

**Image + orchestration.** `Dockerfile` builds every binary (`collector`, `trade-engine`,
`order-executor`, `reconciler`, `balance-sync`, `health-monitor`, `dashboard`,
`retention-worker`, `migrate`) into one small static (distroless, non-root) image — **no
config or secret is baked in**. `docker-compose.yml` runs MariaDB 10.6 + Redis 7 (both
health-checked), a **one-shot `migrate`**, then the 8 services; each service waits on
`migrate` via `service_completed_successfully` and mounts `configs/config.toml` read-only.

**Production-like example config.** `configs/production.example.toml` — **placeholders
only**: no exchange API key, no plaintext credential, `master_key = ""`, and
`[execution] mode = "off"`. The DB password is a `CHANGE_ME` placeholder; hosts default to
the compose service names (`mariadb`/`redis`).

**Startup order (services never self-migrate).** DB → Redis → `migrate` → services → verify
dashboard (`/healthz`, `/api/live`) → verify public market data → verify a dry-run. Every
service calls `migrate.EnsureCurrent` at startup and **fails fast** if the schema is behind
the code (proven by `TestServicesRefusePendingMigrations`).

**Local/staging dry-run.** Set `[execution] mode = "dry_run"` → the executor wires
**simulated** clients (`internal/simexec`); the full `signal → cycle → buy → executor →
simulated fill → sell → close` lifecycle runs through the REAL boundaries with **zero real
exposure and no network** (`DEPLOY.md` §4; observable on the dashboard cycles/orders/fills/
logs). `scripts/local-dryrun-check.sh` is a read-only smoke check (healthz + safe mode +
no-secret responses).

**Safe defaults (verified by tests).** `[execution] mode` defaults `off`; the example never
sets `live`; `live_controls.kill_switch` defaults engaged (`TestKillSwitchDefaultsEngaged`);
no active session ⇒ live buys denied; empty `master_key` ⇒ credential loading + live
execution safe-disabled; the shipped example contains no credentials and redacts its DSN/
master key (`TestProductionExampleIsSafeAndSecretFree`). No-real-mutating-in-dry-run /
live-disabled-without-credentials / dashboard-no-secrets are already covered by the PR19–PR26
suites.

**What remains after PR27.** Production hardening of the image/orchestration (resource
limits, real secrets management, a TLS reverse proxy for the dashboard, optional k8s
manifests) and the operator-driven first real venue order (gated by the full PR20–PR25
stack). Rule #3 keeps automated tests venue-free.

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

- **No real venue order has executed yet; PR25 ships the runbook + hardening, not a console.**
  `RUNBOOK.md` + the startup safety summary + operator warnings + the per-session audit export
  are all in place, but the first real order is an operational action still pending its
  pre-reqs (real master key, provisioned+validated credential, deliberate kill-switch-off,
  passing preflight, active acknowledgement + session). The live operator console remains a
  JSON-API-only surface (no bespoke UI). The warnings/export read from DB state; market-data
  freshness is the `comparison_event` proxy. Rule #3 keeps tests venue-free.
- **The canary run session ships a JSON API + audit, not a live operator console.** PR24
  adds `POST /api/live/session/start|stop` (admin) + `GET /api/live/session` + the
  first-order checklist + live_audit correlation, but no bespoke UI. The actual first venue
  order still needs PR20a real-client wiring + a real master key + a started session, and no
  end-to-end live order has run (rule #3 keeps tests venue-free). Scope stays single-canary
  by design. The session view's counts are per-exchange/day approximations of the run.
- **Live preflight + canary ship a JSON API; the canary scope/freshness windows are set via
  SQL/admin, not a config UI yet.** PR23 delivers the read-only `GET /api/live/preflight`,
  `GET /api/live/acknowledgements`, and the admin `POST /api/live/acknowledge`, plus the
  guard-enforced acknowledgement gate. Operators still set `live_controls.canary_exchange_id`
  / `canary_market_id` / freshness windows / `require_canary_ack` directly (no dedicated
  editor). The private-health WARN is accepted automatically when `health_required=0`; a
  per-warning "accept" workflow is future work. Market-data freshness is a DB proxy (a recent
  `comparison_event`), so it reflects what the engine last computed rather than a live Redis
  read. No real live order has been executed end-to-end against a venue (rule #3).
- **Credential provisioning ships a JSON API, not a UI or CLI yet.** PR22 delivers
  create/rotate/disable/validate via the `Provisioner` + authenticated dashboard endpoints,
  but no bespoke credential UI and no offline encrypt-and-insert CLI (useful for first-token
  bootstrap; the same `Provisioner` would back it). Validation requires the caller to pass a
  read-only client (built by PR20a's `Builder`), so a one-click "validate" button that hits
  the live venue is not wired into the dashboard yet. The master key is still a single
  config-file value (no HSM/KMS, no per-key rotation of the master key itself).
- **Operator reconciliation ships a JSON API + static action catalog, not a rich UI.** PR21
  delivers the list/detail/preview/apply/audit endpoints (auth: `reconcile_operator`/`admin`)
  but no bespoke operator front-end. The detail view does not yet offer a live read-only
  exchange status-fetch button (the read-only client path exists via PR20a; wiring it into
  the detail view is a later refinement). The balance cross-check is a single-asset advisory
  heuristic (base asset, 1% tolerance), never a hard block. `mark_failed` with open exposure
  deliberately keeps the lock — the operator must first record/sell the inventory.
- **Live is wired but credentials are provisioned out-of-band.** PR20a builds real private
  clients (factory + in-memory decryption) gated by the unchanged PR20 guard, but there is
  **no provisioning/rotation UI or encrypt-and-insert CLI yet** — encrypted credential rows
  are inserted out-of-band (an operator encrypts with `secrets.Encrypt` under the master
  key and writes the `nonce||ciphertext||tag` blob + `key_version`). Real end-to-end live
  sending is unverified against a real venue (rule #3: no live calls in tests); the first
  rollout is kept tiny by the PR20 caps + a single `live_enabled` exchange/symbol. The
  AES key is `SHA-256(master_key)`; a future hardware-KMS/HSM-backed key path is not yet
  implemented.
- **The safety core (PR1–PR7 + PR12) is complete; PR8 adds signal detection but
  still nothing trades.** The trade-engine now writes `comparison_events`/`signals`
  and keeps a pending buy intent fresh, but it does **not** create cycles/orders or
  enqueue new requests (PR9), so the executor still has nothing to claim; the
  `order-executor` binary wires no real private clients (`AllowLiveExecution=false`);
  and the `reconciler` binary wires no read-only clients yet (credential decryption
  is a later PR), so it inspects DB state and safely skips unverifiable exchanges.
- **PR8 is signal-only: it neither creates NOR refreshes any buy request** — the
  no-duplicate "refresh the existing QUEUED buy" logic belongs to PR9's `buyflow`
  (`RefreshActiveCycleBuy`, transactional with the cycle/lock), gated behind
  `Config.PrepareBuyCycles` (off by default). The simulated-IOC execution parameters
  (wait/cancel) are not in the schema yet; the PR9 buy-intent payload carries only
  price/quantity/config context. `comparison_events` are written
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
- **Config editing (PR17) is operational config only; credential editing is deferred.**
  There is no credential mutation route yet (encryption/key-versioning/never-leak is a
  dedicated future PR; the `credential_operator` role is reserved). Dashboard tokens are
  provisioned out-of-band (an admin inserts a SHA-256 hash); a token-management UI and
  richer session handling are later. Read views remain open (local); the WS origin
  allowlist defaults to permissive for local use. Open-cycle sell management currently
  reads live config (the cycle keeps its config_version stamp); fully per-cycle-stamped
  sell parameters are a future refinement. The HTML index is minimal (JSON API + WS).
- **Regime is computed but not yet consumed.** PR15 calculates + stores the market
  regime; the trade-engine does not yet read it to adjust accept/reject/size/spread/
  reprice, and there is no per-cycle regime snapshot at signal time yet. Baskets are
  DB-configured (no UI until the dashboard PR); with no baskets configured the
  calculator does nothing. After a restart the rolling price series is empty, so the
  regime is `UNKNOWN` until it warms up to span the configured timeframes.
- **`health-monitor` private probe is now a read-only credentialed balance check**
  (PR20a, via `credentials.Validate`); with no/invalid master key or no active credential
  it stays public-only and `private_status` is `UNKNOWN`. WebSocket health is recorded if
  a WS probe is supplied but the binary wires none yet (WS is collector-side and partial).
  The collector keeps its own pre-PR14 health recorder; migrating it onto `health.Recorder`
  is a later cleanup.
- **`balance-sync` and `reconciler` now build credentialed read-only clients** (PR20a, via
  the factory + in-memory decryption, held through narrowed non-mutating interfaces). With
  no/invalid master key or no active credential they wire no clients and idle/inspect-only
  safely. Per-exchange timeout/concurrency are bounded; a failed read never wipes balances
  and a missing asset is never zeroed; the reconciler still never auto-sends/auto-cancels.
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
PR18 (retention), PR19 (dry-run), PR20 (limited-live safety layer), PR20a (credential
decryption + real private-client wiring), PR21 (operator resolution for `NEEDS_RECONCILE`),
PR22 (credential provisioning & rotation tooling), PR23 (live preflight & canary rollout
controls), PR24 (first real canary live-run instrumentation), PR25 (real canary execution runbook &
production hardening), PR26 (critical pre-deploy code audit & invariant verification),
PR27 (deploy packaging & local/staging dry-run readiness). **Remaining:** bespoke UIs for
credential provisioning + operator reconciliation + canary/preflight config + the live
operator console (all ship JSON APIs + audit + a runbook today), an optional offline
encrypt-and-insert CLI for first-token bootstrap, HSM/KMS-backed master keys, production
hardening of the image/orchestration (resource limits, secrets management, TLS proxy, k8s),
and the operator-driven first end-to-end live order against a venue (rule #3 keeps tests
venue-free).

## 19a. Decisions log

- **PR12 (correction, round 2) — already-stored REJECTED/FAILED orders block safe-close**: the
  live-status REJECTED fix (round 1) still let an order ALREADY persisted as `REJECTED` (zero
  fill) through — `reconcileCycle` skipped terminal orders, so it looked like a clean zero-fill
  terminal and the cycle safe-closed + released the lock. Now the terminal-order scan flags a
  stored `REJECTED` or `FAILED` order → cycle `NEEDS_RECONCILE`, lock held (never safe-close);
  clean `CANCELLED`/`EXPIRED` zero-fill terminals stay eligible (gated by the active-request
  check). Tests: TestStoredRejectedOrderNotSafeClosed (buy) + TestStoredSellRejectedNotSafeClosed.

- **PR12 (correction) — reconciler REJECTED, active-request safe-close guard, no silent illegal
  transitions**: three startup-reconciler holes. (#2) `decideKnownOrder` advanced a `REJECTED`
  order to a terminal state (`OrderRejected`), which let the cycle safe-close + release the lock
  — treating a rejection like a clean zero-fill cancel. REJECTED is now `NeedsReconcile` for both
  sides (an execution anomaly; a sell rejection must keep the lock because the buy leg may hold
  inventory; a buy must not silently clean-close and hide the rejection). (#3) `safeClose` now
  verifies, atomically inside the close tx, that no `exchange_request` is active
  (QUEUED/CLAIMED/IN_FLIGHT/RETRY_SCHEDULED) before closing/releasing the lock — else it refuses
  and the cycle → NEEDS_RECONCILE (the executor could still send that request). (#4)
  `applyOrderOutcome` no longer returns nil (silent success) on an illegal transition — it
  diverts the order to NEEDS_RECONCILE and surfaces apply failures, so the report never claims an
  advance that did not happen. Tests: decide REJECTED→needs-reconcile, gated
  reject-keeps-lock (buy+sell), active-request-blocks-safe-close, illegal-advance→reconcile,
  applyOrderOutcome-diverts-illegal. Cut from accepted PR11 (`1333f09`); PR6–PR11 preserved.

- **PR11 (correction) — sell rejection keeps the lock + executor empty-id boundary**: two
  sell-side holes. (1) A DEFINITE sell rejection reused the BUY rejection path
  (`OnPlaceRejected` → cycle FAILED + **lock released**), which is unsafe because the buy leg
  already holds inventory — a freed scope could start a new cycle over live exposure. New
  `OnSellPlaceRejected`: request FAILED, order+cycle NEEDS_RECONCILE, **lock HELD**;
  `handleSellPlace` uses it. (2) Made the executor the FINAL safety boundary against
  `CancelOrder("")`/`GetOrder("")`: `handleSellCancel`/`handleSellStatus` (and, defense-in-depth,
  the buy `handleCancel`/`handleFinalStatus`) refuse an empty `exchange_order_id` before any
  exchange call → request terminal, order+cycle NEEDS_RECONCILE, lock held. Tests:
  TestSellDefiniteRejectionKeepsLock, TestSellCancelEmptyExchangeOrderIDReconciles (cancelCount
  0), TestSellStatusEmptyExchangeOrderIDReconciles (getCount 0) — all assert lock ACTIVE.

- **PR11 (order-lifecycle safety) — ambiguous mutating outcomes, capability-aware order
  reads, per-exchange balance cadence**: audited the order lifecycle against the reference
  system (iranArb) and confirmed the safety core was already implemented + tested — ambiguous
  `PlaceOrder`/`CancelOrder` (timeout/network) → `deadReconcile` (DEAD + order/cycle
  NEEDS_RECONCILE, lock held, **never re-sent**); definite-rejection classification is
  conservative; the sweeper dead-letters stuck mutating requests and `ScheduleRetry` refuses
  them; the reconciler is read-only and looks orders up by `exchange_order_id` **then by
  `local_client_order_id`** (client-order-id lookup for capable venues) before ever
  contemplating a re-place, defaulting to NEEDS_RECONCILE on any doubt; order-result reading is
  capability-aware (`Capabilities.OrderUpdatesWS` + `NormalizedOrderEvent` unify WS and REST;
  all current venues are REST-poll-only so no dead WS consumer is wired); the sell sizes/closes
  on `orders.filled_quantity` (order fills are the matched-quantity source, wallet snapshots are
  confirmation). One concrete gap fixed: **balance-sync now polls per-exchange on its own
  rate-limit-aware cadence** (`Config.IntervalFor` + `MinInterval` floor + skip-not-yet-due),
  matching the reference's per-exchange `balance_poll_interval`; default-config behaviour is
  unchanged (all every `Interval`). Documented the whole model in a new §10d + updated §11a. No
  new mutating exchange path; executor remains the only sender.

- **PR11 (correction) — sell-side pre-send validation, empty-id safety, cost-basis + PnL
  guards**: brought the sell path to the buy path's safety level. Sell `PLACE_ORDER` routes by
  DB order role (PR10 already added `dispatchPlace`) and the sell payload is validated before
  MarkInFlight/PlaceOrder (`SellIntentPayload.Validate`); a bad/undecodable/wrong-side sell is
  request FAILED + order/cycle NEEDS_RECONCILE with the lock **held** (inventory exists — never
  failed+released like a buy). `OnSellPlaceAck` with an empty `ExchangeOrderID` → NEEDS_RECONCILE
  (no blind `GetOrder("")`/`CancelOrder("")`; `ensurePoll`/`RepriceSell` guard it too).
  `ProcessSellStatus` requires a usable cost basis (derive `ExecutedQuote/FilledQty`, else
  ambiguous → NEEDS_RECONCILE, no fill row with zero price). `closeCycleWithPnL`/
  `writeCloseAccounting` no longer ignore query errors (`_ = Scan(...)`) — they load with error
  checks and validate positive buy/sell qty+quote + a sold-vs-bought tolerance
  (`ErrIncompleteCloseAccounting`); the automatic close diverts to NEEDS_RECONCILE (lock held)
  rather than close with missing/invalid data, and the operator path returns the error. Fixed an
  opreconcile test seed that used a filled buy with zero `quote_spent` (unrealistic) to give it a
  real cost basis. No direct state assignment bypass; executor remains the only sender.

- **PR10 (correction, round 2) — decode-error clean rejection + DB-role dispatch**: two
  remaining safety holes. (1) An undecodable buy `PLACE_ORDER` payload only failed the request,
  leaving order QUEUED / cycle BUY_REQUEST_QUEUED / lock ACTIVE — a stuck inconsistent state.
  Now a decode error WITH order/cycle context goes through `OnPlaceRejected` (request+order+
  cycle FAILED, lock RELEASED) via the official state path; a request without order/cycle is a
  request-only failure. (2) The executor dispatched buy vs sell from `payload.side`
  (untrusted) — a buy order with a `side=sell` payload could bypass buy validation into the
  sell handler. Dispatch (`executor.dispatchPlace`) now uses the DB order **role** as the
  source of truth (`entry_buy`→buy, `exit_sell`→sell); the lying payload routes to the buy
  handler and is rejected by `Validate()`. `orders.PayloadSide` was removed to kill the unsafe
  pattern; the sell handler got its own `SellIntentPayload.Validate` (bad/undecodable sell →
  request FAILED + order/cycle NEEDS_RECONCILE, lock held, since inventory exists). New tests:
  malformed payload, wrong-side payload, `simulated_ioc=false`, wrong `order_type` — all assert
  `PlaceOrder` not called + clean cleanup. Pre-existing sell tests unaffected (sellflow emits
  valid payloads).

- **PR10 (correction) — pre-send validation, empty-id safety, cost-basis fills**: cut from
  accepted PR9; PR1–PR9 fixes preserved (FeeFor, PR7 queue guards, PR8 subscription reconnect
  + USDT/IRT fan-out, PR9 buyflow refresh guards — all green in the full sweep). (5)
  `BuyIntentPayload.Validate()` gates the buy BEFORE MarkInFlight/PlaceOrder so a
  malformed/zero-value price/qty (or bad side/type/non-IOC/empty client id) is never sent —
  `decimalOrZero` could otherwise ship a 0-value order; invalid → clean `OnPlaceRejected`
  (nothing placed, lock released). (6) A place ack with no usable `ExchangeOrderID` no longer
  schedules a blind `CancelOrder("")`/`GetOrder("")`; order+cycle → NEEDS_RECONCILE, lock
  held. (7) A full/partial fill requires a usable cost basis — `usableAvgPrice` derives it
  from `ExecutedQuote/FilledQty` when the venue omits `AvgPrice`; a full fill with neither is
  Ambiguous → NEEDS_RECONCILE (no fill recorded with a zero/invalid price). (8) The
  `RETRY_SCHEDULED` convention (`retry_count=0` planned step vs `>0` real retry) is documented
  (§8/§10b) and now explicitly tested for the scheduled cancel + final-status. Pre-existing
  executor tests were updated to seed complete (buyflow-shaped) payloads so the new validation
  is exercised, not tripped. No direct state assignment bypass; the executor remains the only
  sender; no runtime `V3_*` env.

- **PR9 (correction) — buyflow refresh consistency + guards, signal linking, validity**:
  cut from accepted PR8; `cmd/trade-engine` enables `PrepareBuyCycles: true` (PR9 owns buy
  prep; the engine library still defaults it false as the gate, and fees use
  `Snapshot.FeeFor`). Five fixes: (5) `RefreshActiveCycleBuy` now refreshes the full CYCLE
  signal snapshot — not just the order/request — so cycles/orders/exchange_requests describe
  the same current intent (no stale/misleading audit); (6) refresh only proceeds when the
  whole state is queued/active (lock ACTIVE + cycle BUY_REQUEST_QUEUED + order QUEUED +
  request QUEUED via `SELECT … FOR UPDATE`), and each guarded UPDATE re-asserts its state and
  checks `RowsAffected == 1`; (7) a non-positive price/quantity is rejected (create errors,
  refresh no-ops), and the tick/step/min market-rule boundary is documented (validated at
  send; a venue rejection is handled cleanly by the executor, never sent blindly); (8) the
  originating signal is linked to the created/refreshed cycle (`signals.cycle_id`) in the same
  tx. Retired the PR8-only "cmd must not enable PrepareBuyCycles" invariant (PR9 legitimately
  enables it); tightened static invariant #3 / `TestNoDirectStateUpdates` to flag state
  ASSIGNMENTS only, so the newly-required guarded WHERE-clause state checks are not
  false-positives. No engine mutating exchange call; order-executor remains the only sender.

- **PR8 (correction, round 3) — the trade-engine BINARY is signal-only too**: round 2 made
  the engine library default-safe but left `cmd/trade-engine` setting `PrepareBuyCycles: true`,
  so the real executable still called `buyflow`. The binary now sets `PrepareBuyCycles: false`
  in PR8 (enabling it is PR9's job). A static invariant — `scripts/check-critical-invariants.sh`
  check #7 and `audit.TestTradeEngineSignalOnlyInPR8` — fails the build if `cmd/trade-engine`
  sets the flag true. The stale `cmd/trade-engine` package comment about refreshing/removing a
  QUEUED buy intent was corrected (PR8 writes only `comparison_events` + `signals`).
- **PR8 (correction, round 2) — engine signal-only via a capability flag**: the signal path
  called `prepareBuy`→`buyflow` unconditionally for trading-enabled markets, so PR8 could
  still indirectly create cycles/orders/requests/locks. Buy-cycle preparation is now gated
  behind `Config.PrepareBuyCycles`, which the engine **leaves false by default** — so PR8's
  only writes are `comparison_events` + `signals` even when a trading-enabled market's signal
  passes. The gated cycle-creation tests enable the flag explicitly (PR9 behavior). New strong
  test `TestSignalOnlyEvenWhenTradingEnabled` (signal+trading enabled, signal passes → 0
  cycles/orders/exchange_requests/symbol_locks) fails under the old behavior and passes now.
- **PR8 (correction, round 1) — resilient subscription, quote-rate fan-out, FeeFor**:
  the `market_events` loop returned `nil` on an unexpected channel close (silent stop); it now
  resubscribes with capped backoff and only stops on ctx-cancel. `targets` did not
  re-evaluate IRT markets when their `USDT/IRT` quote rate ticked — it now re-evaluates all
  signal-enabled rial-quoted markets on that exchange. Fee resolution already used
  `Snapshot.FeeFor` (per-exchange default, no key-0 leak — PR6). The standalone
  `UpdatePendingBuyRequest`/`RemovePendingBuyRequest` the review flagged never existed in
  code (a stale doc artifact, corrected). Branch cut from `pr7-queue-recovery-guards`.

- **PR7 (correction) — queue crash-recovery + guarded transitions**: `SweepStuck` now recovers
  stale `CLAIMED` requests (claimed but never marked `IN_FLIGHT`, so never sent) by resetting
  them to `QUEUED` and clearing `claimed_by`/`claimed_at` — safe for any type because
  `IN_FLIGHT` (not `CLAIMED`) is the pre-send boundary. `MarkInFlight` checks `RowsAffected`
  and returns `ErrRequestNotClaimed` on zero rows; all four mutating executor paths already
  skip the send when it errors. `MarkSucceeded/MarkFailed/MarkDead` are guarded on
  `status IN ('CLAIMED','IN_FLIGHT')` + `RowsAffected`: a conflicting newer terminal status
  yields `ErrRequestNotActive` (a late worker can't overwrite e.g. `DEAD` with `SUCCEEDED`),
  while re-applying the SAME status is an idempotent no-op (needed for crash-recovery
  reprocessing, e.g. a repeated `ProcessFinalStatus`). `MarkInFlight` stays strict (no
  idempotent no-op) because it gates a side-effecting send. Definite `PlaceOrder` rejection
  already moved the order out of `QUEUED` to `FAILED` via `ApplyOrderTransition`
  (`OnPlaceRejected`), and ambiguous outcomes already went to `DEAD` + `NEEDS_RECONCILE`; both
  kept + tested. No order/cycle state is updated outside `internal/state`. Branch cut from
  `pr6-config-fee-scope-validation`; single PR7 history row (pre-rebase duplicate gone).

- **PR6 (correction) — fee scoping, write validation, single-active enforcement**: default
  exchange fees are scoped by `exchange_id` (`DefaultFeesByExchangeID`) instead of sharing a
  single map keyed by `exchange_market_id` with `0`="default" (where every exchange's default
  overwrote the previous at key 0); `Snapshot.FeeFor(exchangeID, exchangeMarketID)` resolves a
  market override first, else THIS exchange's default, never another exchange's. The
  representative write `UpdateMinSpreadBps` now validates before opening the transaction, so an
  invalid value commits nothing (no version, no symbol_config change, no audit). `ActiveVersion`
  returns `ErrMultipleActiveVersions` rather than `ORDER BY id DESC LIMIT 1` when the table
  holds more than one active row. Integration-test seeds carry per-run suffixes (repeat-safe on
  a reused DB). No DB-level uniqueness constraint was added for active versions (the detect-and-
  error route was chosen so the "two active" case is still seedable + testable). Branch cut from
  `pr5-collector-ws-reconnect`; the single PR6 history row is corrected (pre-rebase duplicate gone).

- **PR5 (correction) — collector resilience & publish ordering**: an unexpected WebSocket
  close while the collector context is active is no longer a silent stop — it is logged,
  counted as a health failure (`WSFailureCount`), and reconnected with capped exponential
  backoff (200 ms → 30 s) for the lifetime of the context; the only clean exit is
  ctx-cancel, and a sustained outage additionally polls REST between attempts (sequential,
  no double-ingest). `ingest` now publishes a `market_event` ONLY after both `SaveOrderBook`
  and `SavePrice` succeed (a failed save suppresses the event so consumers never see a
  snapshot that is not cached). REST `received_at` is stamped after a successful
  `GetOrderBook`, not before the request. Collector remains PublicClient-only with no
  order/private/credential path. The single PR5 history row was corrected (the pre-rebase
  branch's duplicate row is gone after rebasing onto `pr4-exchange-masking-precision`).

- **PR4 (correction) — token masking, error-text masking, exact decimals**: the raw-API IO
  logger now (a) masks bearer/JWT token fields (`access`/`refresh`/`accessToken`/
  `refreshToken`/`jwt`/`bearer`) in JSON + non-JSON bodies, so Bitpin's `{"access":…,
  "refresh":…}` auth response cannot leak into `api_call_logs.response_body`; (b) masks the
  stored error text via `MaskErrorText` (was a raw `err.Error()`, which can embed a signed URL
  / token) before writing `api_call_logs.error`; and (c) keeps money values off `float64` —
  Exir order books were already decoded via `json.Number`+`NewFromString` (PR26 hardening),
  and the two `decimal.NewFromFloat(0.1)` Rial→Toman multipliers (nobitex/ramzinex) are now
  `decimal.RequireFromString("0.1")`. No exchange-mutating path was added — only masking +
  decimal-construction changes. Branch cut from `pr3-replay-strict-version` (carries the
  PR1/PR2/PR3 corrections).

- **PR3 (correction) — strict replay disambiguation**: `applyTransition`'s zero-row branch now
  accepts a replay no-op **only** when the row is in the target state at exactly
  `expected_version + 1` AND the recorded event at that version is the same `from→to`
  (`replayEventMatches`). Previously any row already in the target state was treated as a
  replay regardless of version, so a stale caller (e.g. `SELL_REPRICE_PENDING→SELL_SUBMITTED`
  with `expected_version=10` against a row already at `SELL_SUBMITTED` version 20) was wrongly
  accepted; it now returns `ErrStaleVersion`. No new state-mutating SQL was added — only the
  CAS UPDATE (unchanged) and a read-only event-existence SELECT — so there is no direct
  state-update bypass. `resolve.go` shares `applyTransition`, so operator resolutions inherit
  the same strict rule. Branch cut from `pr2-schema-corrections` (carries the PR1+PR2 fixes).

- **PR2 (correction) — schema hardening as a forward migration**: the missing FKs
  (`signals.exchange_id`, `signals.config_version`), operational indexes
  (`comparison_events`/`signals` by exchange+symbol+created_at), and financial-value CHECK
  constraints (`orders`/`fills`/`exchange_markets`/`wallet_balances_*`) ship as new migration
  025 rather than edits to the original 002/005/006 files — consistent with the immutable-
  applied-migrations rule enforced by the PR1 correction (`EnsureCurrent` would hard-fail any
  DB that had applied the old checksums). `tick_size`/`step_size` use `>= 0` (not `> 0`)
  because 0 is the system's documented "no snapping" sentinel; relaxed after the sellflow
  fixtures surfaced it. No FK was added to a retention table (those stay FK-free for cheap
  pruning). This branch is cut from the corrected PR1 branch, so it includes the
  EnsureCurrent checksum/unknown-version fixes + the Makefile config-comment fix.

- **PR1 (correction) — startup migration safety hardened**: `migrate.EnsureCurrent`/`Status`
  now verify the integrity of the APPLIED set, not just version presence — an edited
  already-applied migration (`ChecksumMismatchError`) or an applied version the binary does
  not recognize (`UnknownAppliedVersionError`, schema newer than code) makes service startup
  refuse to run. Previously only `migrate.Run` checked checksums, and services call only
  `EnsureCurrent`. Pure `checkAppliedIntegrity` core is unit-tested + gated EnsureCurrent/
  Status tests. Also fixed the Makefile comment that wrongly mentioned config via env vars
  (config is file-only via `-config`); no runtime `V3_*` env introduced.

- **PR27 — deploy packaging is safe-by-default, credential-free, no live trading**: a
  distroless image with all binaries (no secret baked in) + a compose stack (DB/Redis/
  one-shot migrate/8 services) + `configs/production.example.toml` (placeholders only, mode
  `off`, empty master key) + `DEPLOY.md` (startup order + dry-run procedure) +
  `scripts/local-dryrun-check.sh`. Verified: example config is safe + secret-free + redacts
  the DSN; services refuse pending migrations (`EnsureCurrent`); the kill switch defaults
  engaged. Dry-run uses simulated clients (no network/orders); live stays disabled without
  credentials.

- **PR26 — pre-deploy audit found no critical bug; invariants are now statically enforced**:
  `scripts/check-critical-invariants.sh` + `internal/audit/invariants_test.go` fail on a
  runtime `V3_*` env var, a mutating exchange call outside the executor/adapters, a direct
  `UPDATE …state` outside `internal/state`, a dashboard encrypted-blob reference, a
  secret-named log field, or a read-only service main holding a `PrivateClient`. Two
  hardening fixes: `queue.ScheduleRetry` dead-letters (never reschedules) a mutating request
  (defense-in-depth); `exir` order books parse via `json.Number`→`NewFromString` (exact, no
  float). Correction: the crash/rollback recovery paths are now proven by six venue-free
  fault-injection tests (TEST-ONLY `executor.faultAfterSend` + `opreconcile.faultBeforeCommit`
  seams + a send-counting fake), covering commit-before-send, MarkInFlight crash, place/cancel
  completion rollback, reprice-cancel-in-flight crash, and reconcile-apply crash — every one
  conservative (DEAD + NEEDS_RECONCILE, never re-sent, lock held until exposure proven). No
  live order sent; no API key needed.

- **PR25 — production hardening for the first canary order, no scope change**: a `RUNBOOK.md`
  operator runbook + a secret-free startup safety summary logged by each live-capable binary
  (`live.BuildSafetySummary`) + operator warnings (`GET /api/live/warnings`) + a secret-free
  per-session live-audit export (`GET /api/live/session/export`). Emergency stop (session stop
  or kill switch) blocks new buys immediately, keeps sell/cancel/status, recalls nothing
  already sent — documented + tested per cycle state.
- **PR25 — a recent dry-run must precede the first live buy**: `StartSession` enforces
  `preflight.RecentDryRunOK` for the exchange/symbol (a live buy is never the first run of
  that path). Also fixed `asInt` to parse driver `[]byte`/`float64` ids so session-view
  counts + the audit export correlate correctly.

- **PR24 — a live run session makes the first order observable + stoppable, without
  broadening scope**: `live_run_sessions` (migration 024) records operator/scope/credential/
  preflight-hash/ack-id/caps/status/reasons + the first-order checklist. `StartSession`
  requires canary scope + a current ack + dynamic readiness + no active session; the guard's
  live-BUY path additionally requires an ACTIVE session, so `StopSession` blocks new buys
  immediately while sell/cancel/status stay available (a per-run, audited complement to the
  kill switch).
- **PR24 — every live_audit row is correlated** with `live_session_id` + `acknowledgement_id`
  + `preflight_hash` (migration 024 columns; guard populates them). The first real buy of a
  session writes a one-time **first-order checklist** (mode/exchange/symbol/caps-remaining/
  credential-status/ack/session/kill-switch/request+order+cycle ids) — no secrets. Start/stop
  require admin; the session view is read-only.

- **PR23 — preflight is read-only; the acknowledgement is the gate**: `preflight.Checker`
  (DB handle only, reflection guard: no exchange method) runs the full readiness checklist
  and never mutates trading state. A live BUY additionally requires an operator
  acknowledgement bound to the preflight `config_hash`, enforced inside the PR20 guard — so
  accidental live trading is blocked even with creds/caps/controls present.
- **PR23 — the config hash binds config, not ephemera**: `ConfigHash` covers caps, live
  flags, canary scope, credential identity, freshness windows, mode + ack requirement;
  excludes market freshness/balances/kill switch. So a config change invalidates a prior ack
  (re-acknowledge required) but a market tick does not.
- **PR23 (correction) — a config hash is NOT the sole gate**: conditions rot with time even
  when config is unchanged, so the live-BUY guard additionally enforces an acknowledgement
  EXPIRY (`canary_ack_max_age_minutes`) AND re-checks the dynamic safety subset
  (`preflight.DynamicRecheck`: credential/market/balance freshness, reconcile cap, stuck
  IN_FLIGHT, dangerous queue) immediately before each buy. Expired ack, stale data, a new
  reconcile/stuck-request, or a re-engaged kill switch deny the next buy; sell/cancel/status
  stay allowed. Migration 023 adds `canary_ack_max_age_minutes`; the dashboard ack list
  shows an `expired` flag.
- **PR23 — canary = require_canary_ack(default 1) + single scope + max_open_cycles=1**: the
  guard restricts live buys to the configured canary exchange/symbol with a valid ack; the
  cap of one open cycle yields one-buy-at-a-time. Market-data freshness is proxied by a
  recent `comparison_event` (engine writes only when Binance + Iranian books are both fresh),
  keeping preflight DB-only (no Redis dependency). Migration 023 adds the freshness/canary
  columns + `live_acknowledgements`.

- **PR22 — credential provisioning encrypts in memory, stores only ciphertext**:
  `Provisioner.Create`/`Rotate` use the PR20a `secrets.Cipher` (AES-256-GCM,
  `nonce‖ciphertext‖tag`, key=SHA-256(master key)); plaintext is never stored/logged/
  returned/audited; the API returns only the new id + status. Master key from the config
  file only; empty key → safe-disabled (503).
- **PR22 — rotation guarantees a single active credential**: new active at key_version+1 +
  disable ALL previously-active in one tx, so the PR20a provider is never ambiguous. Disable
  keeps the row (history); validation is a read-only balance check (no place/cancel).
- **PR22 — credential editing is a separate duty**: only `credential_operator`/`admin`
  (`requireCredentialOperator`); viewer/config_operator/reconcile_operator are refused. The
  dashboard shows status only; `credential_audit` (migration 022) holds no secret material.

- **PR21 — NEEDS_RECONCILE has exactly one exit: an operator resolution**: a dedicated
  state-machine path (`ApplyCycleResolution`/`ApplyOrderResolution`) separate from the
  trading map, gated to `From=NEEDS_RECONCILE` + an explicit target whitelist, so the
  automatic flow can never exit and an illegal target is rejected. Every change still goes
  through `internal/state` (CAS + event row).
- **PR21 — no blind close**: the operator must see the full context (`GET /api/reconcile/
  {id}`) and a `preview` (exact proposed changes, zero mutation) before an `apply`; a reason
  is mandatory. Preview/apply require `reconcile_operator`/`admin`; list/detail/audit are
  read-only.
- **PR21 — lock released only on proven no-exposure**: the symbol lock is released only when
  net inventory = 0 / full safe exit; never on the button alone.
- **PR21 (correction) — `mark_failed` cannot strand exposure**: FAILED is terminal, so with
  open/unknown exposure `mark_failed` is REFUSED by default (cycle kept in NEEDS_RECONCILE,
  lock held, audited) — a warning is not enough. Forcing it requires an explicit
  `external_resolution_confirmed=true` + a mandatory `external_resolution_reason` (operator
  attests the exposure was handled outside the system), recorded in the audit
  (`external_resolution_confirmed` column, migration 021). Proven zero exposure is allowed
  but the preview prefers `cancel_zero_exposure`.
- **PR21 — fill safety**: operator fills validate side/qty/price/fee/fee-asset, reject
  oversell + duplicate fill id (`(order_id, exchange_fill_id)` unique), update cumulative
  order fields, and reuse `orders.ResolveCloseFromReconcile` for closing PnL accounting.
- **PR21 — local tool, no exchange mutation**: the `opreconcile.Resolver` holds only a DB
  handle (reflection guard: no Place/Cancel); a balance cross-check is advisory (warns, never
  auto-blocks). Audited immutably in `reconcile_resolutions` (migration 021).

- **PR20a — credentials decrypt in memory only, gated by the unchanged PR20 guard**:
  `internal/secrets` (AES-256-GCM, `nonce||ciphertext||tag`, key=SHA-256(master key)) +
  `internal/credentials.Provider` (an `exchanges.CredentialProvider`). Plaintext is never
  written back/logged/returned/in-errors; failures mark the row unusable and never panic.
- **PR20a — master key is config-file only**; empty/invalid key disables credential
  loading + live execution safely (no clients, nothing sent). No runtime env var.
- **PR20a — real clients are built through the factory**, never hardcoded per-exchange in
  the executor; the Provider is injected as `Creds`; unsupported exchange ⇒ no client. The
  selection is the single `enabled && active`, highest-`key_version` credential (rotation-
  ready); disabled/old/non-active rows are ignored.
- **PR20a — read-only services hold narrowed interfaces**: balance-sync
  (`balance.BalanceClient`), health private probe (`credentials.BalanceReader` via
  `Validate`), reconciler (`reconciler.ReadOnlyClient`) — Place/Cancel unreachable
  (compile-time + reflection guards). Credential validation is a read-only balance read
  only (never places/cancels).
- **PR20a — dashboard shows credential STATUS only** (`GET /api/credentials`): exists/
  enabled/status/key_version/algorithm/last_checked/non-secret-note — never key material
  or the encrypted blob.

- **PR20 — limited live is a safety PR with the final gate in the executor**: the
  `live.Guard` is the load-bearing check immediately before each real PLACE/CANCEL (mode/
  AllowLiveExecution/not-dry-run/exchange+symbol live-enabled/caps/credentials/kill-switch/
  state); the engine's `AllowNewBuyCycle` is a first check, not the only one. A denial
  fails the request without sending and is audited (`live_audit`).
- **PR20 — safe by default at every layer**: mode must be explicitly `live`; the kill
  switch defaults engaged (1); every cap in `live_controls` is required (any missing →
  denied); `exchanges.live_enabled` + `exchange_markets.live_enabled` default 0. Caps:
  open-cycles, daily-orders, daily-quote, order-notional, base-qty, consecutive-failures,
  unresolved-reconcile (migration 020).
- **PR20 — kill switch is asymmetric**: it blocks new buy cycles + new buy PLACEs (new
  exposure) but allows sell PLACEs (inventory exit), cancels, and status polls so open
  cycles stay safely managed.
- **PR20 — real credentials are deferred to PR20a**: live mode wires no real client and
  `AllowLiveExecution` stays false until credential decryption lands, so `live` is a
  fully-tested safety harness that sends nothing yet; the safety machinery is exercised
  with a fake (no-network) client and `live_audit` proves allow/deny decisions.
- **PR20 — no-blind-resend preserved in live**: ambiguous live PLACE → order/cycle
  `NEEDS_RECONCILE`, request `DEAD`; tested end-to-end with the `place_timeout` scenario.

- **PR19 — dry-run is config-driven + safe by default**: bootstrap `[execution] mode`
  (off|dry_run|live), default off → no client + AllowLiveExecution=false (no order can
  be sent). Dry-run/live are explicit; no runtime env var anywhere.
- **PR19 — simulated client (`simexec`) has no network code** and satisfies
  `exchanges.PrivateClient`; the executor wires it (not real adapters) in dry-run, so no
  real PlaceOrder/CancelOrder is reachable. Configurable scenarios (full/partial/zero/
  ambiguous/rejected/timeout/cancel-race).
- **PR19 — the full lifecycle goes through the real boundaries** (queue → executor →
  internal/orders → sellflow); the engine only stamps `cycles.dry_run` (migration 019),
  never closes a cycle directly.
- **PR19 — dry-run is identifiable**: dashboard surfaces `dry_run` on cycles/orders/
  requests/fills; the reconciler logs a `dry_run_cycle` decision so a simulated order is
  never confused with a real one.

- **PR18 — retention can only touch a fixed whitelist** of high-volume tables (each with
  a whitelisted timestamp column); permanent trading tables are absent and thus never
  deletable even if a retention_settings row names them (the worker iterates the
  whitelist, not the config). No Redis, no exchange calls.
- **PR18 — config-driven, never guessed**: missing config or retention_days ≤ 0 →
  do-nothing; disabled → skip. Batch params (batch_size/max_batches_per_run/pause_ms)
  added in migration 018.
- **PR18 — batched deletes** (`DELETE … WHERE ts < cutoff LIMIT batch_size`, bounded by
  max_batches, short-batch early-exit, optional pause) — never one huge delete.
- **PR18 — dry-run reports cutoff + estimated rows and deletes nothing**; a single-run
  `GET_LOCK` advisory lock prevents concurrent workers (can't acquire → clean exit).
- **PR18 — failure isolated + observable**: one table's error is recorded and the run
  continues (or stops if configured); each run writes a summary to app_logs (no
  secrets); a cancelled context stops cleanly.

- **PR17 — auth before any mutation**: mutating routes require a bearer token whose
  SHA-256 hash matches an enabled `dashboard_tokens` row (migration 017; plaintext never
  stored). No/bad token → 401; insufficient role → 403. Roles: viewer/config_operator/
  credential_operator/admin; config editing needs config_operator/admin. Reads stay open.
- **PR17 — every config edit is one versioned+audited+validated tx**: activate a new
  config_version, update only provided fields, write a config_change_audit row per field
  (old/new/operator/reason). changed_by is the authenticated operator (never client
  input). Validation rejections → 400; no-op edits → 400.
- **PR17 — enable-flag hierarchy enforced** (trading ⊆ signal ⊆ collection); sell_manage
  is independent so disabling trading never stops sell management of open cycles.
- **PR17 — `exchange_markets` has no config_version column**, so flag edits stamp the
  version on config_versions + audit only (the row carries no stamp); other config tables
  stamp it on the row.
- **PR17 — credential editing deferred** to a dedicated PR (encryption/key-version/
  never-leak); no credential route ships, so the auth boundary trivially covers it.
- **PR17 — hot reload via existing periodic reloads** (configstore.Cache + regime
  LoadBaskets); no service restart for normal config edits. Active cycles keep their
  stamped config_version and are never rewritten.
- **PR17 — WS origin allowlist** (`AllowedWSOrigins`) added; the WS remains command-free
  (read-only), so it can't be a config-editing vector.

- **PR16 — dashboard is read-only by construction**: the `Server` holds only a
  `*sql.DB` (no exchange client/queue — reflection guard) and registers GET-only routes,
  so any mutating method is 405 and there is no config-editing path (that's PR17).
- **PR16 — generic `jsonRows`** turns read-only SELECTs into JSON (decimals/JSON/text →
  strings, ints → numbers, NULL → null), so endpoints are thin SELECTs; lists take a
  defaulted + hard-capped `?limit=`; missing data → empty array (no panic).
- **PR16 — secrets never reach the browser**: credentials table never read; the
  `api-logs` view re-masks (defence in depth) the already-masked headers/bodies/url.
- **PR16 — queue display uses `step_kind`** (RETRY_SCHEDULED + retry_count 0 →
  scheduled_next_step, >0 → retry) so a planned simulated-IOC/reprice step isn't shown
  as a failed retry; balances expose a clean `stale` boolean (value never zeroed on
  absence); cycle detail carries a `fee_note` (realized_quote nets quote fees only).
- **PR16 — WebSocket is push-only**: it sends a safe periodic snapshot and takes no
  commands from the socket (live updates never control trading).

- **PR15 — regime reads Binance ONLY from Redis** (never a direct Binance call); a
  structural test asserts the Calculator holds no order client. It reads basket config
  from MariaDB and writes regime current/history to MariaDB; no trading/cycle/queue
  writes.
- **PR15 — scoring is multi-timeframe momentum** from a rolling per-symbol price series
  sampled out of Redis: per-symbol bps change vs a ~T-ago reference, weighted across
  symbols then timeframes → basket score; direction/level from configured thresholds;
  confidence = fresh-symbol-fraction × timeframe-coverage-fraction. The formula is in
  `regime.Calculate` (pure, unit-tested); nothing is hardcoded (baskets/weights/
  thresholds/timeframes are DB config).
- **PR15 — stale data is never fabricated**: a stale/missing symbol is excluded
  (lower confidence); a timeframe the series can't span is skipped; no fresh data → the
  regime is `UNKNOWN` with a `stale_reason`. A Redis miss records nothing (no crash); a
  restart warms up from empty (UNKNOWN until the series spans the timeframes).
- **PR15 — current upserted; history written on change by a FULL-FIELD hash**
  (`state_hash`, migration 016): direction, level, confidence, score, per-timeframe
  scores, per-symbol contributions, stale_reason, config_version — so confidence/score
  evolution (same label) and `UNKNOWN`-with-changed-reason are captured, while a
  genuinely identical regime is idempotent. A stale-data `UNKNOWN` is written to current
  (with reason), never a fabricated regime. History is FK-light + timestamp-indexed.
  (Clarification 1+2; see `regime.TestHistoryCapturesFullEvolution`.)
- **PR15 — hosted in the trade-engine** (it already has the Redis client); the engine
  provides the `PriceSource` (Binance mid/bid from `price:` keys with the venue time).
  Engine consumption of the regime is deferred.

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
- **Pending-intent scope is `exchange_market_id`** (one strategy today). The
  no-duplicate refresh of an existing QUEUED buy request is **PR9's** `buyflow`
  (`SELECT … FOR UPDATE` + a `status='QUEUED'` guard make CLAIMED/IN_FLIGHT requests
  untouchable), gated behind `Config.PrepareBuyCycles`. **PR8 itself never creates OR
  refreshes a buy request** — it is signal-only.
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
| PR5 | `pr5-collector-ws-reconnect` | **in review** | Redis market-data layer + collector. `internal/events` (BookSnapshot/PriceSnapshot/MarketEvent with timestamps), `internal/redis` market store (orderbook:/price: keys + TTL, `market_events` pub/sub, ErrNotFound), `internal/collector` (Collector using only PublicClient; WS-or-poll; DB-driven targets; DB health recorder; `MarketStore`/`HealthRecorder` interfaces), `FakePublicClient`, cmd/collector wired. **Correction:** an unexpected WS close while ctx is active reconnects with capped exponential backoff (never silently abandons a target; only ctx-cancel stops it; counted as a health failure + `WSFailureCount`); `market_event` is published ONLY after both `SaveOrderBook` and `SavePrice` succeed; REST `received_at` is stamped after a successful `GetOrderBook`. Tests: events, collector (fakes: poll/WS/health/shutdown/public-only, **ws-reconnect-on-unexpected-close**, **no-publish-when-save-book/price-fails**), sqlmock targets+health, gated real-Redis round-trip. Redis stays cache-only; collector uses only PublicClient; no trading/order/cycle/credential code. |
| PR6 | `pr6-config-fee-scope-validation` | **in review** | `internal/configstore`: DB-backed versioned trading config. `Snapshot` (MarketConfig merging exchange_markets flags + symbol_configs params, ExchangeConfig, fees, retention, active version), `Store.LoadSnapshot`/`ActiveVersion`, copy-on-write `Cache` + background `Run` reloader (non-blocking; keeps good config on reload failure), `ActivateVersion` + audited `UpdateMinSpreadBps` (version+audit in one tx, no secrets), validation (value sanity + enable-flag hierarchy), version-stamping helpers. **Corrections:** default fees are scoped per exchange (`DefaultFeesByExchangeID` keyed by exchange_id + `FeesByMarketID` keyed by exchange_market_id) with `Snapshot.FeeFor(exchangeID, exchangeMarketID)` (market override → THIS exchange's default, never another's) — replaces the unsafe single map where every default collided at key 0; `UpdateMinSpreadBps` validates BEFORE the tx (negative spread activates no version / mutates no symbol_config / writes no audit); `ActiveVersion` returns `ErrMultipleActiveVersions` instead of silently picking the latest; integration tests use per-run suffixes (repeat-safe). Tests: sqlmock loaders/version/audit, cache COW/reload/concurrent-read, validation, FeeFor scoping/priority (offline), gated fee-scoping/invalid-write-rejected/multiple-active-rejected + repeat-safe full-path. File-only bootstrap unchanged; no env config; not yet wired into a binary. |
| PR7 | `pr7-queue-recovery-guards` | **in review** | `internal/queue` (DB-backed priority queue): Enqueue (idempotency-rejected), cross-process-safe Claim (GET_LOCK + count + FOR UPDATE SKIP LOCKED; priority/next_retry_at/per-exchange-limit/enabled/type filters), MarkInFlight, MarkSucceeded/Failed/Dead, ScheduleRetry (capped backoff→DEAD), conservative SweepStuck (read-only requeue / mutating→DEAD+order NEEDS_RECONCILE). `internal/executor` (order-executor): claim+dispatch loop, read-only & mutating handlers, conservative ambiguous→DEAD+reconcile, atomic complete+order-transition (rollback-safe), `AllowLiveExecution` guard (default off), NO direct-send path. **Corrections:** `SweepStuck` also recovers stale `CLAIMED` (never sent → requeued to QUEUED, claim cleared); `MarkInFlight` checks `RowsAffected` → `ErrRequestNotClaimed` (executor does not send); `MarkSucceeded/Failed/Dead` are status-guarded (`WHERE status IN ('CLAIMED','IN_FLIGHT')` + `RowsAffected`) → `ErrRequestNotActive` on a conflicting newer status, idempotent no-op on same status; definite `PlaceOrder` rejection moves the order out of `QUEUED` to `FAILED` via `ApplyOrderTransition` (already correct); ambiguous → `DEAD` + order `NEEDS_RECONCILE` (already correct). Tests: queue sqlmock + gated MariaDB (concurrent claimers, **stale-CLAIMED recovery**, **MarkInFlight zero-row**, **terminal status guards + idempotency**), executor classifiers + reflection no-send guard + gated end-to-end with fake clients (**MarkInFlight-failure-blocks-send**, definite-rejection-out-of-QUEUED, ambiguous-NEEDS_RECONCILE). Order/cycle state only via `internal/state`; nothing trades yet. |
| PR12 | `pr12-reconciler-safeclose-guards` | **in review** | Cut from accepted PR11 (`pr11-ambiguous-lifecycle-safety`, `1333f09`); PR6–PR11 fixes preserved (FeeFor, queue guards, DB-role dispatch, buy/sell validation, empty-id safety, sell-rejection-keeps-lock, guarded PnL close — full sweep green). **Corrections:** (#2) exchange status `REJECTED` is NO LONGER advanced to a clean terminal — it is an execution anomaly → `NEEDS_RECONCILE` (never safe-close/lock-release; sell rejection keeps the lock); (#3) `safeClose` now checks, in the close tx, for any active `exchange_request` (QUEUED/CLAIMED/IN_FLIGHT/RETRY_SCHEDULED) and refuses to close / release the lock when one exists; (#4) `applyOrderOutcome` never silently skips an illegal transition — it diverts the order to `NEEDS_RECONCILE` (report never claims a non-advance). `internal/reconciler` (read-only; never auto-sends — holds a `ReadOnlyClient` with no Place/Cancel): `ReconcileStartup` + idempotent `RunPeriodic`; pure decision matrix (`decide.go`); capability-based known/unknown-exchange-order-id paths (unknown→never resend, positively-identify-or-NEEDS_RECONCILE); cycle decisions Continue/SafeClose/NEEDS_RECONCILE; **clean zero-fill safe-close → CANCELLED (NO_FILL) + lock release, NOT FAILED** (correction); missing/unknown order ≠ proof of no fill; decisions logged to app_logs; state via state machine. `internal/symbollock` read/release helpers (Acquire is PR9). cmd/reconciler wired (no clients). Tests: pure decide unit + gated MariaDB (decision matrix, safe-close+lock-release, ambiguous-keeps-lock, client-id attach, idempotent repeat, stuck-reporting, rollback, no-mutating-call guard). Completes the safety core (PR1–PR7 + PR12). |
| PR8 | `pr8-engine-signal-only` | **in review** | `internal/engine` (trade-engine signal loop): subscribe `market_events`; read Redis books/prices + configstore snapshot; **owner-defined spread implemented as planned** = (Binance best bid − Iranian best ask)/ask×10000, fee-adjusted (taker buy + maker sell); USDT direct / IRT-IRR convert via same-exchange `USDT/IRT` rate (missing/stale → no signal); freshness + enable-flag + config-v0 gating; write `comparison_events` (every computable comparison) + `signals` (passed), config-version stamped, quote_unit + reference_rate audited. **SIGNAL-ONLY: the only writes are `comparison_events` + `signals` — no exchange calls, no cycle/order/exchange_request/symbol-lock writes, EVEN for a trading-enabled market with a passing signal.** Buy-cycle preparation is gated behind `Config.PrepareBuyCycles` (default FALSE) and is PR9's transactional `buyflow`. **The `cmd/trade-engine` binary leaves `PrepareBuyCycles: false` in PR8 — the real executable is signal-only; PR9 enables it.** **Corrections:** removed the unconditional `prepareBuy`/`buyflow` call from the signal path (now flag-gated, off by PR8 default); set `cmd/trade-engine` `PrepareBuyCycles: false` + a static invariant (script check #7 + `audit.TestTradeEngineSignalOnlyInPR8`) that fails the build if the binary enables it; fees via `Snapshot.FeeFor(exchangeID, exchangeMarketID)` (per-exchange default, no key-0 leak); `market_events` subscription resilient — an unexpected close while ctx is active resubscribes with capped backoff and only stops on ctx-cancel (never silently returns nil); a `USDT/IRT` quote-rate tick re-evaluates all signal-enabled rial-quoted markets on the same exchange; corrected the stale doc/comments that claimed PR8 refreshes pending buy intent. Migration 009 (audit columns); `MarketConfig.ExchangeID`. cmd/trade-engine wired (no private clients; **PrepareBuyCycles off — signal-only**). Tests: offline spread/quote/targets-quote-rate-dependents/subscription-reconnect/no-client/**trade-engine-signal-only-static-invariant** + gated MariaDB+Redis (USDT signal, below-threshold, stale/missing, disabled-for-signal, IRT conversion, fee-adjusted, per-exchange-default-fee + override, **signal-only-EVEN-when-trading-enabled (0 cycles/orders/requests/locks)**, USDT/IRT-reevaluates-dependent-IRT, config-stamp; PR9-gated cycle-creation tests enable the flag). |
| PR9 | `pr9-buyflow-refresh-guards` | **in review** | `internal/buyflow` (+ `symbollock.Acquire`): first code that creates trading rows. Cut from accepted PR8 (`pr8-engine-signal-only`); `cmd/trade-engine` now sets `PrepareBuyCycles: true` (PR9 enables buy prep; the engine library still defaults it false as the gate). Fees come from `Snapshot.FeeFor(exchangeID, exchangeMarketID)` (per-exchange default, no key-0 leak — PR6). **Corrections:** (5) `RefreshActiveCycleBuy` now refreshes the FULL cycle signal snapshot (signal_time/prices/spread/fee_adjusted/buy_size/config_version), so cycle+order+request describe the same intent; (6) refresh is guarded on lock ACTIVE + cycle BUY_REQUEST_QUEUED + order QUEUED + request QUEUED (SELECT … FOR UPDATE), each guarded UPDATE re-asserts state and checks RowsAffected==1; (7) non-positive price/qty rejected (CreateBuyCycle errors, Refresh no-op) — venue tick/step/min validated at send, rejection handled cleanly by executor (PR7); (8) originating signal linked to the created/refreshed cycle (`signals.cycle_id`) in-tx. Removed the PR8-only "cmd must not enable PrepareBuyCycles" static invariant; tightened invariant #3 / `TestNoDirectStateUpdates` to flag state ASSIGNMENTS only (not the new guarded WHERE-clause state checks). On an accepted signal for a trading-enabled, fresh market it runs ONE transaction — insert cycle (config-stamped + signal context + execution mode) → acquire symbol lock (dup scope → `ErrSymbolLocked` → rollback, no orphan) → insert entry_buy order (`local_client_order_id`, limit, TIF NULL) → state machine cycle `NEW→SIGNAL_DETECTED→BUY_REQUEST_QUEUED` + order `NEW→REGISTERED→QUEUED` → enqueue `PLACE_ORDER` (deterministic idempotency key, full intent payload) → commit. Owner-defined maker-first/taker-fallback decision (`buyflow.Decide`, pure): maker limit below ask by `maker_price_offset_bps`, taker at ask after `maker_attempts_before_taker` maker attempts within `maker_signal_window_seconds`; persists intended mode/attempt/offset/ask. One shared attempt counter advances on create AND on refresh of the active scope (resets on window expiry). No-duplicate via the lock; the active cycle's still-QUEUED buy is **refreshed in place and re-decided** (so the SAME request escalates MAKER_FIRST→MAKER_RETRY→TAKER_FALLBACK without a duplicate); cycle-tied requests never deleted; CLAIMED/IN_FLIGHT never mutated. **Executes nothing** (no private client, no place/cancel/query, no fills, no lock release). Migration 010 (symbol_configs maker/taker cols + orders/cycles exec-mode cols); configstore loads the policy. Tests: offline Decide + gated (atomic create, rollbacks, dup-lock-blocks, maker→retry→taker across cycles, window reset, refresh-advances-attempt-and-escalates, refresh-window-expiry-resets, refresh-no-dup, CLAIMED/IN_FLIGHT untouched, idem-key unique, config stamp, flags/stale block, state-machine events, no private client). |
| PR10 | `pr10-place-validation-fill-safety` | **in review** | `internal/orders` (buy-side order/fill processing) + executor wiring. Cut from accepted PR9 (`pr9-buyflow-refresh-guards`), preserving PR1–PR9 fixes (FeeFor, queue guards, subscription reconnect, buyflow refresh guards — verified by the full sweep). **Corrections:** (5) `BuyIntentPayload.Validate()` runs BEFORE MarkInFlight/PlaceOrder — a malformed/zero price/qty, wrong side/type/non-IOC, or empty client id is never sent (→ clean `OnPlaceRejected`: request+order+cycle FAILED, lock released); (6) a place ack with empty `ExchangeOrderID` schedules NO blind cancel/status → order+cycle NEEDS_RECONCILE, lock HELD; (7) a full/partial fill needs a usable cost basis — `usableAvgPrice` derives `ExecutedQuote/FilledQty` when `AvgPrice`≤0, and a full fill with neither is Ambiguous→NEEDS_RECONCILE (lock held, no fill row with zero price); (8) scheduled CANCEL/GET_ORDER keep `retry_count=0` (planned step, not a retry) — documented + tested. **Round 2:** (#1) an UNDECODABLE buy payload (with order/cycle context) now resolves via `OnPlaceRejected` (request+order+cycle FAILED, lock RELEASED) instead of only failing the request — no more stuck order/cycle/lock; (#2) PLACE_ORDER is dispatched by the **DB order role** (`entry_buy`/`exit_sell`), never `payload.side` — a wrong-side payload on a buy order routes to the buy handler and is rejected by `Validate()`, never slipping into the sell handler; `PayloadSide` removed; sell gets its own `SellIntentPayload.Validate` (bad sell → FAILED + NEEDS_RECONCILE, lock held). Simulated IOC as queued work (no worker sleeps): PLACE ack → `OnPlaceAck` (order QUEUED→SUBMITTED→ACKED, cycle →BUY_SUBMITTED, schedule CANCEL at `now+maker_wait`) → CANCEL ok/definite-reject → `OnCancelResult` (order →CANCEL_PENDING, schedule GET_ORDER) → `ProcessFinalStatus` (classify → fills + transitions + lock). Pure `Classify` (full/partial/zero/ambiguous); missing order ≠ zero fill; zero-fill → CANCELLED (`SIMULATED_IOC_ZERO_FILL`, lock released) not FAILED; partial → continue filled qty (lock held); full → BUY_FILLED (lock held); ambiguous (incl. ambiguous cancel/place) → order+cycle NEEDS_RECONCILE (lock held, never re-sent); definite place-rejection → `OnPlaceRejected` (FAILED + lock released). Fill accounting (filled/remaining/avg/quote/fee/fee_asset/`actual_execution_mode`/`fill_result`/`last_normalized_status`) + idempotent aggregate `fills` row (deterministic id). All state via `internal/state`; queue+state+fill+lock in one tx (never SUCCEEDED if state failed). Native IOC never forced (TIF empty). `queue.EnqueueScheduled`; `execution.OrderStatus.Liquidity`; migration 011; `BuyIntentPayload` moved to `internal/orders`. Tests (fake clients only): offline Classify matrix + gated (place→cancel→final scheduling, zero/partial/full, missing-not-zero, ambiguous-cancel→reconcile, place-rejected-clean, fee/avg, maker/taker, idempotent repeat, rollback) + executor end-to-end IOC loop. |
| PR11 | `pr11-sell-validation-pnl-safety` | **in review** | Cut from accepted PR10 (`pr10-place-validation-fill-safety`, `622668b`); PR6–PR10 fixes preserved (FeeFor, PR7 queue guards, PR8 subscription reconnect + USDT/IRT, PR9 buyflow refresh guards, PR10 DB-role dispatch + buy validation + empty-id + cost-basis — all green in the full sweep). **Corrections:** (#2/#3) sell `PLACE_ORDER` is routed by DB order role (not `payload.side`) and the sell payload is validated before MarkInFlight/PlaceOrder (`SellIntentPayload.Validate`); invalid/undecodable/wrong-side sell → request FAILED + order/cycle NEEDS_RECONCILE, lock held. (#4) empty sell `ExchangeOrderID` → NEEDS_RECONCILE, lock held, no blind follow-up (also guarded in `ensurePoll`/`RepriceSell`). (#5) sell fill needs a usable cost basis (derive `ExecutedQuote/FilledQty`, else ambiguous → NEEDS_RECONCILE, no fill). (#6) `closeCycleWithPnL`/`writeCloseAccounting` check all query errors + validate buy/sell qty+quote positive + qty tolerance (`ErrIncompleteCloseAccounting`) → don't close with missing/invalid accounting (automatic path diverts to NEEDS_RECONCILE). (#7) `RepriceSell` with an empty resting-sell `exchange_order_id` → NEEDS_RECONCILE, lock held, no blind `CancelOrder("")`. `internal/sellflow` (exit sell create/reprice/Manager) + `internal/orders` sell processing + executor routing + engine driver. Sell on the ACTUAL filled inventory (`bought − sold`, step-floored), never the requested qty; partial buys sell their filled part (`BUY_PARTIALLY_FILLED→SELL_REQUEST_QUEUED`). Price `floor(binanceRef×(1−sell_offset_bps/10000), tick)`, min-order enforced; offset/tick/step/min are DB config (loaded into `MarketConfig`). `CreateSell` one tx (insert sell order → cycle→SELL_REQUEST_QUEUED + order NEW→REGISTERED→QUEUED → enqueue sell PLACE; rollback on failure; no-duplicate via active-sell guard). Resting place (`OnSellPlaceAck`, no auto-cancel) + Manager-driven `sell_status` poll (`ProcessSellStatus`): partial→SELL_PARTIALLY_FILLED (manage remainder), full→SELL_FILLED→CLOSED + PnL + lock release, ambiguous/missing→NEEDS_RECONCILE. Repricing cancel→replace, interval-gated (`reprice_interval_seconds`/`last_reprice_at`), skipped while a sell place/cancel is CLAIMED/IN_FLIGHT; cancel's final status always read before reselling; ambiguous→NEEDS_RECONCILE. Close writes exit accounting + `realized_quote` (fees netted only when quote-denominated; migration 012). All state via `internal/state`; queue+state+fill+lock atomic; engine never calls exchanges (executor only). Tests (fake clients): pure price/tick/step/min + gated sellflow (create full/partial, no-dup, below-min, tick-snap, rollback, reprice interval/in-flight/no-resting, Manager-creates-sell) + gated orders sell (place-ack-rests, partial-manages, full-closes+PnL, missing-ambiguous, idempotent, reprice-cancel partial/raced-full) + executor end-to-end sell loop. |
| PR13 | `pr13-balance-sync` | **accepted** | `internal/balance` + `cmd/balance-sync`: continuous read-only balance sync. Narrow `BalanceClient` (only `Name`+`GetBalances` — no place/cancel reachable). Per poll, per exchange/asset: content hash `sha256(asset\|available\|locked\|total)` over canonical decimals; `wallet_balance_history` row only when the hash changes (no dup spam); `wallet_balances_current` upserted every observation with fresh `last_seen_at` (migration 013). Decimal end-to-end into `DECIMAL(36,18)` (never float; 18-dp preserved); `total` derived as available+locked when omitted. Bounded concurrency + per-exchange timeout; one exchange's failure/timeout is isolated and NEVER wipes/zeros prior balances; a missing asset is never zeroed/deleted (its row survives, `last_seen_at` goes stale). Changes no cycles/orders/queue. Binary wires no clients yet (credential decryption later) and idles safely; no secrets logged. Tests (fake read-only clients): offline hash + read-only-interface guard + no-clients startup; gated (first-obs current+history, unchanged-no-dup, changed-avail/locked add history, missing-asset-not-zeroed, failure-isolation-keeps-previous, precision, timeout-keeps-previous, context-cancel-stops). |
| PR14 | `pr14-health-monitor` | **accepted** | `internal/health` + `cmd/health-monitor`: read-only per-exchange health. Monitor invokes only caller-supplied read-only `ProbeFunc`s (public `GetMarkets`; private balance read when creds exist) — no place/cancel reachable (reflection guard); no cycle/order/queue writes. `Classify(err)` → normalized Status (HEALTHY/DEGRADED/UNAVAILABLE/AUTH_FAILED/RATE_LIMITED/UNKNOWN) + Category (timeout/network/exchange_5xx/exchange_4xx/auth/rate_limit/unsupported/invalid_response/unknown) from execution sentinels + NormalizedAPIError + ErrUnsupported + json errors; `context.Canceled` not recorded. `Recorder` upserts `exchange_health_current` (per-kind status, latency, last_success/failure, consecutive_failures reset-on-success, error/timeout/rate/auth counters, last_error_category/message) + appends `exchange_health_samples` (no FK, timestamp-indexed). Public/private tracked separately; auth error → api_key_status invalid; transient failure never wipes last_success; bounded concurrency + per-probe timeout isolate failures. Migration 014 (normalized status + failure-tracking cols). Binary runs public probes only (private UNKNOWN until creds); no secrets logged/stored. Tests (fake read-only probes): offline Classify matrix + read-only guard + no-targets startup; gated (healthy public, timeout/auth/rate/network/5xx/invalid classified+counted, private-auth→key-invalid, failure isolation, consecutive-then-reset, last-success preserved, context-cancel-stops). |
| PR15 | `pr15-market-regime` | **accepted** | `internal/regime` + migration 015/016 + trade-engine wiring: market-regime calculation from Binance prices read ONLY from Redis (no Binance calls; structural guard asserts no order client; no cycle/order/queue writes). DB-configurable baskets (`market_regime_baskets`/`_basket_symbols`/`_timeframes`): symbols+weights, timeframes+weights, neutral/moderate/strong thresholds, update interval, config version. `regime.Calculate` (pure): multi-timeframe momentum from a rolling per-symbol price series — per-symbol bps change vs ~T-ago reference, weighted across symbols then timeframes → score; direction (BULLISH/BEARISH/NEUTRAL/UNKNOWN) + level (STRONG/MODERATE/WEAK/FLAT/UNKNOWN) from thresholds; confidence = fresh-symbol-frac × timeframe-coverage-frac. Stale/missing symbol excluded (lower confidence); no fresh data → UNKNOWN + stale_reason (never fabricated); Redis miss records nothing (no crash). `market_regime_current` upserted (idempotent); `market_regime_history` written only on direction/level change (deduped); both config-version-stamped, FK-light + timestamp-indexed. Calculator samples Redis into the series + recomputes per basket interval; engine hosts it + provides the PriceSource (binance mid/bid). Tests: offline calc matrix (bullish/strong, threshold mapping, weighted symbol + timeframe, missing→confidence, stale-excluded, no-data-UNKNOWN, insufficient-history, empty-basket) + no-order-client guard; gated (load config, current-upsert + history-on-change + config-version, calculator samples+persists, redis-miss no-crash/no-fabricate). Clarification: history dedupes on a FULL-FIELD state_hash (migration 016) so confidence/score evolution + UNKNOWN-reason changes are captured (TestHistoryCapturesFullEvolution). |
| PR16 | `pr16-dashboard` | **accepted** | `internal/dashboard` + `cmd/dashboard`: READ-ONLY operator views. Server holds only a `*sql.DB` (no exchange client/queue — reflection guard); all routes GET-only so any mutating method (incl. config edit) is 405; no place/cancel/cycle/order/queue/config mutation. GET JSON endpoints: cycles open/closed/{id}-detail, orders, fills, requests, signals, comparisons, balances, health, regime, logs, api-logs (masked), config (read-only snapshot), `/ws`, index, healthz. Generic `jsonRows` (SELECT→JSON); `?limit=` defaulted+capped; missing data→empty array (no panic). Cycle detail composes orders/fills/requests/state-events/locks/logs + maker-taker fields + fee_note. Queue `step_kind` (RETRY_SCHEDULED rc==0→scheduled_next_step, rc>0→retry). Balances `stale` flag (never zeroed on absence). Health public/private/api-key/ws + counters. Regime direction/level/confidence/score/contributions/stale. api-logs re-masked (defence in depth; credentials never read). WebSocket pushes safe periodic snapshot (open cycles/health/regime/balances), takes no commands. Separate binary (restart isolates). Tests: offline (no-order-client guard, mutating-method-405, step_kind, mask-secrets) + gated (all endpoints missing-data 200, seeded cycle detail + maker/taker + 404, retry-vs-scheduled, balances stale + value-preserved + api-log masking, pagination limit, WebSocket snapshot). Config editing + auth deferred to PR17. |
| PR17 | `pr17-config-editing` | **accepted** | `internal/dashboard` (auth/admin) + `internal/configstore` (admin) + `internal/regime` (admin) + migration 017: authenticated, authorized, versioned, audited, validated config EDITING. Still no trading: no place/cancel/cycle/order/queue/credential mutation route. Auth = bearer token, SHA-256-hashed in `dashboard_tokens` (plaintext never stored); no/bad token → 401, insufficient role → 403. Roles viewer/config_operator/credential_operator/admin; editing needs config_operator/admin. Each edit = ONE tx: activate new config_version + update provided fields + config_change_audit per field (old/new/changed_by=authenticated operator/reason). Validation (min_spread≥0, buy_size>0, unit∈{base,quote}, offsets/intervals/retries/slippage sane, maker_window>0, taker_mode=ASK, fees≥0, regime thresholds ordered, weights>0) → 400; no-op → 400. Enable-flag hierarchy trading⊆signal⊆collection enforced; sell_manage independent (disabling trading never stops open-cycle sell mgmt). Editable: symbol config, market flags, exchange config, fees, regime basket/symbol/timeframe; `GET /api/audit`. exchange_markets has no config_version col → version on config_versions+audit only. Hot reload via existing configstore.Cache + regime LoadBaskets (no restart); active cycles keep stamped config_version (never rewritten). WS origin allowlist added (WS stays command-free). Credential editing deferred to a dedicated PR (no route ships). Tests: offline (symbol/exchange Validate matrices) + gated (unauth→401, viewer→403, config_operator versioned+audited symbol update, invalid→400, flag hierarchy 400/200, exchange+fee edits + negative-fee 400, regime basket edit + bad-ordering 400, audit endpoint, no-credential/no-trading mutation routes). |
| PR18 | `pr18-retention-worker` | **accepted** | `internal/retention` + `cmd/retention-worker` + migration 018: controlled retention of high-volume operational tables. Fixed whitelist (api_call_logs/comparison_events/exchange_health_samples/app_logs/wallet_balance_history/market_regime_history, all created_at); permanent tables (cycles/orders/fills/signals/symbol_locks/exchange_requests) absent → never deletable even if a retention_settings row names them. Config-driven (enabled/retention_days/batch_size/max_batches_per_run/pause_ms); missing/retention_days≤0 → do-nothing (never guessed), disabled → skip. Batched DELETE … WHERE ts<cutoff LIMIT batch_size (bounded by max_batches, short-batch exit, optional pause) — never one huge delete. Dry-run reports cutoff + estimated rows, deletes nothing. Single-run GET_LOCK advisory lock (can't acquire → clean exit, no deletes). One table's failure recorded + run continues; cancelled ctx stops cleanly; run summary written to app_logs (no secrets). No Redis, no exchange calls. Binary runs once on startup then every 6h; `-dry-run` flag (no runtime env var) for dry-run. Tests: offline whitelist/permanent guard + gated (missing-config no-op, disabled no-op, dry-run no-op+cutoff+estimate, batch-delete only-old + recent-preserved, batch_size+max_batches honored, permanent-table never targeted, one-table-failure recorded+continue, advisory-lock blocks concurrent, run recorded to app_logs, ctx-cancel clean). |
| PR19 | `pr19-dry-run` | **accepted** | `internal/simexec` + migration 019 + config `[execution] mode` + engine/buyflow/executor/dashboard/reconciler wiring: dry-run trading mode runs the FULL lifecycle (signal→cycle→lock→buy→queue→executor→simulated fill→sell→simulated status→close→reconcile) through the REAL queue/executor/order-processing/sellflow boundaries against a SIMULATED client — no real PlaceOrder/CancelOrder ever sent. Activation config-driven + safe-by-default: `[execution] mode` off (default; no clients, AllowLiveExecution=false) / dry_run (wire simexec clients + AllowLiveExecution=true + engine stamps cycles.dry_run) / live (real clients, deferred → falls back to safe off). `simexec.Client` (no network) satisfies exchanges.PrivateClient; scenarios full/partial/zero/ambiguous/rejected/place_timeout/cancel_race. Engine never closes cycles directly. Dashboard surfaces dry_run on cycles/orders/requests/fills; reconciler loads cycles.dry_run + logs a dry_run_cycle decision (never confuses simulated with real). Tests: offline (simexec scenario matrix, no-mutating-network, default full-fill) + config default-safe (mode off ⇒ not dry/live) + gated (full lifecycle buy→sell→CLOSED+lock-released, zero-fill→CANCELLED, partial-buy→sells-filled-qty-only, ambiguous→NEEDS_RECONCILE, dashboard dry_run label, reconciler dry_run identification). |
| PR20 | `pr20-limited-live` | **accepted** | `internal/live` (Guard) + migration 020 (`live_controls` singleton + `exchanges`/`exchange_markets`.live_enabled + `live_audit`) + executor/engine/dashboard/cmd wiring: the limited-live SAFETY layer. Real live orders allowed ONLY under explicit caps + a global kill switch + per-exchange/per-symbol live flags + credential availability + valid state, with the FINAL gate INSIDE order-executor (not only the engine). Safe by default: mode must be explicitly `live`; kill switch defaults engaged (1); every cap required (any missing → denied); live_enabled flags default 0. Caps: max open cycles / daily orders / daily quote / order notional / base qty / consecutive failures / unresolved reconcile. Executor `liveGatePlace`/`liveGateCancel` run `live.Guard.CheckPlace`/`CheckCancel` immediately before each real PLACE/CANCEL (mode/AllowLiveExecution/not-dry-run/exchange+symbol live/caps/credentials/kill-switch/state); deny → request FAILED without sending + audited; allow → sent + audited. Kill switch is asymmetric: blocks new buy cycles + buy PLACEs, allows sell PLACE (inventory exit) + cancel + status. Engine `AllowNewBuyCycle` is the first check (kill switch + open-cycle cap). No-blind-resend preserved (ambiguous live PLACE → order/cycle NEEDS_RECONCILE, request DEAD). Dashboard `GET /api/live`: LIVE mode, kill switch, caps, live-enabled exchanges/symbols, daily-order/open-cycle allowance, credential STATUS only (no key material), unresolved-reconcile count, last live allow/deny. **Real credential decryption + real-adapter wiring deferred to PR20a** — until then `live` wires no real client (`AllowLiveExecution` false) and sends nothing; the safety machinery is fully exercised with a fake (no-network) simexec client. Tests: offline none new; gated live guard (allowed-baseline+audit, denies matrix [dry-run/kill-switch/not-configured/exchange-not-live/symbol-not-live/no-credentials/oversized-notional/oversized-qty], kill-switch-allows-sell+cancel, cancel-needs-creds, AllowNewBuyCycle caps, daily-order cap) + gated executor live-gate (allow→fills+audit, kill-switch→blocked+FAILED+deny-audit, no-credentials→refused, ambiguous→NEEDS_RECONCILE+DEAD-no-resend) + gated dashboard `/api/live` (LIVE/kill-switch/controls/credential-status-no-secrets). |
| PR20a | `pr20a-credential-decryption` | **accepted** | `internal/secrets` + `internal/credentials` + executor/balance-sync/health/reconciler/dashboard wiring: real credential decryption + real private-client wiring, gated by the unchanged PR20 guard. `secrets`: AES-256-GCM, stored `nonce||ciphertext||tag`, AES key = SHA-256(master key); only AES-256-GCM supported; Encrypt/Decrypt symmetric; empty master key → ErrNoMasterKey (safe-disable); decrypt failure → ErrDecrypt (no plaintext). `credentials.Provider` (an `exchanges.CredentialProvider`): selects the single enabled+active, highest-key_version credential, decrypts api_key/secret/passphrase IN MEMORY; disabled/non-active/old-version ignored; unsupported-algo/decrypt-failure → mark row status='error' (non-secret note) + error with no plaintext; never writes back/logs/returns plaintext. `credentials.Builder.BuildPrivate` builds via the FACTORY (`exchanges.NewPrivateClient`) injecting the Provider as Creds + DB symbol map; active-credential-only; unsupported exchange → no client; no per-exchange hardcoding; no network at construction. `Provider.Validate` = read-only balance check ONLY (BalanceReader interface; never place/cancel), stamps active/invalid. Executor (live) builds real clients for live-enabled+active-credential exchanges, AllowLiveExecution=true, guard unchanged; no/invalid master key → no clients, nothing sent. balance-sync/health-private-probe/reconciler build credentialed clients held through narrowed non-mutating interfaces (BalanceClient/BalanceReader/ReadOnlyClient). Dashboard `GET /api/credentials` (+ /api/live block): STATUS ONLY (exchange/label/status/enabled/key_version/algorithm/last_checked/non-secret-note) — never key material or blob. Master key is config-file only (no runtime env). Tests: offline crypto (roundtrip, wrong-key→ErrDecrypt-no-leak, missing-key, truncated/corrupt, algorithm guard) + narrowed-interface compile+reflection guards (no Place/Cancel) + gated credentials (decrypt-valid, wrong-master-key-marks-error, missing-key-disables, unsupported-algo-marks-error, disabled-ignored, active-over-non-active, highest-key_version-selected, factory-injects-decrypted-creds, build-refuses-without-credential, validate-is-read-only-never-place/cancel) + gated dashboard `/api/credentials` (status-only, no secret fields, blob bytes absent). No real network in any test; no PlaceOrder/CancelOrder during validation. Remaining: provisioning/rotation UI + encrypt-and-insert CLI. |
| PR21 | `pr21-operator-reconcile` | **accepted** | `internal/opreconcile` + `internal/state` (operator-only exit) + `internal/orders` (shared close) + dashboard endpoints + migration 021 (`reconcile_resolutions`): authenticated, audited, explicit operator resolution of NEEDS_RECONCILE — the ONLY exit from that state, never automatic. `state.ApplyCycleResolution`/`ApplyOrderResolution`: separate from the trading map, require From=NEEDS_RECONCILE + an explicit target whitelist (cycle: BUY_FILLED/BUY_PARTIALLY_FILLED/SELL_PARTIALLY_FILLED/SELL_FILLED/CANCELLED/FAILED/CLOSED; order: FILLED/PARTIALLY_FILLED/CANCELLED/FAILED), same CAS+event; illegal target rejected. `opreconcile.Resolver` (DB handle only — reflection guard: no Place/Cancel; no exchange import): Preview (read-only, exact proposed changes + warnings, zero mutation) then Apply (one tx: re-validate → state machine → record fill → release lock only if safe → audit). Actions: cancel_zero_exposure, attach_exchange_order_id, mark_buy_filled, mark_buy_zero_filled, mark_sell_filled (full exit → CLOSED + PnL via orders.ResolveCloseFromReconcile), mark_sell_partially_filled, mark_order_cancelled_zero_fill, keep_needs_reconcile, mark_failed. Lock released ONLY on proven zero exposure / full exit (never on the button). mark_failed safety: FAILED is terminal, so with open/unknown exposure it is REFUSED (kept in NEEDS_RECONCILE, lock held, audited) unless the operator sets external_resolution_confirmed=true + a mandatory external_resolution_reason (then FAILED + lock released, audited with the flag); proven zero exposure allowed but prefers cancel_zero_exposure. Fill safety: side/qty/price/fee/fee-asset validated, oversell + duplicate-fill-id rejected, cumulative order fields updated. Balance cross-check advisory (warn >1%, never blocks). Dashboard: GET /api/reconcile (list), /api/reconcile/{id} (full context: cycle/exchange/orders/fills/requests/events/locks/logs/reason/balances/prior-resolutions/actions), /api/reconcile/audit; POST …/preview + …/apply gated by requireReconcileOperator (reconcile_operator/admin → 401/403); operator from the session, never the body; secrets never shown. No exchange mutation. Audit `reconcile_resolutions` (operator/time/cycle/order/action/old+new states/reason/fill/before+after/lock_released). Tests: offline (state resolution success/illegal-target/non-reconcile-from rejected + whitelist; resolver-holds-no-exchange-client) + gated opreconcile (zero-exposure-close+release, buy-fill-records+holds-lock, sell-fill-closes+releases, partial-keeps-lock, duplicate-fill/invalid-qty/oversell rejected, attach-oid, keep-no-release, failed-with-exposure-keeps-lock, preview-no-mutate, balance-warning, reason-required, mark_failed-open-exposure-refused+kept-in-reconcile, mark_failed-forced-with-external-confirmation, forced-requires-external-reason, zero-exposure-failed-warns) + gated dashboard (401/403 auth incl. wrong-role, detail-context+no-secrets, preview-no-mutate, apply-resolves+audits-operator, invalid→400, list). Correction: `mark_failed` refuses to strand open/unknown exposure (kept in NEEDS_RECONCILE) unless explicitly forced with `external_resolution_confirmed`+reason (migration 021 adds the audit column). |
| PR22 | `pr22-credential-provisioning` | **accepted** | `internal/credentials.Provisioner` + dashboard endpoints + migration 022 (`credential_audit`): operator create/rotate/disable/validate of exchange credentials. Plaintext exists ONLY in memory: Create/Rotate encrypt each secret with the PR20a `secrets.Cipher` (AES-256-GCM, `nonce‖ciphertext‖tag`, key=SHA-256(master key)) and store ONLY ciphertext — never logged (log-capture test), never returned (API responds id+status only), never audited. Master key from the config file only (no runtime env); empty key → `ErrNoMasterKey` → endpoints safe-disabled (503); wrong key cannot decrypt (PR20a ErrDecrypt). Create: exchange/label/key_version/algorithm(only AES-256-GCM)/enabled/status(enum)/api_key/api_secret/optional passphrase + mandatory reason; duplicate (exchange,label) → 400. Rotate: new active at key_version+1 (derived `…#vN` label) + disable ALL previously-active in one tx → exactly one active credential (no ambiguity); PR20a provider resolves to the new secret. Disable: enabled=0/status=disabled, KEEPS the row (secrets not deleted), provider ignores it. Validate: read-only balance read via the narrow `BalanceReader` (cannot place/cancel), stamps status + audits. Authz: create/rotate/disable require `credential_operator`/`admin` (`requireCredentialOperator` → 401/403); viewer/config_operator/reconcile_operator refused. Dashboard shows status only (`GET /api/credentials`, `/api/credentials/audit`) — never key material/blob/plaintext. Audit `credential_audit` (exchange/credential/operator/action/old+new status/old+new key_version/reason; no secrets). Tests: gated credentials (create-encrypts+roundtrips+no-plaintext-in-blob/logs, wrong-master-key-cannot-decrypt, duplicate-label→400, input validation, create-audit, rotation-activates-new+disables-old+single-active+both-audited, disable-ignored-by-provider+row-kept, validate-read-only+audit, no-master-key→ErrNoMasterKey) + gated dashboard (create/disable 401/bad/403-for-viewer+config_operator+reconcile_operator, create-via-http-returns-no-secrets+stores-ciphertext, invalid→400, disable-via-http, audit-endpoint-no-secrets). No real network in any test; validation cannot place/cancel; no runtime env var. |
| PR23 | `pr23-live-preflight` | **accepted** | `internal/preflight` + `internal/live` (canary ack gate) + dashboard endpoints + migration 023 (`live_controls` freshness/canary cols + `live_acknowledgements`): strict read-only live readiness checklist + an explicit operator acknowledgement the guard enforces, so live trading can't start accidentally even with creds/caps/controls. `preflight.Checker` (DB handle only — reflection guard: no place/cancel/balance/order; no exchange import; mutates nothing): `Run` produces a Report (per-check pass/fail/warn, failures/warnings, ready, config_hash). Checks: execution-mode-live, kill-switch known+disengaged, exchange+symbol live-enabled, caps configured+sane (+canary max_open_cycles=1), credential exists/enabled/active/validated + validation-fresh (credential_validation_max_age_minutes), private-health ok (WARN accepted when health_required=0), balance recent, market-data fresh (recent comparison_event ⇒ Binance+Iranian fresh), reconcile within cap, no stuck IN_FLIGHT mutating, no stale lock, no DEAD mutating on real cycles, recent dry-run CLOSED for the exchange/symbol, auth path (enabled token), audit path (live_audit). `ConfigHash` covers config-relevant inputs only (caps/live-flags/canary/credential-identity/freshness/mode/ack-req — excludes market freshness/balances/kill-switch). `POST /api/live/acknowledge` (admin) re-runs preflight, refuses unless ready (409), records `live_acknowledgements` bound to the config hash (operator/exchange/symbol/credential/caps/reason), deactivating any prior. Guard (require_canary_ack default 1): a live BUY must be within the canary exchange/symbol scope AND covered by an active ack whose preflight_hash == current ConfigHash AND not expired (canary_ack_max_age_minutes) AND pass a dynamic re-check (credential/market/balance freshness, reconcile cap, stuck IN_FLIGHT, dangerous queue) — missing/out-of-scope/stale/expired/dynamic-fail → deny; sells+cancels unaffected. `GET /api/live/preflight` + `GET /api/live/acknowledgements` (with an `expired` flag) read-only. No exchange mutation; preflight places/cancels nothing. PR20 guard remains mandatory. Tests: offline (checker-holds-no-exchange-client) + gated preflight (passes-when-ready, fails on credential-missing/validation-stale/kill-switch/caps-missing/market-stale/balance-stale/reconcile-over-cap/stuck-inflight/no-recent-dry-run, does-not-mutate, ack-records-operator+hash, ack-refused-when-not-ready, config-change-invalidates-ack) + gated live (canary-ack-required+stale-after-config-change, canary-scope-restricts-to-one-market, ack-required-but-scope-unset) + gated dashboard (preflight-read-only, acknowledge-requires-admin [401/403 viewer+config+credential+reconcile, 200 admin records operator+hash], not-ready→409). No real network in any test. Correction: a config-only hash is not the sole gate — the live-BUY guard also enforces ack EXPIRY (canary_ack_max_age_minutes) + a dynamic re-check (preflight.DynamicRecheck) before each buy; tests: expired-ack-denies, stale-credential/market/balance-after-ack-denies, new-reconcile/stuck-inflight-after-ack-denies, kill-switch-reengaged-denies, sell/cancel-unaffected. |
| PR24 | `pr24-canary-session` | **accepted** | `internal/live` (run sessions) + executor + dashboard endpoints + migration 024 (`live_run_sessions` + `live_audit` correlation cols): first real canary live-run instrumentation — makes the first order observable, correlatable, and stoppable WITHOUT broadening scope (still one exchange/symbol/cycle/tiny notional, gated by the PR23 ack). `live_run_sessions` records operator/exchange/symbol/credential/preflight-hash/ack-id/caps/status/start+stop reasons/first-order-checklist. `StartSession`: verifies canary scope + a current (hash-matching, non-expired) acknowledgement + dynamic readiness + no active session, then inserts ACTIVE; contacts no exchange. The guard's live-BUY path now ALSO requires an ACTIVE session (added to canaryAckOK after ack/expiry/dynamic), so `StopSession` blocks new buys immediately while sell/cancel/status stay allowed (per-run audited complement to the kill switch). `POST /api/live/session/start|stop` require admin (start re-runs preflight → 409 if not ready; 400 on scope/ack/readiness; 409 if already active); `GET /api/live/session` read-only shows session + order count/quote used/open cycles/last order/last deny/kill switch/mode/ack status. Every live_audit row tagged with live_session_id + acknowledgement_id + preflight_hash. First real buy of a session writes a one-time first-order checklist (mode/exchange/symbol/caps-remaining/credential-status/ack/session/kill-switch/request+order+cycle ids) — no secrets, idempotent. Tests: gated live (start-requires-valid-ack, out-of-scope-rejected, failed-readiness-rejected, succeeds+recorded+single, stop-blocks-buys+allows-sell/cancel+audited, stop-without-active, no-active-session-denies-buy, live_audit-includes-session+ack+hash, first-order-checklist-written+no-secrets+idempotent) + gated dashboard (start requires admin [401/403], requires ack [400], failed-preflight [409], start→view-ACTIVE→stop→no-active + second-stop 409). No real network in any test; scope stays single-canary. |
| PR25 | `pr25-canary-runbook` | **accepted** | `RUNBOOK.md` + `internal/live` (startup summary, dry-run gate) + `internal/preflight` (RecentDryRunOK) + cmd (startup log) + dashboard (warnings + audit export) + the asInt fix: real-canary execution runbook & production hardening, no scope change (still one exchange/symbol/cycle/tiny notional, gated by preflight+ack+session). RUNBOOK.md: provision→validate→configure caps/scope→dry-run→preflight→acknowledge→disengage kill switch→start session→watch first order→stop→kill switch→inspect/export audit→resolve NEEDS_RECONCILE, + an emergency-stop-by-cycle-state table. `live.BuildSafetySummary` (read-only, no secrets): execution mode/live-enabled/kill-switch/canary exchange+symbol/caps-configured/credential STATUS/active-session/new-live-buys-allowed (full guard verdict) — logged at startup by order-executor + trade-engine. `StartSession` now also requires a recent successful dry-run (preflight.RecentDryRunOK) for the exchange/symbol. `GET /api/live/warnings` (read-only, severity-tagged): live-mode-enabled, kill-switch-disengaged, session-active, first-order-pending/sent, unresolved-reconcile, balance/market/credential-validation stale. Emergency stop (session stop or kill switch) blocks new buys immediately + keeps sell/cancel/status + recalls nothing already sent (documented + tested per cycle state). `GET /api/live/session/export` (read-only, no secrets): session + preflight hash + acknowledgement + caps + requests + orders + allow decisions + denials + first-order checklist + stop reason (via PR24 live_audit correlation). Fixed asInt to parse driver []byte/float64 ids so session counts + export correlation work. Tests: gated live (startup-summary-no-secrets + off-mode, emergency-stop-matrix [stop blocks buys/keeps sell+cancel; kill switch same], start-requires-recent-dry-run) + gated dashboard (warnings appear in live-danger states incl. stale balance/market, audit export includes session/caps/checklist/decisions + no secrets). No real network in any test; scope stays single-canary. |
| PR26 | `pr26-predeploy-audit` | **accepted** | `scripts/check-critical-invariants.sh` + `internal/audit/invariants_test.go` + two hardening fixes + focused tests: a read-only critical pre-deploy audit (NO deploy, NO live order, NO API key, NO scope change). Audited all safety layers — runtime-config boundary, exchange-mutation boundary, DB-commit-before-send, queue claim SQL (OR/AND precedence correct + type/enabled/concurrency filters), mutating-retry safety, state-machine enforcement, symbol-lock safety, buy/sell lifecycles, simulated-IOC classification, decimal/precision, live-guard deny matrix, credential/secret masking, dashboard authz, operator reconciliation, audit correlation, crash/restart — partly via independent read-only sub-audits of the riskiest areas. **No critical bug found; every invariant HOLDS.** Static checks (CI + `go test`): fail on runtime V3_* env, PlaceOrder/CancelOrder outside executor/adapters, direct UPDATE cycles|orders SET state outside internal/state, dashboard encrypted-blob reference, secret-named log field, read-only service main holding PrivateClient — all pass. Hardening fixes (defense-in-depth, not bugs): (1) `queue.ScheduleRetry` dead-letters a mutating request (+ order NEEDS_RECONCILE) instead of ever rescheduling — guards a future caller from blind re-send; (2) `exir` order books parse via json.Number→decimal.NewFromString (exact, no float round-trip). Tests added: audit static invariants (6), queue mutating-retry guard, exchanges mask-covers-every-adapter-secret-field (incl secret_key), dashboard asInt driver-type parsing (the PR25 []byte/float64 id bug). Remaining risks documented: a few crash-recovery scenarios are venue/fault-injection-only (conservatively handled by sweeper→DEAD+reconcile). Verification: static script exit 0; offline `go test ./...` ok; full gated `-p 1` green; build/vet/gofmt clean; go.mod unchanged. Correction: added six venue-free crash/rollback fault-injection tests (TEST-ONLY executor.faultAfterSend + opreconcile.faultBeforeCommit seams + a send-counting fake client): commit-before-send recoverable+no-dup, MarkInFlight-crash, place-completion-rollback, cancel-completion-rollback, reprice-cancel-in-flight-crash, reconcile-apply-crash — all conservative (DEAD + NEEDS_RECONCILE, never re-sent, lock held). No live order sent; no real API key used. |
| PR27 | `pr27-deploy-packaging` | **in review** | `Dockerfile` + `docker-compose.yml` + `configs/production.example.toml` + `DEPLOY.md` + `scripts/local-dryrun-check.sh` + safe-default tests: local/staging deployment packaging with NO live trading and NO real credentials (no PlaceOrder/CancelOrder, no API key). Dockerfile builds all 9 binaries (collector/trade-engine/order-executor/reconciler/balance-sync/health-monitor/dashboard/retention-worker/migrate) into one small distroless non-root image — no config/secret baked in. docker-compose: MariaDB 10.6 + Redis 7 (health-checked) + a one-shot migrate + the 8 services, each waiting on migrate via service_completed_successfully and mounting configs/config.toml read-only; dashboard on :8080. production.example.toml: placeholders ONLY — no API key, no plaintext credential, master_key empty, [execution] mode=off (safe default; off/dry_run for local/staging). DEPLOY.md: startup order (DB -> Redis -> migrate -> services -> verify dashboard/market-data/dry-run; services fail fast on pending migrations) + a full dry-run procedure (simulated clients, zero exposure). Tests: offline production-example-is-safe-and-secret-free (mode != live, master_key empty, DSN redacted, no api_key/secret in file) + gated services-refuse-pending-migrations (EnsureCurrent) + kill-switch-defaults-engaged. Existing PR19-PR26 suites cover no-real-mutating-in-dry-run, live-disabled-without-credentials, dashboard-no-secrets. Verification: all 9 binaries build, `docker compose config` valid, scripts bash-clean, static invariant script PASS, offline `go test ./...` ok, full gated `-p 1` green, build/vet/gofmt clean, go.mod unchanged. No live order; no real API key.  |
