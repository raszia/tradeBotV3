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
| `dashboard` | **PR16:** strictly read-only HTTP views + snapshot-only WebSocket (same-origin). **PR17:** authenticated + audited config editing / operator actions. **No trading control.** Separate binary. | no |
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
  dashboard/   read-only HTTP + WebSocket views [PR16]; config editing       [PR17]
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
- **PR13 (026):** `exchange_configs.balance_poll_interval_seconds` (INT, NULL/0 = default) +
  `CHECK (>= 0)` — the per-exchange balance-sync cadence (see §11a), a forward ALTER migration.
- **PR15 (015/016/027):** `market_regime_baskets`, `market_regime_basket_symbols`,
  `market_regime_timeframes`, `market_regime_current`, `market_regime_history` (015);
  `market_regime_current.state_hash` (016); **`market_regime_history.state_hash` +
  `.stale_reason`, and the regime-config CHECK constraints** (027 — see §15).
- **PR17 (028):** `dashboard_users` (PBKDF2 password hash, role, active) + `dashboard_sessions`
  (sha256(token) only) — the dashboard login/session model (see §14a).
- **PR18 (029):** `retention_settings` CHECK constraints — `retention_days ∈ NULL|[1,3650]`,
  `batch_size ∈ [1,50000]`, `max_batches_per_run ∈ [1,10000]`, `pause_ms ∈ [0,60000]` (safe
  bounds; the worker also validates at runtime — see §16a).
- **PR19 (030):** `sim_exchange_orders` — persistent simulated-exchange order state so
  dry-run `simexec` survives restarts and is shared across executor/reconciler instances
  (see §16b). Carries an explicit mutable lifecycle (`status`, `filled_quantity`) so ambiguous
  place/cancel timeouts are modelled faithfully, and `UNIQUE(exchange_code, client_order_id)`
  so an order can be recovered by client_order_id after a place timeout AND a re-placed order is
  immutable/idempotent. Simulation only; no real exchange, no secrets.
- **PR19 round 2 (031):** `idx_cycles_dry_run_state (dry_run, state)` — used by the mode-scoped
  claim's dry_run cycle-set subquery (`EXPLAIN` on 10.6 shows `MATERIALIZED c ref
  idx_cycles_dry_run_state`) and by mode-scoped open-cycle scans; justified by the real plan, not
  the name (§16b). No column added, no data rewritten.
- **PR19 round 3 (032):** `sim_exchange_orders` gains `order_type`, `time_in_force` (so the full set
  of exchange-visible immutable fields is stored + compared) and `hidden_probes` (models eventual-
  consistency: an accepted order invisible to the first GetOrder lookups). The invariant "a mutating
  request can never reach the exchange without a cycle + order" is enforced at RUNTIME (claim clause
  + pre-send fail-closed guard), NOT a CHECK, so the generic `exchange_requests` queue stays
  decoupled from trading FKs (§16b).

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

**`exchange_cooldowns` (migration 033, PR20 correction).** One row per exchange holding a
DURABLE rate-limit park deadline: `exchange_id` (PK, FK→exchanges ON DELETE CASCADE),
`cooldown_until`, `reason`, `source`, `updated_at`, plus `idx_exchange_cooldowns_until`.
Written extend-only (`GREATEST`), reloaded by the order-executor at startup. Holds no secrets
(reason = category/code, source = which signal identified it). See §16c.

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
- **Parked-exchange gate (PR20).** Before claiming for an exchange the executor checks
  its REACTIVE rate-limit cooldown (§16c): a parked exchange is **skipped entirely** —
  nothing is claimed, so no request of any type can reach it until the cooldown expires.
  It is poll-driven (the next tick re-checks), so there is no busy loop and no goroutine
  sleeps holding claims.
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
  `RETRY_SCHEDULED` with exponential capped backoff, or → `DEAD` at max_retries. It
  **refuses mutating requests** — a PLACE/CANCEL is never rescheduled by the generic
  retry path.
- **`Release` (PR20)** returns a `CLAIMED` row to `QUEUED` (`WHERE id=? AND
  status='CLAIMED'`). It is the safe **pre-send defer**: the row was claimed but never
  reached `IN_FLIGHT`, so nothing was sent. The executor uses it when an exchange gets
  parked between the claim and the send.
- **`RequeueProvenUnexecuted` (PR20)** is the ONLY sanctioned retry of a *mutating*
  request. The caller must have PROOF from the venue's documented contract that the
  request was rejected **before execution** (`RateLimitInfo.DefiniteRejection`, §16c);
  it sets `RETRY_SCHEDULED` with `next_retry_at` at the cooldown deadline, is bounded by
  `max_retries`, and on exhaustion dead-letters conservatively (`DEAD` + the owning order
  `NEEDS_RECONCILE`) rather than sending again. Because `next_retry_at` is **persisted**,
  a restart during the cooldown cannot duplicate the mutation.
- **`MarkInFlight` at the REAL network boundary + GUARDED (PR7 + PR20 correction, round 8):** ALL
  THREE private adapters (Nobitex, Wallex, Bitpin) implement `MutationPreparer`; every pre-send
  step — credentials, symbol, payload, AND the final `http.Request` — runs BEFORE MarkInFlight, so
  the row becomes IN_FLIGHT only immediately before the actual order/cancel `http.Do` (bind context
  + send, nothing fallible in between). A crash during preparation (token acquisition or an
  in-memory credential read) leaves it CLAIMED, never "maybe sent". Guard unchanged: the
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
    non-retryable → `FAILED`. A **rate-limit** error additionally parks the exchange
    (§16c), so the reschedule naturally lands after the cooldown.
  - **rate-limited mutating (PR20, checked BEFORE the branches below):** a throttle
    signal parks the exchange, then splits on proof — a venue-PROVEN pre-execution
    rejection → `RequeueProvenUnexecuted` (retry after the cooldown, never blind); ANY
    other rate-limit-looking response, **including HTTP 200 with a throttle body** →
    treated as AMBIGUOUS (never success, never retried) and resolved by the §10d
    read-only recovery probe with the symbol lock HELD.
  - mutating: `MarkInFlight` → send → on **success** complete the request AND
    advance the order (`QUEUED→SUBMITTED`, stamping `exchange_order_id`) in **one
    transaction** (rule #9; if the order transition fails the whole tx rolls back
    and the request stays `IN_FLIGHT` for the sweeper); on a **definite rejection**
    (clear 4xx / insufficient balance) → `FAILED` (order unchanged); on an
    **ambiguous** outcome (timeout/network/5xx/unknown) → `DEAD` + order
    `NEEDS_RECONCILE`.
- **A refused mutating request is never just "failed" (PR20).** When the live gate denies (or
  a payload/market cannot be resolved), the executor resolves it through the official state
  path — buy → FAILED + lock released, sell → NEEDS_RECONCILE + lock held, cancel → DEAD +
  NEEDS_RECONCILE + lock held — so no order is left `QUEUED` with no executable request. See
  §16c.
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

**Per-exchange, rate-limit-aware cadence.** Each exchange is polled on **its own interval**,
so different venues (e.g. Nobitex 30 s, Bitpin 60 s, Wallex 45 s) do NOT share one global
cadence: `balance.Config.IntervalFor(code)` returns the per-exchange interval (0 → the default
`Interval`), floored by `Config.MinInterval` so a misconfiguration can never over-poll. The run
loop wakes at the finest effective interval and **skips any exchange not yet due** (per-exchange
`lastPolled` tracking), so a rate-limited venue is polled *less often* without slowing the
others (mirrors the reference's per-exchange `balance_poll_interval` + skip-not-due). The
binary wires `IntervalFor` from the **DB config** — `exchange_configs.balance_poll_interval_seconds`
(migration 026; loaded into `configstore.ExchangeConfig.BalancePollIntervalSeconds`) — so the
cadence is per-exchange, config-driven, and backward-compatible (no value → the default for all).
All venues are REST-only (no balance WebSocket). These snapshots feed reconciliation and the
advisory balance cross-check (§10d) — they are inventory **confirmation**, never the primary
matched-quantity source (order fills are).

**Clients are wired (read-only) — `balance-sync` really syncs.** `cmd/balance-sync` builds real
authenticated balance clients via the credential factory (DB-decrypted credentials), each
narrowed to the read-only `BalanceClient` interface (`Name` + `GetBalances` only — no
`PlaceOrder`/`CancelOrder` reachable by construction). If the master key is missing/invalid, or
an exchange has no active credential, that client is simply not built and the syncer idles for it
(no panic, no live credentials required in tests) — a safe degrade, not a skeleton. API
keys/secrets are never logged; raw API logs still pass the secret masker.

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

**Private health is wired read-only (Option A), consistent with balance-sync (PR13).**
The binary builds an authenticated probe through the same read-only credential path
`balance-sync` uses: `credentials.NewProvider` → `Builder.BuildPrivate(code)` →
`Provider.ProbePrivateHealth(ctx, code, BalanceReader)`. It takes the **narrow read-only
`BalanceReader`** (a balance read only), so no `PlaceOrder`/`CancelOrder` is reachable
from the private probe — enforced by `credentials.TestProbePrivateHealthIsReadOnly`.

**Continuous probing must not invalidate a credential on a transient error.** The health
probe uses `ProbePrivateHealth`, **not** the strict one-shot `Provider.Validate`. It marks
the credential `invalid` **only on a definite auth error**; a temporary failure (timeout /
network / rate-limit / HTTP 429 / exchange 5xx) leaves the credential row **active and
untouched** and surfaces only as private **health** status (UNAVAILABLE/DEGRADED/
RATE_LIMITED) with `consecutive_failures` incremented and `last_success_at` preserved. This
is essential because `BuildPrivate` only builds from `active` credentials — otherwise a
brief exchange incident could permanently disable a valid credential and silently stop
private probing/live trading. (See §16d's two-path credential-validation note.)

When the master key is missing/invalid or an exchange has no active credential, that
exchange falls back to **public-only** and its `private_status` stays `UNKNOWN` — a
**deliberate safe fallback, not an accidental omission**: private health is never marked
unhealthy merely because credentials are absent, and the monitor never panics (no live
creds required in tests). API keys/secrets are never logged or stored; adapter errors are
pre-masked and the stored `last_error_message`/sample `error` are truncated.

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
wraps `execution.Err*` sentinels, and — PR20 — carries `RateLimit *RateLimitInfo`
for throttle responses; see the rate-limit block below), and
`execution.{OrderRequest,OrderAck,
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
- **Rate-limit normalization (PR20 — `internal/exchanges/ratelimit.go`):** each adapter
  turns its venue's DOCUMENTED throttle signals into `Category=CatRateLimit` +
  `RateLimitInfo{RetryAfter, Source (status|header|body|code), DefiniteRejection, Code}`,
  wrapping `execution.ErrRateLimited`. Detection is **never HTTP-429-only**: it considers
  the status, the structured venue error code, JSON body fields, the body message, the
  `Retry-After` header, `X-RateLimit-Remaining:0` + `X-RateLimit-Reset`, and **successful
  HTTP responses carrying a business-level throttle error**. Shared parsers:
  `ParseRetryAfter` (delta-seconds or HTTP-date; malformed/non-positive → 0 so the caller
  applies its configured fallback), `RetryAfterFromHeaders`, `ApplyRateLimitSignals`, and
  the deliberately-narrow `LooksLikeRateLimitMessage` fallback (matches only unambiguous
  throttle phrases, so it can never fire on "limit order" / "price limit"). Any
  venue-supplied wait is capped at 15m. **`DefiniteRejection` is established per exchange
  and per operation from a reliable contract — never inferred globally** — and is what
  entitles a mutating request to be re-queued (§8/§16c). The rules are ported from the
  owner's proven iranArb system where it had them (Nobitex, Bitpin).
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
  business errors; private/book WS (Centrifuge) deferred → polling. **Throttling
  (PR20):** the documented `{"status":"failed","code":"TooManyRequests","backOff":N}`
  envelope arrives on **429 AND on HTTP 200** — `backOff` is in **SECONDS**
  (iranArb-verified), capped at 15m; a parsed envelope is a **definite pre-execution
  rejection** (`status:"failed"` is Nobitex's documented not-performed contract), while a
  bare 429 with no parseable envelope is NOT.
- **Wallex** orders are **keyed by `client_id`, not an exchange order id** (cancel/
  get take the client id); TMN→IRT; `x-api-key` auth; WS deferred → polling. Throttling
  is detected from the status + standard headers, and from a `success:false` 200 body
  only via the conservative phrase fallback; never a proven rejection (no documented
  contract). iranArb had no reactive Wallex rule (proactive pacing only), so nothing is
  guessed here.
- **Bitpin** JWT access/refresh token flow (cached ~14m); rate-limit sensitive
  (429 back-off — `Retry-After` integer seconds, else the DRF body regex
  `"available in N seconds"`; iranArb-proven parsers, reused for the order paths, where a
  429 is **not** a proven rejection); underscore symbols (`BTC_IRT`); `identifier` =
  client order id; WS deferred → polling.
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
  (concurrency/timeouts/rate limits), fees, retention settings, and the active
  `config_version`. `Store.LoadSnapshot` builds it; a missing active version is
  tolerated (`Version = 0`, the "unconfigured" state).
- **Per-exchange operational fields — who actually consumes them (PR20).** A config field
  that looks active but is ignored is a safety hazard (an operator "tightens" a limit that
  does nothing), so the current wiring is stated exactly:

  | `exchange_configs` field | Consumed at runtime by | Status |
  |---|---|---|
  | `rate_limit_per_sec` | order-executor PROACTIVE pacer (§16c) | **wired in PR20** |
  | `retry_backoff_ms` | order-executor REACTIVE fallback cooldown (§16c) | **wired in PR20** |
  | `balance_poll_interval_seconds` | `cmd/balance-sync` per-exchange cadence | wired (PR13) |
  | `max_concurrent_requests` | nothing — `executor.Config.LimitFor` is unwired, so the effective claim limit is **1 per exchange per tick** | **NOT consumed (§18)** |
  | `request_timeout_ms` | nothing — a request's `timeout_ms` is stamped at enqueue from `symbol_configs.order_timeout_ms` | **NOT consumed (§18)** |
  | `max_retries` | nothing — a request's `max_retries` is stamped at enqueue from `symbol_configs.max_retries` | **NOT consumed (§18)** |

  The order-executor reads the wired fields live through the copy-on-write cache
  (`Config.ExchangeTuningFor`), so an edit takes effect on the next reload without a
  restart, and a DB blip retains the last good snapshot. PR20's mandate covered the two
  rate fields; the three remaining dead fields are recorded in §18 rather than left to look
  active.
- **Every reload is validated before it is swapped in (PR20 correction).** The initial load
  is validated synchronously at startup (§16c). The PERIODIC refresh is validated too:
  `Cache.RunValidated` calls the SAME validator on each reload and only replaces the active
  snapshot if it passes; an invalid or incomplete reload (a removed `exchange_configs` row for
  a wired live exchange, or a negative value) is rejected and the **last known-good snapshot
  keeps serving** — an invalid reload can never silently disable pacing. `RunValidated` also
  performs NO immediate reload at start (the validated startup load already ran), removing a
  redundant, potentially-unvalidated second load right after startup.
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
a reflection test asserts this), and **every route is GET-only** — any POST/PUT/PATCH/
DELETE (e.g. an attempt to edit config, cancel an order, or retry a request) is `405`,
because there is no mutating route at all. It never places/cancels orders, creates
cycles/orders, and never mutates cycles/orders/queue/locks/config.

**Config editing is PR17 scope, not PR16.** PR16 only *displays* config read-only.
Config editing / operator actions are a **separate later PR (PR17)** and must be
implemented there with explicit safety controls (auth/authz, versioning, audit,
validation). No editing route, auth layer, or mutating handler ships in PR16.

**DB errors never become a partial 200.** The composite endpoints (`/api/cycles/{id}`
and `/api/config`) run several sub-queries; **every** sub-query is error-checked and any
failure returns `500`. A failed query is never silently swallowed into a `200` with
partial data — an operator must never misread "no orders" / "no logs" / "empty config"
as fact when the query actually failed. (Only `sql.ErrNoRows` for the cycle lookup is a
`404`.)

**Endpoints (all GET, read-only SELECTs, JSON):** `/api/cycles/open`,
`/api/cycles/closed`, `/api/cycles/{id}` (detail), `/api/orders`, `/api/fills`,
`/api/requests`, `/api/signals`, `/api/comparisons`, `/api/balances`, `/api/health`,
`/api/regime`, `/api/logs` (app_logs / reconciler decisions), `/api/api-logs` (masked),
`/api/config` (read-only snapshot), `/ws` (live), `/` (index), `/healthz`. List
endpoints take `?limit=` (defaulted + hard-capped). Missing data returns an empty
array (never a panic).

**WebSocket is snapshot-only, same-origin, and error-safe.** `/ws` pushes a SAFE periodic
snapshot (open cycles, health, regime, balances) every `WSInterval`; it only SELECTs and
sends. Three guards:
- **Snapshot-only / command-free.** Incoming client messages are **drained and ignored**
  (no command handler for cancel/retry/config-edit/mutate — a test sends such messages and
  asserts nothing mutates and the socket keeps delivering snapshots). Live updates never
  control trading.
- **Same-origin only.** The upgrader's `CheckOrigin` (`sameOriginOnly`) permits an upgrade
  only when the request `Origin` host equals the request `Host` (case-insensitive), so a
  foreign website opened in the operator's browser cannot connect and read operational data
  (rejected with `403`) — important when the dashboard listens on `0.0.0.0`. A request with
  **no `Origin` header is allowed** (browsers always send `Origin` on a WS handshake, so a
  missing one is a non-browser client — curl/health-probe/native tooling — not the
  cross-site threat); a malformed/opaque (`null`) origin is rejected. A configurable
  allowlist + full auth are PR17 scope.
- **No partial snapshots.** `snapshot()` error-checks **every** query; if any fails it does
  NOT send a healthy-looking snapshot with empty sections — it sends a generic
  `{"type":"snapshot_error","error":"dashboard snapshot unavailable"}` (raw DB details are
  logged server-side, never sent to the browser) and keeps the connection so a transient
  failure recovers on the next tick. `stale` is normalized to a boolean, matching HTTP
  `/api/balances`.

The loop ends on client disconnect or server shutdown.

**No secrets — both log views masked.** The dashboard never reads the credentials table.
BOTH the `api-logs` view AND the `app_logs` view (`/api/logs` and the cycle-detail
`logs`) re-mask, defence in depth, the already-masked-at-storage content — the values of
sensitive keys (authorization/bearer/api-key/secret/client-secret/api-secret/signature/
access-token/refresh-token/token/password/passphrase/cookie/…) are redacted so no secret
reaches the browser even if one was written to `app_logs` upstream.

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

**Auth & network exposure.** PR16 ships **no user authentication** (it is strictly
read-only); the WebSocket is restricted to **same-origin** connections (see above) as a
baseline browser safeguard. **Read-only does NOT mean safe for public exposure** — anyone
who can reach the port can read all operational data, and same-origin does not stop
`curl`/scripts/port-scanners/non-browser clients (or requests with no `Origin`). Therefore
the dashboard must be **bound to localhost** (`configs/production.example.toml` and
`config.example.toml` ship `listen_addr = "127.0.0.1:8080"`; the docker-compose host port
is published loopback-only as `127.0.0.1:8080:8080`) and exposed only behind a **trusted
VPN or an authenticated reverse proxy** — never published directly to an untrusted network.
A test (`config.TestProductionExampleDashboardIsLoopbackOnly`) asserts the shipped examples
never bind the unauthenticated dashboard to `0.0.0.0`/a public interface. The WebSocket also
sets a small inbound frame read limit (it accepts no commands). Full dashboard
authentication/authorization + a configurable WS-origin allowlist land with the
config-editing PR (**PR17**). See DEPLOY.md §3a.

## 14a. Dashboard login + config editing (implemented in PR17 — `internal/dashboard` + `internal/configstore` admin)

PR17 adds a **real login/session system** and **authenticated, authorized, versioned,
audited, validated, concurrency-safe** editing of the DB-backed operational config. It
still **does not trade** — there is no place/cancel, and no cycle/order/queue/lock/
credential mutation route; only config rows change, through the versioned-write path.

**Login + sessions.** A small set of trusted internal **users** (`dashboard_users`,
migration 028): `username`, a salted **PBKDF2-HMAC-SHA256** `password_hash` (never
plaintext), `role`, `active`, `last_login_at`. `POST /login` verifies the password
(constant-time) against an *enabled* user and starts a server-side session
(`dashboard_sessions`): an opaque 256-bit random token is minted, only its **sha256 hash**
is stored (never the token), with a `user_id`/`role`/`username` snapshot and `expires_at`.
The raw token is returned as an **HttpOnly, SameSite=Lax, Secure-when-HTTPS** cookie
(`[dashboard] secure_cookies`). `POST /logout` revokes the session. Bootstrap the first
admin with `dashboard -create-user user:role` — the **password is read from a hidden TTY
prompt (or stdin), never a command-line argument** (so it can't leak via shell history /
`ps` / logs).

**Route protection + live user state.** Only `/healthz` and `POST /login` are
unauthenticated. **Everything else — the UI (`/`), every read API (`/api/*`), the
config-editing mutations, and the WebSocket (`/ws`) — requires a valid session** (401
otherwise). Session resolution **joins `dashboard_users`** and reads the user's **current**
role, requiring `active = 1`: so disabling a user (`active=0`) immediately breaks their
existing sessions (→ 401), and a role change (e.g. admin→viewer or a promotion) takes effect
on the very next request **without a re-login** — no stale role snapshot from the session
row is ever trusted.

**Roles.** `viewer` | `config_operator` | `admin` (rank-ordered). Read routes need any
logged-in role; config editing needs **`config_operator`+** (a `viewer` editing → **403**);
**high-risk** flag changes need **`admin`** (see below). `requireRole` composes over
`requireSession` (401 first, then 403).

**Optimistic concurrency + single-active invariant (no lost updates).** Every edit body
carries `expected_config_version`. In the edit transaction, configstore **locks ALL active
`config_version` rows `FOR UPDATE`** (no `LIMIT`) and requires **exactly one**: zero →
`ErrNoActiveVersion`, **two-or-more → `ErrMultipleActiveVersions`** (→ 409, edit refused —
no new version, no config change, no misleading audit — rather than silently picking one),
one → compare to `expected_config_version`. A mismatch → `ErrStaleConfigVersion` → **409
Conflict** (tx rolls back, previous version stays active). Concurrent editors who both
loaded *V* are serialized by the row lock: exactly one commits (activating *V+1*), the loser
re-reads *V+1 ≠ V* and gets 409 — two tabs can never silently overwrite each other. The
target config row is also locked `FOR UPDATE`. A transient InnoDB deadlock on this hot row
is retried (`withTxRetry`) so the loser converges to a clean 409, never a 500. A
missing/zero `expected_config_version` is rejected (400 — mandatory).

**Trading ⟹ sell-management (invariant).** `enabled_for_trading=true` while
`enabled_for_sell_manage=false` is **rejected (400)** — enforced on the *effective*
post-change state, so it also blocks "disable sell-manage" on a market whose trading stays
on. Otherwise the engine could open new buys the sell manager ignores, stranding inventory.
So to turn sell management off you must also turn trading off in the same edit. Safe
combinations: `trading=false, sell_manage=true` (stop new buys, keep managing open cycles)
and `trading=false, sell_manage=false` (allowed only with no open exposure, below).

**Sell-management disable guard (execution safety).** Disabling `enabled_for_sell_manage`
for a market that still has **open/unresolved exposure** — any cycle in
`BUY_REQUEST_QUEUED`/`BUY_SUBMITTED`/`BUY_PARTIALLY_FILLED`/`BUY_FILLED`/
`SELL_REQUEST_QUEUED`/`SELL_SUBMITTED`/`SELL_REPRICE_PENDING`/`SELL_PARTIALLY_FILLED`/
`SELL_FILLED`/`CANCEL_PENDING`/`NEEDS_RECONCILE` — is rejected inside the transaction with
`ErrSellManageExposed` → **409** ("sell management cannot be disabled while open exposure
exists"), leaving the flag/version/lock unchanged. The queued/submitted **buy** states are
deliberately included: a buy that is queued or submitted can fill at any moment, so
disabling sell management first would strand that fresh inventory. Additionally, the two
**high-risk** transitions — **enabling trading** (`enabled_for_trading=true`) or **disabling
sell management** (`enabled_for_sell_manage=false`) — require the **admin** role
(config_operator → 403).

**Buy creation re-checks the LIVE flags on the SAME row, in the SAME lock order (race- and
deadlock-free).** Both paths take locks in ONE order — **`exchange_markets` row first, then
the active `config_version`**: `buyflow.CreateBuyCycle` step 0 re-reads the market row
`FOR UPDATE` (then takes the `config_versions` FK lock when it inserts the cycle with
`config_version`), and `UpdateMarketFlags` locks the market row `FOR UPDATE` before its
active-version lock. Matching the order avoids the ABBA deadlock a config edit and a buy
would otherwise hit. `CreateBuyCycle` requires **both** `enabled_for_trading` and
`enabled_for_sell_manage` true, else `ErrMarketNotTradable` (a clean engine no-op — no
cycle/order/request/lock/audit). So they serialize cleanly: if a config edit disabled
trading/sell-management first, the buy sees it and refuses (a stale in-memory config can't
open a buy after the DB flags were turned off); if the buy commits first, the edit sees the
open cycle and its exposure guard rejects the disable. A concurrency test drives the real
`UpdateMarketFlags` against `CreateBuyCycle` over many rounds (both orderings) with zero
deadlocks and zero orphans.

**Validation — offsets can't make a non-positive price.** `sell_offset_bps` and
`maker_price_offset_bps` are bounded to **[0, 10000)**: the price is
`reference × (1 − offset/10000)`, so `10000` → zero and `>10000` → negative, which would
break sell/maker-buy creation on already-acquired inventory. (Plus the earlier rules:
min_spread≥0, buy_size>0, unit∈{base,quote}, timeouts>0, retries≥0, taker_mode=ASK, fees≥0.)

**Fee edits — no swallowed errors, ownership validated, no-op-safe, audited by market.**
`UpsertFee` treats only `sql.ErrNoRows` as "no previous fee"; any other read/scan error aborts
the tx **before** activating a version, writing a fee, or auditing. A market-specific fee's
`exchange_market_id` must **belong to** the given `exchange_id` (verified in-tx) — a market of
another exchange is rejected (400) with no version/audit. **No-op detection is by DECIMAL
value** (`0.001` == `0.00100000`): resubmitting unchanged fees returns `ErrNoChanges` (400)
**before** activating a version — no version/fee/audit, so it can't advance the version and
force a spurious `409` on another editor. Audit rows are written **per changed field only**
(maker-only edit → one `maker_fee` row). The audit **entity uniquely identifies what changed**:
an exchange-wide default fee is `entity_type=exchange_default_fee`, `entity_id=exchange_id`; a
market-specific fee is `entity_type=exchange_market_fee`, `entity_id=exchange_market_id` (so two
markets of one exchange get distinct audit rows; the exchange id travels in the reason).

**Mandatory reason + trustworthy audit.** Every edit requires a **non-empty, non-whitespace
`reason`** (else 400). Each edit runs in ONE transaction: activate a new `config_version`,
update only provided fields, and write a `config_change_audit` row **per changed field**
(config_version, entity_type, entity_id, field, **real** old_value/new_value, `changed_by`,
reason, timestamp). **`changed_by` is ALWAYS the authenticated session user — never a
client-supplied value**; a `changed_by` field in the body is rejected as an unknown field.
The new `config_version` is stamped on the edited row where the table has the column
(`symbol_configs`/`exchange_configs`/`exchange_fees`); `exchange_markets` has none, so the
version lives on `config_versions` + audit. An edit with no real change → 400 (`ErrNoChanges`).

**Strict JSON parsing.** Bodies are decoded with `DisallowUnknownFields()` and must contain
**exactly one** JSON object — a second decode must be `io.EOF`, so a trailing object or
trailing garbage → 400. Body size is bounded.

**Validation before write (→ 400).** `min_spread_bps ≥ 0`; `buy_size > 0`;
`buy_size_unit ∈ {base,quote}`; **`0 ≤ sell_offset_bps < 10000`** and
**`0 ≤ maker_price_offset_bps < 10000`** (≥10000 → zero/negative price);
`reprice_interval_seconds ≥ 0`; `order_timeout_ms > 0`; retries ≥ 0; maker fields sane;
`taker_price_mode = ASK`; exchange `max_concurrent_requests > 0`, `request_timeout_ms > 0`;
fees non-negative. Enable-flag hierarchy `enabled_for_trading ⊆ enabled_for_signal ⊆
enabled_for_collection` enforced, **plus the `enabled_for_trading ⟹ enabled_for_sell_manage`
invariant** — trading may not stay on while sell management is off (disabling trading alone
still keeps managing open cycles).

**Editable surfaces (POST, config_operator+):** symbol config (`/api/config/symbol/{em}`),
market flags (`/api/config/market/{em}/flags`), exchange config (`/api/config/exchange/{ex}`),
fees (`/api/config/fee`). Read: audit history `GET /api/audit`, current user `GET /api/me`.
(Regime-basket editing and a credential-editing surface are deferred to their own later PRs
— PR17 ships no route for them, and there is NO order/cycle/queue/lock/credential mutation.)

**Hot reload (no restart).** Services pick edits up through their periodic reloads
(trade-engine `configstore.Cache`), so edits apply without restarting any service. Active
cycles keep their **stamped `config_version`** and are never rewritten; new cycles use the
new active version.

**WebSocket.** Unchanged from PR16 (snapshot-only, same-origin, `snapshot_error` on DB
failure, no commands) — and now, like every other route, it requires a logged-in session.

## 15. Market regime (implemented in PR15 — `internal/regime`)

The regime calculator scores **DB-configurable baskets** of Binance symbols into a
normalized market regime. It reads Binance prices **only from the Redis market-data
cache** (it never calls Binance — no REST/WS, no clients), reads basket config from
MariaDB, and writes regime current/history to MariaDB. It makes **no** trading
decisions and never touches cycles/orders/queue/locks/trading-config (a later
trade-engine PR may consume the regime; PR15 only computes + stores it). Migration 015
added the basket/symbol/timeframe/current/history tables; 016 added `state_hash` to
current; **027 added `state_hash` + `stale_reason` to history and the config CHECK
constraints**.

**Config model (nothing hardcoded).** `market_regime_baskets` (name, enabled,
`update_interval_seconds`, `neutral_band_bps`/`moderate_threshold_bps`/
`strong_threshold_bps`, `config_version`) + `market_regime_basket_symbols`
(binance_symbol, weight, enabled) + `market_regime_timeframes` (label, seconds,
weight). Baskets are configured via DB (dashboard later); the calculator computes
nothing until a basket is configured.

**Config validation (rejected in BOTH layers).** Invalid config must never produce a
regime result, so the rules are enforced twice:
- **DB (migration 027 CHECK constraints):** `update_interval_seconds >= 0`,
  `neutral_band_bps >= 0`, `moderate_threshold_bps >= neutral_band_bps`,
  `strong_threshold_bps >= moderate_threshold_bps`, symbol `weight > 0`, timeframe
  `seconds > 0` and `weight > 0`. A bad row can't even be inserted.
- **Code (`Basket.Validate`, called by `LoadBaskets`):** the same rules; `LoadBaskets`
  returns `ErrInvalidBasketConfig` (never a partial/garbage basket) so the calculator
  skips the pass rather than computing from bad config — defence in depth for any row
  that predates the constraints or was hand-edited. (Admin edits in §14a validate on the
  write path too.) A basket with no *enabled* symbols/timeframes is **not** a config
  error — it simply degrades to `UNKNOWN`.

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

**Current + history (both self-describing).** `market_regime_current` is upserted per
basket (idempotent for a genuinely identical regime); `market_regime_history` gets a row
whenever the regime **changes**, where "changed" is a **full-field content hash**
(`state_hash`) over direction, level, confidence, score, per-timeframe scores, per-symbol
contributions, stale_reason, and config_version — **not** just the direction/level label.
So `BULLISH/STRONG @ confidence 0.35 → 0.90` records a new history row, and an `UNKNOWN`
whose `stale_reason` changes records one too.

Crucially, **history stores the same full-field payload as current, including
`state_hash` and `stale_reason`** (migration 027 added both to `market_regime_history`;
migration 016 added `state_hash` to current). Before that, history held only
direction/level/confidence/score/timeframe_scores/symbol_contributions — so it could show
*that* the regime changed but not *what* changed (e.g. an `UNKNOWN` whose `stale_reason`
evolved). Now every history row is self-describing: its `state_hash` equals the
`market_regime_current.state_hash` at that evolution point, and its `stale_reason`
explains a degraded/`UNKNOWN` entry. `WriteResult` only treats `sql.ErrNoRows` from the
`SELECT state_hash` change-check as "no previous state → changed"; any **other** select
error is returned (never swallowed into a misleading history insert). Both tables are
config-version-stamped; history is high-volume, timestamp-indexed, no FK on the write path
(retention friendly).

**Dashboard interpretation (current vs history).** `market_regime_current` is the *latest*
regime per basket — read it for "what is the regime now" (direction/level/confidence/score
+ `stale_reason` when degraded; a stale-data `UNKNOWN` is never shown as a still-valid
regime). `market_regime_history` is the *append-only evolution* — one row per distinct
`state_hash` — read it for a timeline of how the regime changed, including confidence/score
drift and changing `UNKNOWN` reasons. Two history rows differ iff their `state_hash`
differs, so the dashboard can diff/group by it.

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

**Config-driven with SAFE BOUNDS (nothing hardcoded, nothing unbounded).** Per-table
`retention_settings`: `enabled`, `retention_days`, plus `batch_size` /
`max_batches_per_run` / `pause_ms` (migration 018). A missing row, NULL days, or
`retention_days < 1`, means **not configured → do nothing** (a deletion window is never
guessed); `enabled=0` → skip. Every setting has a **documented safe range**, validated at
runtime and mirrored as DB CHECK constraints (migration 029): `retention_days ∈ [1, 3650]`
(≤10 years), `batch_size ∈ [1, 50000]`, `max_batches_per_run ∈ [1, 10000]`, `pause_ms ∈
[0, 60000]`. A value out of range is a **per-table validation error → that table is
skipped (no DELETE), the error is recorded, and the other tables continue** — never
silently replaced with a default.

**Batched deletes (never one huge delete; overflow-safe cutoff).** Per table the cutoff is
**`start.AddDate(0, 0, −retention_days)`** — calendar subtraction, so a large day count can
never overflow `time.Duration` and push the cutoff into the future (matching almost every
row). Then `DELETE … WHERE <ts> < cutoff LIMIT batch_size` repeated up to
`max_batches_per_run`, stopping early on a short batch, with an optional `pause_ms` between
batches — avoiding long locks / replication pain.

**One pinned connection for the whole run.** The run pins **one `*sql.Conn`** and does
**everything on it** — `GET_LOCK('v3tradebot_retention')`, load settings, dry-run counts,
the batch DELETEs, and the app_logs report — then `RELEASE_LOCK` on that same connection
**before** it is closed. So the advisory lock and the DELETE work can never land on
different pooled connections: it is correct even with `SetMaxOpenConns(1)` (no self-hang),
and the connection-scoped lock stays tied to the connection doing the deletes (a second
worker can't overlap). The lock is released on every exit path (including errors) via
`defer` on the pinned conn.

**Dry-run.** A dry-run (the `-dry-run` command-line flag — an execution mode, never a
runtime env var) reports per table the cutoff, configured batch size, and **estimated
rows** (`COUNT(*) WHERE <ts> < cutoff`) and deletes **nothing**.

**Lock-skipped runs are still reported + logged.** If the lock can't be acquired (another
worker active), the run returns a **completed** report with `LockAcquired=false` and
`SkippedReason="another retention run is active"` and **still writes an `app_logs` entry** —
so operators can distinguish "did not run" / "ran and skipped (busy)" / "ran and failed" /
"ran successfully".

**Failure behaviour.** One table's error is recorded in its result and the run continues to
the others (or stops if `StopOnError`); batches are bounded (no infinite retry); a cancelled
context stops cleanly between batches/tables. **When any table errored, the run's `app_logs`
summary is logged at `warn`** (not a clean-success `info`), so a partial failure is visible.

**Observability.** Every run writes a summary to `app_logs`
(`source_binary='retention-worker'`, no secrets): start/finish, duration, dry-run flag,
lock-acquired + skipped-reason, and per-table {cutoff, configured/enabled, deleted count,
batches, estimated (dry-run), skipped reason, validation/error}. Level is `info` on success,
`warn` when any table errored.

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

**Activation (config-driven, safe by default, STRICTLY VALIDATED — never a runtime env
var).** The bootstrap `[execution] mode` must be **EXACTLY** one of `off`|`dry_run`|`live`:
an empty value normalizes to `off`, but any other value (a typo like `dryrun`, `DRY_RUN`,
`simulate`) is a **hard startup error** — it is never silently treated as `off`
(`config.Validate`). Both `config.example.toml` and `production.example.toml` ship an
explicit `[execution] mode = "off"`.
- **`off` (default)**: the trade-engine is wired with `PrepareBuyCycles=false`, so it is
  **strictly signal-only** — it consumes market data, compares, and records
  comparison_events/signals, but creates **NO cycle / order / PLACE_ORDER request /
  symbol lock** (it never calls `buyflow.CreateBuyCycle`). The order-executor wires no
  clients. So a safe `off` deployment never fills the DB with executable trading state a
  later mode change could pick up. Dry-run/live must be **explicit**.
- **`dry_run`**: the order-executor wires `simexec` clients and the trade-engine both
  prepares buys and stamps created cycles `dry_run=1`.
- **`live`**: real private clients + the PR20 safety stack.

**Strict dry-run/live SEPARATION (two guards).** A dry-run executor must process only
requests whose cycle is `dry_run=1`, a live executor only `dry_run=0` — enforced twice:
(1) at **queue claim** — `queue.Claim` takes a mode filter and joins `cycles`, so a
mismatched request is never claimed (and the in-flight slot count is mode-scoped too, so a
dry and a live executor sharing an exchange don't contend); (2) a **final pre-send check**
immediately before every `PlaceOrder`/`CancelOrder` (`abortOnModeMismatch`) — if the
cycle's `dry_run` doesn't match the executor mode it refuses to send and leaves the request
**untouched** (never marked failed/sent/completed; the sweeper reverts it for the
correct-mode executor). So `simexec` can never mark a real cycle filled, and a live client
can never send for a dry-run cycle. The pre-send check **fails closed** (PR19 round 2):
`cycleDryRun` returns an error on any DB error / missing cycle / invalid-or-NULL `dry_run`,
and the guard then refuses to send and leaves the request recoverable — "cannot confirm the
cycle's mode → do not call the exchange". The final live gate applies the same fail-closed
check before a real send.

**Simulated client (`simexec`, no I/O, PERSISTENT).** It implements
`exchanges.PrivateClient` with **no network code at all**. Its order state is stored in
**`sim_exchange_orders` (migration 030)**, not a process-local map: every simulated
PlaceOrder is recorded (keyed by the deterministic `SIM-<client_order_id>` AND by
`client_order_id`, with an explicit mutable lifecycle `status`+`filled_quantity`), so ANY
executor instance — a different process, one after a restart, or the reconciler — can look
it up (by exchange_order_id OR client_order_id) and observe the SAME deterministic state. A
follow-up `GET_ORDER` handled by a different instance therefore resolves correctly instead
of returning `ErrOrderUnknown` and spuriously pushing a dry-run cycle to `NEEDS_RECONCILE`.
Base scenarios: `full_fill`, `partial_fill` (half, remainder cancelled), `zero_fill`,
`ambiguous` (deliberately GetOrder-unknown → NEEDS_RECONCILE), `rejected` (definite place
rejection), `cancel_race`. The dry-run binary default is `full_fill`.

**Immutable idempotency.** A re-placed identical `client_order_id` returns the first
accepted order deterministically (no second row — an accepted order is immutable, enforced by
`UNIQUE(exchange_code, client_order_id)`); the same id with a **different** payload is a hard
conflict, never an overwrite.

**Ambiguous-execution recovery (PR19 round 2).** On Iranian venues an order or cancel may
succeed at the exchange while the HTTP response times out. A timeout is treated as an UNKNOWN
outcome — never a success, never a failure, and **never blindly retried**. The simulator
models this faithfully and the executor recovers it read-only:

- *Accepted-but-timed-out PLACE* — `place_timeout_accepted_{open,partial_fill,full_fill}`
  PERSIST the order first (with its real state/fill), then return `ErrAckTimeout` **without**
  the exchange order id; `place_timeout_not_accepted` (and the legacy `place_timeout` alias)
  persist nothing. The executor's ambiguous-place path DEAD-letters the place (consumed, never
  re-sent) and schedules a **read-only `ambiguous_place_probe` GET_ORDER** that looks the order
  up **by `client_order_id`** (the exchange id was never returned). Found → resume the normal
  ack flow (`OnPlaceAck`/`OnSellPlaceAck`, which drives the cancel → final-status path that
  records fills); provably-not-placed (`ErrOrderUnknown`) → resolve cleanly (buy: FAILED + lock
  released, no exposure; sell: NEEDS_RECONCILE + lock held); transient → bounded read-only
  retry, then NEEDS_RECONCILE.
- *Ambiguous CANCEL* — `cancel_timeout_{but_canceled,still_open,partial_then_canceled,`
  `filled_before_cancel}` mutate the persisted state then return `ErrAckTimeout`. The executor
  DEAD-letters the cancel and schedules a read-only `ambiguous_cancel_probe` GET_ORDER (by the
  known exchange id). Terminal (canceled/partial/filled) → record the ACTUAL filled qty and
  continue via `OnCancelResult`/`OnSellCancelResult` → final status; **still open** (cancel
  didn't take) → re-issue the cancel, **bounded** (a proven re-cancel from a read-only check,
  never a blind resend) — after `maxRecoveryAttempts` it goes to NEEDS_RECONCILE. Fill
  recording is idempotent (deterministic `exchange_fill_id` + `UNIQUE(order_id,
  exchange_fill_id)`), so a re-run or a concurrent instance cannot double-apply a fill.

**Ambiguous-execution HARDENING (PR19 round 3).** Eight refinements make the recovery safe against
eventual consistency, crashes, adapter quirks, and cross-instance drift:

- **A first "not found" is never proof of non-placement.** After a place timeout the probe looks
  the order up and, on `ErrOrderUnknown`, does **bounded read-only retries with backoff** (the
  simulator's `hidden_probes` models a venue where an accepted order is briefly invisible, then
  appears). The symbol lock stays HELD and the cycle is NOT failed on an early miss. Only after the
  bounded retries are exhausted AND the venue declares a **reliable negative**
  (`Capabilities.ReliableNotFound`, false for real Iranian venues) is the order classed
  "provably-not-placed" and failed cleanly; otherwise it goes to NEEDS_RECONCILE (lock held).
- **"Accepts a client id on place" ≠ "can look an order up by client id".** These are separate
  capabilities: `ClientOrderID` (accepted on placement) vs **`LookupByClientOrderID`** (GetOrder
  can resolve an order by client id). All three private venues set `LookupByClientOrderID: true`,
  each via its own mechanism: **Wallex** — GetOrder IS keyed by the client id; **Bitpin** —
  `GET /odr/orders/identifier/<id>/` resolves by the `identifier` (= our client id); **Nobitex** —
  `GetOrderByClientOrderID` lists recent orders and matches the reliable `clientOrderId`. Recovery
  goes through the `ClientOrderLookup.GetOrderByClientOrderID` capability and never passes a client
  id into an exchange-id-only endpoint, nor treats a "not found" as proof of non-placement.
- **Crash-after-send recovery.** If the process crashes after the exchange accepted a PLACE/CANCEL
  but before the response was handled (so no `ErrAckTimeout` probe was persisted), the executor's
  sweep (`recoverStaleMutating`) converts each stale IN_FLIGHT mutation — mode-scoped — into a
  persisted READ-ONLY recovery probe (place by client_order_id, cancel by exchange id) and
  DEAD-letters the original mutation. Never a blind resend.
- **The EXACT sent client id is persisted before the network call.** The executor computes the
  adapter's `ClientOrderIDForSend(local)` (e.g. Nobitex's 32-char truncation), COMMITS it to
  `client_order_id_sent`, THEN sends exactly that value. Recovery looks the order up by
  `client_order_id_sent` (not the raw local id), and a recovered order whose immutable fields
  (side/quantity/symbol) disagree is NEVER attached — it goes to NEEDS_RECONCILE.
- **The simulator is deterministic across restarts/instances.** `CancelOrder`/`GetOrder` behave per
  the order's OWN persisted scenario (and stored immutable fields), NOT the current client
  instance's configured scenario — so a restarted/second instance with a different default never
  changes an existing order's behaviour.
- **A mutating request can never reach the exchange without a cycle + order.** Enforced at runtime
  (the right layer for the generic queue): the mode-scoped claim only claims a mutating request
  whose cycle's dry_run matches the executor, and a pre-send fail-closed guard FAILS a PLACE/CANCEL
  with a missing cycle_id/order_id without any exchange/simexec call.
- **Terminal recovered states are handled directly.** A probe that finds the order already
  filled/canceled/partially-canceled records the fills immediately (no redundant cancel or extra
  GET_ORDER); an open order continues the cancel flow; a rejected order takes the terminal-failure
  path. Recovery avoids redundant API calls and queue rows.
- **The claim index is justified on the real 10.6 plan.** `idx_exreq_claim (exchange_id, status,
  priority, id)` serves an index-ORDERED per-status scan; the single-query status OR
  (`QUEUED OR RETRY_SCHEDULED`) does a small filesort over the bounded candidate set, but at 3334
  candidates that measured ~0.1 ms (a split-query variant was only 1.06x — within noise), so the
  claim is left as-is rather than adding locking/merge complexity to the safety-critical path.
  `idx_cycles_dry_run_state` (031) IS used by the claim's dry_run subquery (EXPLAIN-verified) — kept
  on evidence, not on its name.

**Ambiguous-execution HARDENING round 4 (PR19).** Five further refinements: (1) stale-mutation
recovery (`recoverStaleMutating`) is ATOMIC — a `SELECT … FOR UPDATE SKIP LOCKED` claim + status
re-check, schedule probe, mark DEAD, all in one tx — and the generic `SweepStuck` no longer touches
mutating IN_FLIGHT, so exactly one probe is ever created and the order is never prematurely
reconciled; (2) client-id recovery goes through a dedicated `ClientOrderLookup.GetOrderByClientOrderID`
(Wallex client-id GetOrder, Bitpin `?identifier=`, Nobitex list-recent-orders + match the reliable
`clientOrderId`), never a client id into an exchange-id endpoint; (3) the simulator looks the order
up FIRST and replays the order's OWN persisted scenario (never the current instance's), so behaviour
is deterministic across restarts/instances; (4) a recovered order is identified by its reliable
identifier + symbol + side (quantity is a sanity signal, NOT an exact-identity requirement — venues
round and partial fills are smaller); (5) recovery timing is per-exchange configurable
(`RecoveryConfig`: max_attempts / initial_delay / max_delay / total_timeout) with bounded exponential
backoff + jitter — the window closes on either bound → NEEDS_RECONCILE with the lock HELD and no
blind resend. The exact sent client id is committed before the network call and that persist verifies
EXACTLY ONE row updated (else fail closed).

**Recovery window is RUNTIME-CONFIGURED (PR19 round 4 correction).** The window is not
hard-coded: the bootstrap `[execution.recovery]` section (`max_attempts`, `initial_delay_ms`,
`max_delay_ms`, `total_timeout_ms` — safe defaults 6/1s/30s/5m) plus
`[execution.recovery.per_exchange.<code>]` partial overrides are parsed by `internal/config`,
VALIDATED AT STARTUP (each resolved window must have max_attempts>0, initial_delay>0,
max_delay>=initial_delay, total_timeout>0, and stay within hard safety bounds: ≤100 attempts,
≤1h max_delay, ≤24h total_timeout; negatives and violations are hard startup errors), and wired
by `cmd/order-executor` (`recoveryFromConfig`) into `executor.Config.Recovery`/`RecoveryPerExchange`.
A PARTIAL per-exchange override inherits every unset field from the CONFIGURED global values —
in both the config resolution and the executor's own merge — never from hard-coded defaults.
EVERY unresolved cancel-probe outcome — a transient lookup failure (timeout / network /
rate-limit / retryable 5xx / context deadline or cancellation) exactly like a proven still-open —
consumes an attempt of this SAME persisted window (attempt counter + `FirstProbeAt` wall-clock +
jittered exponential backoff), NEVER the generic queue retry policy (`retry_count`/`max_retries`
stay untouched on probe rows). Window exhaustion marks the probe DEAD and the order/cycle
NEEDS_RECONCILE in ONE transaction (`deadReconcile`) — no crash window between them.

**Does not bypass the architecture.** Dry-run does **not** mark cycles closed from the
engine — every transition goes through the same queue → executor → `internal/orders`
boundaries. The engine only sets the `dry_run` marker.

**Dashboard labelling.** Cycles carry `dry_run`; the orders/requests/fills views surface
it via a join to the cycle, so the dashboard clearly shows `DRY_RUN`.

**Reconciler holds BOTH client sets and never mixes them (PR19 round 2).** `cmd/reconciler`
builds **two** read-only client maps — **simulated** (DB-backed `simexec`, always available;
no network, no credentials) and **real** (credentialed read-only clients where a master key
+ credential exist) — and the reconciler routes STRICTLY by each cycle's `dry_run` flag
(`clientFor(dryRun, code)`): a `dry_run=1` cycle is verified ONLY through a sim client, a
`dry_run=0` cycle ONLY through a real client. It is therefore impossible to query a real cycle
through the simulator, query a dry-run cycle through a real client, send a `SIM-*` id to a real
exchange, or query a real id through the simulator — regardless of the process's own execution
mode (a live deployment still reconciles a stray dry-run cycle correctly, and vice-versa). When
the correctly-scoped client is absent the order is **skipped** (`NoAction`, cycle state
untouched) — never verified through the other mode's client. The reconciler holds every client
through the `ReadOnlyClient` interface (no place/cancel reachable) and loads `cycles.dry_run`
(migration 019). Because both sim and real cycle orders can be recovered by `client_order_id`,
migration 030 adds `UNIQUE(exchange_code, client_order_id)` and migration 031 an
`idx_cycles_dry_run_state` for mode-scoped scans.

## 16c. Limited live execution (implemented in PR20 — `internal/live`)

PR20 is a **safety PR**, not a wiring PR: it enables real live orders **only** under
strict, explicit caps + a global kill switch + per-exchange/per-symbol live flags +
credential availability + an audit trail, with the final gate **inside the
order-executor** (never relying on the engine alone). Safe by default at every layer.

**These controls sit in front of REAL exchange mutations.** The accepted parent already
contains the real-client wiring (§16d, PR20a: encrypted-credential loading, in-memory
decryption, real adapters via the factory) and PR22's provisioning. So in `live` mode
`cmd/order-executor` **builds real private clients with decrypted credentials and can
perform real sends** — every PLACE/CANCEL below is a real venue mutation the moment all
guards pass. Nothing here is a harness: PR20's guard is the last thing between the queue
and a real order. (An earlier draft of this section described the wiring as "deferred to
PR20a" with "no real client"; that is obsolete and is corrected here.) The only reasons
live mode still sends nothing are operational, not architectural, and are listed under
"Current limitations" below.

**Activation rules.** Bootstrap `[execution] mode` must be **explicitly** `live` (default
`off` is safe; `dry_run` keeps using `simexec`). No default live behaviour, no runtime
env var. Beyond the mode, live trading does **not start** unless the DB controls are
configured (see caps) and the kill switch is disengaged.

**Cap model (`live_controls` singleton + per-scope flags; migration 020).** The
`live_controls` row holds the global caps; **every required cap must be set** — if any is
missing (`Configured()` false) live entries are denied. Caps: `max_open_cycles`,
`max_order_notional`, `max_base_qty`, `max_consecutive_failures`,
`max_unresolved_reconcile`. **OWNER DECISION (PR20 correction): there are NO daily trading
limits** — the historical `max_daily_orders` / `max_daily_quote` columns remain in the
table (dropping them would be a needless destructive migration) but are not read, not
required, and never influence a buy or sell decision. Scope is opt-in via
`exchanges.live_enabled` and `exchange_markets.live_enabled` (both default 0). The kill
switch (`live_controls.kill_switch`) **defaults engaged (1)**. **SINGLE-INSTANCE DESIGN
(owner decision): exactly one bot instance runs** — cap checks are plain reads with no
cross-process reservation/coordination layer, deliberately; DB persistence and restart
safety are unchanged.

**A guard denial never strands a cycle (PR20 correction).** A refused request must not leave
`order=QUEUED` + `cycle` open + `lock` HELD with no executable request — that is a permanently
stuck cycle. Every denial (nil Guard, unconfirmable mode, unresolvable market, or the guard's
own verdict) resolves through the OFFICIAL state path, chosen by what is at risk:
- **entry buy denied** — nothing was sent and no exposure exists → `rejectBuyCleanly`: request
  FAILED, order + cycle FAILED, **symbol lock RELEASED**.
- **exit sell denied** — real inventory may exist → `rejectSellToReconcile`: request FAILED,
  order + cycle **NEEDS_RECONCILE**, **lock HELD**. The position stays visible and recoverable,
  never abandoned.
- **cancel denied** — the venue order may be OPEN → `deadReconcile`: request DEAD, order +
  cycle **NEEDS_RECONCILE**, **lock HELD**, so the open order stays under explicit management.

**Proven entry buys (PR20 correction).** Before a real buy, `checkEntryBuyIdentity` runs ONE
authoritative query (`loadSendOrder`) joining the order → its cycle → its `exchange_market` →
that market's exchange, so a missing relationship yields NO ROW and therefore a denial. It
proves together, from the database and never from the payload: `role='entry_buy'`,
`state='QUEUED'`, `dry_run=0`, the order belongs to the REQUEST's cycle AND exchange, the market
belongs to that same exchange, the caller-resolved market matches the order's, the request
symbol equals the registered market's canonical symbol, and both exchange and market are
live-enabled. Any query error, missing row, NULL, or mismatch → **no PlaceOrder call**. The
executor's own market lookup (`orderMarket`) returns an error instead of a silent `0`: market
id 0 previously slipped past the symbol-level live-enabled check, so an unidentifiable market
could reach a real send.

**The payload that will be SENT must equal the persisted order (PR20 correction).** Identity is
not enough: the queued mutation payload and the registered order are two INTERNAL values, and
if they disagree we do not know what we are actually placing (a stale, mis-routed, or tampered
payload). So `matchPayloadToOrder` additionally proves, for BOTH entry buys and exit sells:
quantity, limit price, order type, **time-in-force with EXACT NULL semantics** (a DB NULL/empty
TIF means the strategy chose the venue default — the payload must then ALSO be empty; a non-empty
payload TIF like `FOK` is a mismatch; a DB-recorded TIF must match exactly), local client order
id, and that the side agrees with the order's role. The exit-sell **symbol is mandatory** (an
empty payload symbol is an unproven route, not "no opinion" — it must be present AND equal the
registered market's canonical symbol). This is **not** venue-response matching (§10d), which
deliberately tolerates venue rounding/normalization — here no tolerance applies, the two internal
values must be equal. Comparison is by decimal VALUE, so a `DECIMAL(36,18)` round-trip (`0.50`
vs `0.5`) is not a spurious mismatch, and it is exact — never a float epsilon. The executor
builds the `execution.OrderRequest` FIRST and derives the checked payload from it
(`buildPlacePayload`), so the guard proves precisely the values `PlaceOrder` will send rather
than a parallel re-derivation that could drift.

**The EXACT sent client-order-id is validated, persisted, and sent unchanged (PR20 correction).**
Some adapters normalize/truncate the client id (`ClientOrderIDForSend`), so the value SENT can
differ from the intent's local id. The flow prepares that value BEFORE the final guard and never
transforms it afterwards: normalize → verify non-empty (an empty normalization fails closed, no
send) → set it on the `OrderRequest` → **persist it to `client_order_id_sent` before the final
guard** → the final guard proves the payload's `ClientOrderIDSent` equals the persisted column →
`PlaceOrder` sends exactly that value. The early guard leaves the sent-id empty (nothing is
persisted yet); the final guard, which runs after persistence, enforces it — so the value proven
and stored is the value the venue receives.

**Proven cancels (PR20 correction).** A queue payload's `exchange_order_id` is never trusted on
its own. `checkCancelTarget` proves from the DB that the order belongs to this request's
exchange AND cycle, that its stored `exchange_order_id` is present and EQUAL to the payload's,
and that its state is one a cancel can legally act on (`SUBMITTED`/`ACKED`/`PARTIALLY_FILLED`/
`CANCEL_PENDING`/`NEEDS_RECONCILE` — never `QUEUED`, which never reached the venue, and never a
terminal state). Any missing/unreadable/mismatched field → **no CancelOrder call**.

**Kill switch + entry/exit separation (PR20 correction).** When engaged: no new buy cycle
may start (engine `AllowNewBuyCycle` denies) and no new **buy** PLACE may be sent (executor
gate denies). Risk-reducing paths continue: a **sell** PLACE that PROVABLY exits
already-acquired inventory, CANCEL, and GET_ORDER/status remain allowed so open cycles are
safely managed. A sell is **never blindly classified risk-reducing**: `checkExitSell`
proves it from DB state — exact order/cycle/exchange ownership (`role='exit_sell'`), the
order is QUEUED (state-machine legality), the FULL payload equals the registered order
(quantity/price/type/TIF/client-id/side — see above), the owning cycle is real (`dry_run=0`),
the cycle's entry buys actually FILLED a positive quantity, and the sell fits the remaining
inventory. It also proves the sell is routed to the market where the inventory was ACQUIRED
(`exitMarketMatchesInventory` compares the sell's exchange+market+symbol against the cycle's
filled entry buy): a sell in the wrong market or symbol is not a risk-reducing exit — it opens
NEW exposure somewhere we hold nothing. **The oversell math counts fills from CANCELLED sells (PR20
correction):** a sell that partially filled before being cancelled has removed those units
forever, so excluding an order by its final state would permit an oversell. The condition is

    already_sold + remaining_active_commitments + this_request <= acquired_inventory

where `already_sold` sums `filled_quantity` across ALL other exit sells of the cycle (any
state, cancelled included) and `remaining_active_commitments` sums `quantity - filled_quantity`
for only those that can still execute (a terminal order's remainder can never execute and
contributes 0; `NEEDS_RECONCILE` is deliberately counted as still-active because its remainder
may be resting on the venue). Worked example: bought 1.0, a cancelled sell filled 0.4 → a new
1.0 sell is DENIED (would total 1.4) while a 0.6 sell is allowed (totals exactly 1.0). Proven exits are deliberately exempt from entry-side controls
(kill switch, open-cycle cap, canary ack, configured entry caps, live-enable flags,
failure/reconcile counters): blocking a proven exit strands real inventory and increases
risk.

**Executor-side live guard (the load-bearing gate).** The final live check is in
`order-executor`, immediately before each mutating send (`gatePlace` / `gateCancel`, run as
an early pre-pacing pre-filter and a FINAL guard immediately before `MarkInFlight`). Before a
real PLACE/CANCEL the `live.Guard` verifies: mode is `live`,
`AllowLiveExecution` true, request **not** dry-run, exchange + symbol live-enabled (buys),
caps pass (notional/qty/open-cycles/consecutive-failures/unresolved-reconcile — buys),
active credentials exist, kill switch off (for buys), and the order/cycle state is still
valid. A denial **fails the request without sending** and is audited. The engine performs
a first `AllowNewBuyCycle` check; the executor re-checks — belt and suspenders.
PR20 corrections hardening this gate:
- **live mode + nil Guard fails CLOSED** — an unguarded live executor denies every
  place/cancel (the old `Guard == nil → allow` hole is gone);
- **every safety query fails CLOSED** — `openCycles`, `unresolvedReconcile`,
  `consecutiveFailures`, `credentialsAvailable`, `live()` all return their errors
  explicitly and ANY database error denies the live operation (a condition that cannot be
  evaluated never reads as \"0, therefore fine\");
- **durable allow-audit** — the ALLOW for a real PLACE is committed to `live_audit`
  BEFORE the send; if the audit insert fails the decision flips to DENY (fail closed). A
  risk-reducing CANCEL uses the opposite policy: an audit outage never blocks it — the
  decision is logged (safe fields) for later reconstruction instead.

**No blind resend (unchanged).** All prior safety holds in live: `MarkInFlight` commits
before the send; an ambiguous mutating result → order/cycle `NEEDS_RECONCILE`, request
`DEAD` (never re-sent); a missing order is not proof of zero fill; an `IN_FLIGHT`
mutating timeout is never blindly retried.

**Comprehensive exchange rate-limit detection (PR20 correction; rules ported from the
owner's proven iranArb system).** Rate limiting is NOT detected from HTTP 429 alone. Each
adapter normalizes every documented throttle signal into a `NormalizedAPIError` with
`Category=rate_limit` plus STRUCTURED metadata (`exchanges.RateLimitInfo`): `retry_after`
(venue-provided wait), `source` (status | header | body | code), the venue `code`, and
`definite_rejection` — true ONLY when the venue's documented contract proves the request
was rejected BEFORE execution (never inferred globally). Per venue:
- **Nobitex** — HTTP 429 AND the HTTP-200 business envelope both carry the documented
  `{"status":"failed","code":"TooManyRequests","backOff":N}` shape; `backOff` is SECONDS
  (iranArb-verified unit), capped at 15m; `status:"failed"` is Nobitex's documented
  not-performed contract, so a parsed TooManyRequests envelope IS a definite pre-execution
  rejection (a bare 429 with no envelope is NOT). An HTTP-200 throttle body is never
  mis-classified `CatBadRequest` and never treated as a successful mutation.
- **Bitpin** — 429 with `Retry-After` header (integer seconds) or the DRF body
  `"available in N seconds"` regex (iranArb-proven parser, default 30s at the auth layer);
  order-path 429s carry the wait but are NOT definite (no verified contract).
- **Wallex** — 429 by status (+ standard headers when present); HTTP-200 `success:false`
  bodies use ONLY the conservative phrase fallback; never definite.
- **Public adapters (Binance/Ramzinex/Tabdeal/Exir)** — 429 → `CatRateLimit` wrapping
  `ErrRateLimited` + `Retry-After`/`X-RateLimit-Remaining:0`+`Reset` header parsing.
Generic text matching is a tightly-scoped fallback (`LooksLikeRateLimitMessage`) that can
never fire on harmless words like "limit" (limit order / price limit).

**Per-exchange reactive cooldown (PR20 correction).** Any normalized rate-limit signal
PARKS the affected exchange only: the executor's claim loop skips a parked exchange
entirely (nothing is claimed → order/cancel/status/balance all deferred before any network
call), and a request claimed just before the park is Released back to QUEUED unsent.
Deadlines are absolute and EXTEND-ONLY (a longer later wait extends; a shorter one never
shortens — iranArb-proven), prefer the venue-provided duration, fall back to the
exchange's configured `retry_backoff_ms` then a 60s default, and are bounded (15m default
cap). Mutex-guarded for in-process concurrency (single-instance design), poll-driven (no
busy loop), and observable via safe logs: exchange, reason (category/code only), source,
cooldown-until — never credentials, tokens, signatures, or response bodies.

**A SUCCESSFUL response can still carry throttle information (PR20 correction).** Two cases
that must never be confused:
- **The operation succeeded, but the quota is now exhausted** (HTTP 200 + a real success body +
  `X-RateLimit-Remaining: 0` / `Retry-After`). The completed operation stays successful and its
  result is preserved — a filled or accepted order is NEVER downgraded to a failure or an
  ambiguity because the budget ran out. Only FUTURE requests pause: a shared transport observer
  reports the signal to the executor's `RateLimitSink`, which parks the exchange exactly like
  any other cooldown (and persists it). The observer is read-only with respect to the response
  and lives at the transport because these headers are HTTP-standard rather than venue-specific
  — one implementation covers every adapter and operation and cannot be forgotten by a future
  adapter. `DefiniteRejection` is meaningless here: nothing was rejected.
- **HTTP 200 whose BODY says the venue did not perform the operation** (Nobitex's
  `status:"failed"` + `TooManyRequests` + `backOff`). Not a success: classified per venue and
  operation — a documented not-performed contract permits a re-queue after the cooldown, and
  anything else is ambiguous → read-only probe. Never a blind retry.

The sink is a standalone object (`executor.NewRateLimitSink`), not a method on the Executor:
rule #1's reflection guard requires the Executor's ONLY exported method to be `Run`, so no
exported surface can ever send an order. A sink can only park an exchange.

**Rate limits never cause blind mutation retries (PR20 correction).** For read-only
requests a throttle simply reschedules (and the park defers everything else). For
PlaceOrder/CancelOrder: `definite_rejection=true` (venue-proven, pre-execution — e.g.
Nobitex's envelope) re-queues the SAME persisted request for after the cooldown
(`RequeueProvenUnexecuted`: RETRY_SCHEDULED, bounded by max_retries, dead-letters
conservatively when exhausted) — the only sanctioned mutating retry, and it is not blind;
ANY other rate-limit-looking response — crucially including HTTP 200 with a throttle body
— is an AMBIGUOUS outcome: never marked successful, never retried, persisted and resolved
through the standard read-only recovery probes with the symbol lock HELD.

**Exchange rate config is WIRED (PR20 correction #7).** The previously-dead
`exchange_configs.rate_limit_per_sec` and `retry_backoff_ms` fields are now consumed by
the order-executor via the live configstore cache (`Config.ExchangeTuningFor`):
`rate_limit_per_sec` drives a PROACTIVE per-exchange minimum-interval pacer (prevents
exceeding a known budget); `retry_backoff_ms` is that exchange's REACTIVE fallback cooldown
when a throttled venue provides no wait. Proactive pacing and reactive cooldown are
deliberately separate mechanisms; a valid, longer server-provided backoff always takes
precedence over the fallback.

**Startup order is a safety property (PR20 correction).** Everything a real send depends on is
loaded and validated **synchronously, before the first claim**, and any failure aborts instead
of degrading to defaults:

    resolve exchange ids → StartupLoad (exchange tuning: load + validate)
      → load durable cooldowns → start the cooldown persister → only THEN claim/send
      → (periodic config refresh starts afterwards, and may fail safely)

Loading tuning in the background would let the first requests run with **uninitialized zeros**
(`rate_limit_per_sec = 0` → no pacing at all; `retry_backoff_ms = 0` → the wrong reactive
fallback), and loading cooldowns late would let a restart send to a still-throttled venue. In
`live` mode `validateTuning` additionally requires an `exchange_configs` row for every wired
exchange — a missing row would silently mean "no pacing", which is not a safe default when real
money is at stake — and rejects negative values. A failure at any of these steps returns from
`Run`, so the binary exits and **no real mutation happens with unloaded config**. The periodic
refresh is the only asynchronous part, and a failed reload keeps the last good snapshot.

**Two guards around pacing: early pre-filter, final authoritative (PR20 correction).** The
initial guard can go STALE while a request waits in the pacer (kill switch, live session,
preflight/ack, exchange/market enable flags, credential, order/cycle state can all change). So
a live PLACE/CANCEL is gated TWICE. An EARLY guard runs before pacing — a cheap pre-filter so a
locally-invalid request never consumes a pacing slot; it audits denials but writes NO allow
audit. The FINAL guard runs after pacing, immediately before `MarkInFlight`: it re-checks every
time-sensitive condition against the state AT SEND TIME, writes the durable allow-audit (the one
committed before the send), and proves the persisted sent-id. Any DB error, missing row, changed
state, or disabled setting at the final guard denies with the no-stranding disposition (buy →
FAILED + lock released; sell → NEEDS_RECONCILE + lock held; cancel → DEAD + NEEDS_RECONCILE +
lock held). Every terminal decision is audited exactly once (early guard audits its denials; the
final guard audits the allow, or a deny that only appeared at send time).

**One authoritative disposition, derived from the ORDER, never an unrelated cycle (PR20
correction).** Every terminal disposition of a mutating request that must not / did not execute
— buy denial, sell denial, cancel denial, malformed/pre-handler failure, and retry exhaustion —
goes through ONE function, `orders.DisposeDeniedMutation`. It reads the ORDER `FOR UPDATE`,
**derives the authoritative cycle from `order.cycle_id`** (never the queue request's claimed
`cycle_id`), and reads that cycle `FOR UPDATE`. It then proves the full
request↔order↔cycle↔exchange relationship: the request's claimed `cycle_id`/`exchange_id` must
equal the order's real ones. It releases the symbol lock ONLY for an ENTRY BUY whose zero
exposure is proven (`order.state=QUEUED`, `filled_quantity=0`, `exchange_order_id IS NULL`, and
the cycle still pre-send: `NEW`/`SIGNAL_DETECTED`/`BUY_REQUEST_QUEUED`) AND whose relationship is
consistent — then request/order/cycle FAILED, **lock RELEASED**, all on the ACTUAL cycle.
Anything else — a sell/cancel (inventory / possibly-open order), an entry buy whose exposure
cannot be disproven (a concurrent recovery advanced it to `SUBMITTED`/`ACKED`/
`PARTIALLY_FILLED`, an exchange id appeared, a fill landed), OR an inconsistent relationship
(the claimed `cycle_id` points at a DIFFERENT cycle B) — holds the lock and marks the ACTUAL
order+cycle `NEEDS_RECONCILE` (request `DEAD`). An inconsistent request can therefore never
fail, reconcile, or unlock an unrelated cycle B; only the order's real cycle A is touched.
`OnBuyDenied` is a thin `KindEntryBuy` façade over this.

**Nothing synchronous runs between the final guard and MarkInFlight (PR20 correction).** The
final guard is the LAST thing before `MarkInFlight`. The PR24 first-order checklist — which
does synchronous DB work — was previously recorded inside the gate, between the final guard and
`MarkInFlight`; state could change during it, making the "final" allow stale. It is now recorded
AFTER the exchange operation (in `recordFirstOrderChecklist`, on the success path), off the
critical window. Ordering: `pacing → final guard → MarkInFlight → sendContext → exchange call`.

**Pre-network failures are DEFINITELY-NOT-SENT, never ambiguous (PR20 correction).** Before any
HTTP request leaves the process the adapter (and the executor) do local work — load/decrypt
credentials, acquire/refresh an auth token (Bitpin's `authenticate`/`refresh_token`), validate
the symbol, build the payload/HTTP request. A failure there means the mutation DEFINITELY did not
reach the venue, so it must NOT become an ambiguous outcome (no read-only recovery probe, no
NEEDS_RECONCILE-from-ambiguity). The private adapters mark these with `execution.ErrNotSent`
(`NotSent` for transient failures like a credential-DB blip or an auth-endpoint 429/timeout,
`NotSentPermanent` for deterministic ones like an invalid symbol/request), making ZERO order-
endpoint calls. **Bitpin's token path**: any failure of `bearerToken` (a fresh auth 429, a token
timeout/network error, an invalid token response) is wrapped `ErrNotSent` and the order endpoint
is never called; an auth 429 preserves its rate-limit inside the wrapper so the executor still
arms Bitpin's cooldown. Immediately before the client call the executor also checks
`sendCtx.Err()`: an already-cancelled/expired send context (e.g. shutdown) is treated the same.
The executor classifies `execution.IsNotSent(err)` BEFORE any ambiguous handling. Disposition:
- a PERMANENT not-sent is terminal via `DisposeDeniedMutation` (buy → exposure-proof clean fail;
  sell/cancel → NEEDS_RECONCILE, lock held);
- a TEMPORARY not-sent re-queues via the sanctioned atomic path with the RIGHT backoff (below);
- **pre-handler failures** (a transient `order.role` read error in `dispatchPlace`, a malformed
  `CANCEL_ORDER` payload) never fail only the queue row: a transient one re-queues
  (`RequeueClaimedUnsent`, since the request is still CLAIMED), a permanent/malformed one goes
  through `DisposeDeniedMutation` (order+cycle NEEDS_RECONCILE, lock held) — never stranding the
  cycle.
The recovery DB work detaches to a fresh bounded context when the parent is already cancelled,
so a shutdown-cancelled send still records its definitely-unsent recovery rather than stranding
`IN_FLIGHT`.

**Temporary-not-sent backoff is distinct from a venue cooldown (PR20 correction).** The retry
deadline is chosen by cause, NOT uniformly from the cooldown: a VENUE RATE LIMIT (a proven
pre-execution rejection, or a Bitpin auth 429) arms the cooldown and retries at the
`cooldown_until` deadline; an ordinary TEMPORARY local not-sent (e.g. a credential-DB blip, whose
cooldown is 0) uses a bounded EXPONENTIAL backoff with jitter (base = the exchange's
`retry_backoff_ms`, else 1s; doubled per prior attempt; capped) — so it never burns all
`max_retries` in a second. A PERMANENT local failure is not retried at all. `last_error` and the
logs always preserve the REAL failure reason — a credential failure is never labelled
"rate-limited".

**Mutating requests require both `cycle_id` and `order_id` (PR20 correction).** A PLACE/CANCEL
without an order can never be safely dispatched or recovered — it would strand its cycle. So
`Queue.Enqueue`/`EnqueueScheduled` REJECT a mutating request missing either id
(`ErrMalformedMutation`), and `Queue.Claim` refuses to return a mutating row whose `order_id` or
`cycle_id` is NULL (defending against historical/manually-written rows that predate the check).
A malformed (or inconsistent) row that already exists is finalized by `sweepMalformedMutations`,
whose ownership and execution mode are derived from the AUTHORITATIVE persisted order — NEVER the
untrusted queue metadata (round 9 #1/#3). It selects non-terminal mutating rows that are malformed
(NULL `order_id` or `cycle_id`) OR inconsistent (a valid order whose claimed `cycle_id`/`exchange_id`
does not match the order's), and for each:
- **valid `order_id`** → `orders.DisposeDeniedMutation` derives the cycle FROM THE ORDER (a NULL or
  mismatched claimed cycle is treated as "no claim — derive it"): request DEAD, the ACTUAL order +
  cycle → NEEDS_RECONCILE, lock HELD, any unrelated cycle/lock untouched. **Scoped to the ORDER's
  cycle mode** (so a live NULL-`cycle_id` row is finalized by the LIVE executor even though the queue
  row names no cycle);
- **no order, valid `cycle_id`** → `orders.DisposeMalformedMutation` (request DEAD, that cycle →
  NEEDS_RECONCILE, lock HELD). Scoped to the CLAIMED cycle's mode;
- **neither trustworthy** → request DEAD only; NO cycle/lock is touched.

Only the executor whose mode matches the authoritative cycle finalizes a row, so a live and a
dry-run executor can never mutate each other's cycles or locks. An `off` executor finalizes nothing.
The mode filter is applied INSIDE the SQL, BEFORE the `LIMIT`, with deterministic `ORDER BY er.id`
(round 10 #1): a Go-side skip after `LIMIT n` would let n rows of the OTHER mode fill the window on
every sweep and permanently starve this executor's own rows (50 dry-run rows ahead of one live row
would hide the live row forever). A row with no trustworthy ownership is mode-independent (only the
request is finalized). The DEAD transition uses `MarkDeadMalformed` (`DenialParams.BroadTerminal`)
so it works even on a still-QUEUED row. One finalizer per row (`FOR UPDATE SKIP LOCKED`).

**The mutation network boundary: `PreparePlace/PrepareCancel` → order pacing → final guard →
`MarkInFlight` → `PreparedMutation.Send` (PR20 correction, round 8).** ALL THREE private adapters —
**Nobitex, Wallex and Bitpin** — implement `exchanges.MutationPreparer`. Every piece of
definitely-pre-network work — credential load/decrypt, symbol validation/normalization, payload
construction, and the FINAL `http.Request` construction — happens in `PreparePlace`/`PrepareCancel`
BEFORE the executor commits `MarkInFlight`. `MarkInFlight` is then committed and immediately
followed by `PreparedMutation.Send`, which does the smallest possible send-boundary work: it binds
the send context to the already-built request (`req.WithContext`) and performs the single order/
cancel `http.Client.Do`. Nothing fallible — no credential/token/symbol/payload/request
construction — happens after `MarkInFlight` (`internal/exchanges/prepared.go` `doPreparedRequest`,
plus each adapter's `bitpinPrepared`/`nobitexPreparedPlace`/`wallexPreparedPlace` etc.). So a crash
DURING preparation (e.g. a Bitpin token refresh, or a Nobitex/Wallex in-memory credential read)
leaves the row CLAIMED (swept back to QUEUED, never sent) — not a false "maybe sent"; a preparation
failure is definitely-not-sent while still CLAIMED (the CLAIMED-guarded requeue / disposition).

**Pacing counts actual network calls, not preparation-method invocations (round 8 #3).** The
executor no longer paces before `PreparePlace`/`PrepareCancel`. Instead it passes a pacing hook to
the adapter (`WithNetworkPacer` in the prepare context); the adapter reserves a per-exchange slot
ONLY when it is about to perform an authentication/refresh HTTP call. The order/cancel send is
paced separately by the executor, immediately before `MarkInFlight`. Consequently:
Nobitex/Wallex (in-memory credential, no auth endpoint) and Bitpin reusing a still-fresh cached
token make ZERO preparation network calls and consume exactly ONE slot (the order send); Bitpin
that must authenticate or refresh makes one auth call plus one order call and consumes exactly TWO
slots. Pacing reservations equal actual HTTP calls.

**Bitpin must not send an order after auth is rate-limited, and the retry waits the REAL venue
deadline (PR20 correction, round 6 + round 9 #4).** `bearerToken` is strict for mutations: an active
auth throttle window, a fresh auth 429, or an auth 200 carrying `X-RateLimit-Remaining: 0` all fail
preparation as a definitely-not-sent RATE LIMIT — the order endpoint is never called in the same
invocation, even when a cached token exists. Crucially, `bitpinAuthRateLimited` now populates
`RateLimitInfo.RetryAfter` with the ACTUAL deadline for every case: the parsed `Retry-After`/body
duration for a fresh 429, `time.Until(authThrottledUntil)` for an already-active window, and the
`X-RateLimit-Reset` duration for an exhausted-quota 200. The executor (`notSentRetryAt` →
`armRateLimit`) therefore schedules the queue retry at the real venue deadline — NOT the short
configured `retry_backoff_ms` fallback that would fire every second and exhaust `max_retries` before
the throttle expired. And while the window is active, a cached-token mutation returns the remaining
duration WITHOUT re-hitting the auth endpoint, so a known throttle never repeatedly burns retry
attempts. (Reads keep the lenient behaviour — a cached token during a throttle window is fine for a
GET.)

**Stale candidate discovery is order-authoritative and mode-scoped (PR20 correction, round 9
#1/#2).** `recoverStaleMutating` finds stale `IN_FLIGHT` mutations by JOINing the persisted order and
its cycle, and scoping by the ORDER's cycle mode (`c.dry_run` = this executor's mode) — NEVER the
queue row's `exchange_id`/`cycle_id`. The old per-exchange loop keyed on `er.exchange_id` and an
`EXISTS(cycles WHERE id = er.cycle_id AND dry_run = ?)` filter, which HID rows whose queue metadata
was missing or wrong: a NULL claimed `cycle_id` (never matched the EXISTS), an unwired/foreign
claimed `exchange_id` (no executor iterated it), or a cross-mode claimed cycle (the wrong-mode
executor would pick it up). The order-JOIN discovery finds every such row through its real order and
routes it to exactly the executor that owns the order's cycle mode, so an unwired claimed exchange
can never strand a mutation `IN_FLIGHT` forever, and a dry-run executor can never discover (or touch)
a live order's mutation. NULL-`order_id` rows have no order to JOIN and are handled by
`sweepMalformedMutations` instead. An `off` executor discovers nothing.

The discovery query also RETAINS the authoritative ownership on each candidate (round 10 #2):
`o.cycle_id`, `o.exchange_id` and the order's cycle mode are stored in the stale candidate at
discovery time. If the later authoritative re-read (`orderRecoveryInfo`) fails persistently until
the recovery window expires, `finalizeStaleAuthoritative` resolves the request using ONLY those
retained values: request → DEAD, the ACTUAL order + ACTUAL cycle → NEEDS_RECONCILE, lock HELD — the
queue row's claimed `cycle_id` is NEVER used to mutate cycle/order/lock state when a persisted order
exists (it may point at an unrelated cycle B while the order belongs to cycle A; B stays untouched).
A genuinely-missing order row (ErrNoRows) reconciles the retained actual cycle (there is no order
state left to change).

**Every discovered stale mutating `IN_FLIGHT` request reaches a terminal decision, and only with
proven ownership (PR20 correction, round 8 #4).** `recoverOneStaleMutation` never returns silently
and never builds a probe from mismatched metadata. It re-loads the AUTHORITATIVE order via
`orderRecoveryInfo`, which returns the order's OWN `cycle_id` and `exchange_id`. Then:
- **no order id at all** → `finalizeStaleConservative` (request DEAD, the queue row's cycle, if any,
  → NEEDS_RECONCILE, lock HELD) — nothing is derivable;
- **order missing / temporary DB error** → ErrNoRows or past the recovery hard limit (the request's
  `inflight_at` age vs the recovery TotalTimeout, min 5m) finalizes conservatively; a genuinely
  temporary error is retried on the next sweep, bounded by that limit;
- **ownership mismatch** (the queue row's claimed cycle ≠ the order's cycle, or the queue row's
  exchange ≠ the order's exchange) **or a NULL claimed cycle_id with a valid order_id** →
  `orders.DisposeDeniedMutation` with `Kind=Unknown` (always conservative, never releases the lock):
  the cycle is DERIVED FROM THE ORDER, the request goes DEAD, the ACTUAL order + cycle go
  NEEDS_RECONCILE, the lock is HELD, and NO unrelated cycle/lock is touched. No probe is ever
  created with mixed ownership (e.g. an order from cycle A can never be probed under a claimed cycle
  B — B stays untouched);
- **ownership proven consistent** → BEFORE any probe is created, the executor verifies the ORDER's
  exchange has a USABLE read-only recovery path (round 10 #3): a wired client in this executor's
  client map AND `exchanges.enabled = 1` — because `Claim`'s SQL refuses disabled exchanges and the
  claim loop only iterates wired clients, a probe without both would sit QUEUED forever
  (unclaimable), leaving the order/cycle unresolved and the lock held indefinitely. With a usable
  client, the read-only probe is scheduled using the ORDER's authoritative cycle and exchange
  (place → look up by client-order-id; cancel → by exchange-order-id) and the request is marked
  DEAD. WITHOUT one (no client constructed at startup, no credential, or the exchange is disabled),
  NO probe is created — the request is finalized conservatively instead (`finalizeStaleAuthoritative`:
  request DEAD, ACTUAL order + cycle NEEDS_RECONCILE, lock HELD → manual reconciliation). The
  `enabled` check fails CLOSED (a DB error checking it → conservative finalization, never an
  unclaimable probe). No recovery probe can remain permanently QUEUED.

A creation-time check alone is NOT sufficient (round 11): an exchange can be disabled, lose its
credential, or simply not have its client constructed AFTER a `GET_ORDER` recovery probe was already
committed as `QUEUED` (typically across a restart). The claim loop never iterates a missing client
and `SweepStuck` only touches stale `CLAIMED`/`IN_FLIGHT` rows, so such a probe — and the
order/cycle/lock behind it — would sit stuck forever. Two mechanisms close this:
- **`sweepUnclaimableRecoveryProbes`** runs at STARTUP (before the first claim) and PERIODICALLY. It
  finds non-terminal (`QUEUED`/`RETRY_SCHEDULED`) `GET_ORDER` requests whose ORDER's exchange is not
  usable in this executor — disabled or unwired — deriving ownership and mode from the PERSISTED
  order (never the probe row's claimed cycle_id/exchange_id) and SQL-filtering by the order's cycle
  mode BEFORE `LIMIT` (so the other mode's probes cannot starve this one). Each is finalized
  atomically: probe → DEAD, ACTUAL order + cycle → NEEDS_RECONCILE, lock HELD; unrelated cycles/locks
  untouched. It re-verifies unusability inside the finalizer (a race could re-enable the exchange, in
  which case the probe is left QUEUED for the claim loop), and is idempotent (single finalizer via
  `FOR UPDATE SKIP LOCKED`).
- The ambiguous-outcome paths (`recoverAmbiguousPlace`/`recoverAmbiguousCancel`) RE-CHECK the
  exchange's usable recovery path (`usableRecoveryClientByCode`) immediately before scheduling a
  probe; if it was lost during the send, they reconcile (`deadReconcile` → ACTUAL order/cycle
  NEEDS_RECONCILE, lock HELD) instead of queueing an unclaimable probe.

Together these guarantee no `GET_ORDER` recovery request can remain permanently `QUEUED`.

Every branch runs inside `WithTx` under a `FOR UPDATE SKIP LOCKED` single-finalizer guard, so
exactly one instance converts each row and concurrent sweepers are safe. No stale mutating request
can sit `IN_FLIGHT` indefinitely, and none can apply a fill or transition to the wrong cycle.

**The definitely-unsent retry is atomic, status-guarded, and its exhaustion is
operation-specific (PR20 correction).** The sanctioned mutating retry runs in ONE transaction
that locks the row (`SELECT ... FOR UPDATE`), requires the status to be exactly the expected
pre-execution status — `IN_FLIGHT` for `RequeueProvenUnexecuted` (post-MarkInFlight),
`CLAIMED` for `RequeueClaimedUnsent` (a pre-handler failure) — enforces the retry limit, and
transitions with a status-conditioned `UPDATE ... WHERE id=? AND status=?` that must affect
exactly one row. A request another actor already moved to DEAD/FAILED/SUCCEEDED can never be
resurrected; two concurrent callers can never both increment the retry count. At the retry
limit the queue runs an executor-supplied `onExhaust` callback INSIDE the same transaction,
which applies `DisposeDeniedMutation` for the operation: an **entry buy** with proven zero
exposure → request DEAD, order+cycle FAILED, **lock RELEASED**; an **exit sell** or **cancel**
→ request DEAD, order+cycle NEEDS_RECONCILE, **lock HELD** (inventory / possibly-open order).
So exhaustion resolves request + order + cycle + lock atomically — never leaving the cycle
inconsistent.

**The exchange-call timeout starts at the network boundary (PR20 correction).** The per-request
timeout context (`sendContext`) is created AFTER pacing, the final guard, and `MarkInFlight` —
never before local processing. Creating it earlier would let the pacing wait consume the network
timeout: a request delayed only inside the process could reach `PlaceOrder` with an
already-expired context and be mis-classified as an ambiguous exchange mutation. Now the timeout
measures the real venue call, and a request that never left the process is never made ambiguous
by internal delay.

**Where pacing happens (PR20 correction).** The pacer waits at the real network boundary
(`paceSend`), i.e. AFTER decode → validation → DB loads → live guard → pre-send persistence →
audit, so a request rejected locally never consumes a slot of a scarce per-exchange budget. It
sits immediately BEFORE `MarkInFlight` rather than after it, deliberately: a crash or
cancellation while pacing then leaves the row `CLAIMED` (swept back to `QUEUED`, nothing sent)
instead of `IN_FLIGHT`, which would be treated as "maybe sent" and pushed to NEEDS_RECONCILE for
a request that never left the process. Each request consumes **exactly one** pacing slot
(a duplicate reservation silently halves the exchange's rate budget and adds a full interval of
latency; the executor's tests assert the slot count exactly). `paceSend` **returns an error**
when the wait is cut short by shutdown: the caller then does not `MarkInFlight` and does not
call the client at all — a request that never left the process is DEFINITELY unsent, and
recording it as ambiguous would be a false ambiguity.

**First live phase is tiny.** The intended first rollout is one exchange, one symbol,
very small `max_order_notional`/`max_base_qty`, `max_open_cycles=1` — enforced purely by
the configured caps + the single `live_enabled` exchange/symbol, and surfaced by the
order-executor's startup safety summary (the dashboard has no live view in this lineage —
§18). Broad multi-exchange live is **not** enabled here.

**Audit (`live_audit`; migration 020).** Every live mutating decision (allow or deny) is
persisted: exchange, symbol/market, cycle, order, request id, action/side, notional,
decision, reason, execution mode, config version, timestamp. No secrets.

**Live visibility — DB + logs, not a dashboard endpoint (corrected).** The dashboard in
the current lineage (§14/§14a) exposes **no** `/api/live*` route: live state is observed
through `live_audit` (every allow/deny), `app_logs`, and the startup safety summary
(`live.BuildSafetySummary`, logged by the order-executor: mode, kill switch, caps,
live-enabled scope, credential status — never key material). Earlier drafts of this
section described a `GET /api/live` read-only view; it is **not implemented here**, and
this paragraph is corrected rather than describing it as delivered. See §18.

**Files (PR20 correction).** `internal/live/{live.go,session.go}` (guard, exit proof,
durable audit), `internal/exchanges/ratelimit.go` (+`ratelimit_test.go`) — the normalized
rate-limit model + shared parsers, `internal/executor/cooldown.go`
(+`cooldown_test.go`) — the per-exchange cooldown registry, the proactive pacer,
`noteRateLimit`, and `requeueProvenRejected`; per-adapter detection lives in each
`internal/exchanges/<venue>.go`; `internal/queue/queue.go` gains `Release` +
`RequeueProvenUnexecuted`; `cmd/order-executor/main.go` wires `ExchangeTuningFor` from
the configstore cache.

**Cooldowns are DURABLE (migration 033 `exchange_cooldowns`).** A park deadline must outlive
the process — otherwise a restart resumes sending to a venue that is still throttled. The row
is `(exchange_id PK, cooldown_until, reason, source, updated_at)`. EXTEND-ONLY is enforced in
SQL (`cooldown_until = GREATEST(cooldown_until, VALUES(cooldown_until))`, with reason/source
assigned FIRST so they only change when the deadline actually extends), so even a re-ordered or
concurrent write can never shorten an active longer cooldown. The row holds NO secrets:
`reason` is a category/code (`rate_limit:TooManyRequests`), `source` is which signal identified
it. The pacer stays in-memory by design (a rate budget, not a safety deadline). Both are
per-process — the single-instance decision above is the precondition.

**Graceful shutdown flushes pending cooldowns (PR20 correction).** Because persistence is
asynchronous, an armed-but-unwritten park would be lost if the process exited between the arm
and the worker's write. On graceful shutdown `Run` stops claiming, waits for the persistence
worker to stop (no new persistence events), then runs a FINAL flush of all pending parks with a
SEPARATE bounded context (`ShutdownFlushTimeout`, default 5 s) — so a restart restores them. The
flush cannot hang: if the DB is unavailable it logs the unflushed count and returns within the
timeout. **A HARD crash (kill -9, power loss) cannot be made perfectly durable with asynchronous
persistence** — a park armed microseconds before the crash may be lost; on the next throttle it
is simply re-detected and re-parked (safe, not silent). Making the arm synchronous is not an
acceptable fix (it would risk turning a confirmed mutation into an ambiguous one — see below).

**Persistence is ASYNCHRONOUS and never sits in an exchange response path (PR20 correction).**
Arming a cooldown is pure memory: `armRateLimit` takes a mutex, updates the deadline, marks the
entry pending, and returns. It does **no I/O**. This is a safety property, not an optimisation:
the sink is called from the adapter's HTTP transport, so a slow cooldown write would delay a
**successful** PlaceOrder response back to the caller, and a caller-side timeout would turn a
mutation the venue ALREADY PERFORMED into an ambiguous one — the exact outcome the rest of this
system spends its complexity avoiding. Durability is owned by a separate worker
(`runCooldownPersister`, started by `Run`), which writes pending rows with bounded backoff
(250 ms → 10 s) and is nudged non-blockingly on each arm.

**Persistence retry is tracked independently of deadline extension (PR20 correction).** The
pending set is derived from the invariant `persistedUntil == until`, never from "this call
extended the deadline". Deriving it from extension loses writes: if the first write fails and
the next signal carries an equal/shorter deadline (`extended == false`), the entry would never
be retried and a restart would silently lose the park. A failed write therefore stays pending
**forever** — through any number of non-extending signals — until it succeeds or the failure
policy fires. Entries whose deadline has already expired are dropped from the pending set
(durability for a park that no longer parks anything is pointless).

**Cooldown persistence failure policy — per exchange, entry-buys only (PR20 correction).**
While a park is un-persisted the in-process park still holds, so nothing is sent to that venue
*now*; what is lost is only the guarantee that a restart would still honour it. If that state
lasts beyond `Config.CooldownPersistGrace` (default 30 s) for an exchange, that EXCHANGE's live
ENTRY BUYS are disabled — `gatePlace` denies a buy on `cooldownDurable(code)==false`, routed
through the no-stranding disposition — with an ERROR log naming the outage. Durability health is
tracked in a PER-EXCHANGE map (`durabilityFailed[code]`) maintained by the persister, so an
outage on exchange A never disables exchange B; A re-enables automatically once its writes catch
up (or its park expires). Two paths stay available even for the affected exchange: **proven exit
sells** (risk-reducing — an outage is never a reason to strand inventory) and **CANCEL** (same
asymmetry as the audit-outage policy). A genuinely global DB outage is handled independently by
the fail-closed DB guards; it is not modelled as one exchange's persistence failure.

**Current limitations (not future work — the state of the code).**
- **No real venue order has executed yet.** The wiring exists and the guard is enforced;
  what remains is operational: a real master key in the bootstrap config, a provisioned +
  validated credential, `live_enabled` on the exchange + symbol, configured caps, a
  deliberate kill-switch disengage, a passing preflight + active acknowledgement, and a
  started canary session. Rule #3 keeps every test venue-free, so no automated test proves
  an end-to-end real order.
- **Live mode fails closed on missing prerequisites rather than degrading**: an absent or
  invalid master key disables credential loading (no clients, `AllowLiveExecution` stays
  false, nothing is sent, warning logged); a startup tuning/cooldown load failure aborts
  the binary (see "Startup order"); a cooldown durability outage beyond the grace period
  disables live sends (see "Cooldown persistence failure policy").
- Cooldown/pacing state is per-process (single-instance assumption); the dashboard exposes
  no live surfaces in this lineage (§18).

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

**Credential validation — two paths, both read-only (`BalanceReader`, never place/cancel).**
- `Provider.Validate` is the STRICT, one-shot **operator-initiated** check: an operator
  explicitly asked "is this credential good now?", so success stamps `status='active'` +
  `last_checked_at` and **any** error stamps `status='invalid'`.
- `Provider.ProbePrivateHealth` is the LENIENT, **continuous health-monitor** path. It is
  the one the `health-monitor` private probe uses. It marks `status='invalid'` **only on a
  definite auth error** (`isDefiniteAuthError` = the `execution.ErrAuthFailed` sentinel or a
  `NormalizedAPIError` with category `auth`). A temporary error (timeout / deadline /
  network / rate-limit / HTTP 429 / exchange 5xx / unknown) leaves the credential row
  **untouched and `active`**, surfacing only as private **health** status
  (UNAVAILABLE/DEGRADED/RATE_LIMITED). This prevents a brief exchange incident from
  permanently invalidating a valid credential and silently disabling live trading (a later
  `BuildPrivate` still finds an `active` row). Tested by
  `TestProbePrivateHealthCredentialPolicy` (auth→invalid; timeout/network/rate/5xx→active).

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
whitelist (cycle: BUY_FILLED / BUY_PARTIALLY_FILLED / SELL_REPRICE_PENDING / SELL_PARTIALLY_FILLED /
SELL_FILLED / CANCELLED / FAILED / CLOSED; order: FILLED / PARTIALLY_FILLED / CANCELLED / FAILED), with
the same version-guarded CAS + event-row write as any transition. An illegal target is
rejected (`ErrNotReconcileResolution`). PR21 adds `state.CorrectTerminalOrder` (terminal →
PARTIALLY_FILLED/FILLED) for discovered-fill correction (below).

**Auth + roles (PR21 correction — current session model, orthogonal capability).** The old
bearer-token/`dashboard_tokens` model is gone; reconciliation uses the current
`dashboard_users` + `dashboard_sessions` HttpOnly-cookie model. EVERY reconcile endpoint —
including the read-only list/detail/audit — is gated by `requireReconcileCapable`, an
**exact-set** capability check (`reconcile_operator` **or** `admin`): 401 without a session,
403 for any other role. `reconcile_operator` is deliberately NOT on the config `roleRank`
ladder (`roleAtLeast(reconcile_operator, config_operator)` is false), so it can resolve cases
but **cannot edit configuration** — reconciliation is an orthogonal capability, not a rung
above config. Migration `034` widens the `dashboard_users.role` CHECK to add the value; the
authenticated username is recorded in the audit (never from the body). Cycles carry their
`dry_run` flag through list/detail so live and dry-run cases are visible and labeled.

**Exposure classification — a recorded zero is NOT proof of zero (PR21 correction #1).** The
resolver classifies the cycle's base-asset exposure as **PROVEN_ZERO / OPEN / UNKNOWN /
INCONSISTENT**, computed from the FOR UPDATE-locked orders:
- **OPEN** — recorded net (Σ buy filled − Σ sell filled) > 0.
- **INCONSISTENT** — recorded net < 0 (sells exceed buys); every resolution is refused until
  the data is corrected.
- **PROVEN_ZERO** — net = 0 AND no order carries unconfirmed venue risk (no `exchange_order_id`
  and not in a may-have-executed state: SUBMITTED / ACKED / PARTIALLY_FILLED / CANCEL_PENDING /
  NEEDS_RECONCILE).
- **UNKNOWN** — net = 0 but at least one order MIGHT hold unrecorded inventory (it reached, or
  may have reached, the venue with an unconfirmed outcome — a timed-out place, a cancel that
  may have raced a fill). A recorded `filled_quantity = 0` on such an order is not proof.

A lock is released ONLY for **PROVEN_ZERO** (or a proven full-exit sell). `cancel_zero_exposure`
and `mark_buy_zero_filled` REFUSE OPEN outright and REFUSE UNKNOWN unless the operator supplies
`external_resolution_confirmed=true` + a non-empty `external_resolution_reason` (they checked the
venue). The detail view surfaces the classification.

**Active exchange requests are detected, locked, and block a close (PR21 correction #2/#9).**
Before resolving a cycle out of NEEDS_RECONCILE, the resolver loads and FOR UPDATE-locks EVERY
active (QUEUED / RETRY_SCHEDULED / CLAIMED / IN_FLIGHT) exchange request related to the cycle —
mutating AND read-only `GET_ORDER` recovery — matched by the cycle_id OR by any of the cycle's
order ids (ownership derived from the persisted order, never the request's claimed cycle_id).
If any can still execute, the out-of-reconcile resolution is REFUSED (surfaced in the preview's
`request_changes`) so the operator lets recovery finish first. The cycle, all its orders, all
those requests, and the active symbol lock are locked in the SAME transaction; cumulative fills
and exposure are computed only from those locked rows.

**No blind resolution — mandatory preview then apply, fingerprint-bound (PR21 correction
#6/#7).** `POST …/preview` returns the **exact** proposed effect and writes nothing; it also
returns a short-lived, single-use, operator-bound **preview token** carrying a `state_hash`
fingerprint of the exact cycle/order/request/lock state + operation payload. `POST …/apply`
REQUIRES that token (missing → 400) and, inside the locked apply transaction, re-derives the
fingerprint and **409s on any drift** (a changed cycle/order/request/lock/payload, an expired or
already-used token, or a token from another operator). The preview shows EVERY mutation:
`order_changes[]`, `request_changes[]`, `fill_to_insert`, `accounting_changes`, `lock_change`,
`exposure_before/after`, `exposure_classification`, and `execution_mode` — not just one order.
A `reason` is mandatory. `GET /api/reconcile/{id}` shows the full read context (cycle, orders,
fills, active requests, locks, events, prior resolutions, exposure classification, action
catalog); every sub-query error is a 500 — never a partial 200.

**Resolution actions.** `cancel_zero_exposure` (PROVEN_ZERO → cancel cycle+orders, release
lock), `attach_exchange_order_id` (idempotent/conflict/uniqueness-checked with a version CAS —
see below), `mark_buy_partially_filled` (0 < cumulative buy < order qty → order PARTIALLY_FILLED
/ cycle BUY_PARTIALLY_FILLED, lock held), `mark_buy_filled` (cumulative buy ≥ order qty → order
FILLED / cycle BUY_FILLED, lock held), `mark_buy_zero_filled` (proven-zero buy → CANCELLED,
release lock), `mark_sell_partially_filled` (exposure REMAINS → SELL_PARTIALLY_FILLED, lock
held; refused if it would close the whole exposure), `mark_sell_filled` (closes exposure → CLOSED
+ PnL accounting, release lock), `mark_order_cancelled_zero_fill` (cancel one zero-fill order,
cycle stays NEEDS_RECONCILE), `correct_terminal_order_fill` (record a fill discovered on a
TERMINAL order — see below), `keep_needs_reconcile` (audit only), `mark_failed` (see below).

**Order role/side is validated from the persisted order (PR21 correction #3).** A `mark_buy_*`
action only applies to an `entry_buy`/side=buy order and `mark_sell_*` only to an
`exit_sell`/side=sell order; the request payload's fill side cannot override the persisted role.
An `order_id` that belongs to another cycle is rejected (the resolver only knows this cycle's
loaded orders).

**Order state and cycle state are computed separately (PR21 correction #4).** The order's new
state comes from its cumulative fill vs its own quantity; the cycle's new state comes from the total
exposure and the action. `mark_buy_filled` refuses a fill that does not complete the order (→
`mark_buy_partially_filled`); `mark_sell_partially_filled` refuses a fill that closes the whole
exposure (→ `mark_sell_filled`), so a zero-exposure cycle is never left in SELL_PARTIALLY_FILLED
with the lock held.

**A cycle cannot close while ANY sell order may still execute (PR21 round-2 + round-3 corrections).**
`mark_sell_filled` (and a terminal-fill correction that would close the cycle) close + release the
lock only when NEITHER the selected sell NOR ANY OTHER sell order in the cycle can still execute —
zero TOTAL exposure is not enough. The SELECTED sell must be fully filled, OR the operator confirms
its remainder is cancelled (`external_resolution_confirmed` + reason → the order is recorded terminal
CANCELLED with its partial fill preserved, never an active PARTIALLY_FILLED). Additionally, EVERY
OTHER exit_sell order must be terminal: another sell that is active (SUBMITTED/ACKED/PARTIALLY_FILLED)
**or in NEEDS_RECONCILE** may still fill on the venue and cause oversell/negative exposure, so it
blocks the close even when it has no active queue request at that moment (round-3 blocker 1 —
`otherSells` treats a NEEDS_RECONCILE sell as dangerous, not harmless). Otherwise the cycle stays
NEEDS_RECONCILE with the lock held.

**Remaining exposure always has a deterministic next-sell path (PR21 round-2 + round-3 corrections).**
When a `mark_sell_partially_filled` (or a terminal-fill correction) COMPLETES the selected sell order
while exposure remains and NO OTHER active OR unresolved sell exists, the cycle is moved to
**SELL_REPRICE_PENDING** (a whitelisted resolution target), from which the sell Manager
deterministically creates the next exit sell for the remaining inventory — the remainder is never
stranded. If another live sell is still being managed, the cycle stays SELL_PARTIALLY_FILLED. If
another sell is in **NEEDS_RECONCILE**, the operation is REFUSED (that sell may still execute, so
queuing another exit would risk oversell) — the cycle stays NEEDS_RECONCILE with the lock held until
the operator resolves it.

**Terminal-order fill correction advances the cycle without a dead end (PR21 correction #5 +
round-2).** Recording a fill against a TERMINAL order (CANCELLED/FAILED/REJECTED/EXPIRED) via a
normal fill action is REFUSED. `correct_terminal_order_fill` records a discovered fill once (never
twice) and — because the order is already terminal, so its remainder is non-executable — a FULL
discovered fill re-opens the order to FILLED via `state.CorrectTerminalOrder` (a narrow,
version-guarded, audited terminal→FILLED transition), while a PARTIAL discovered fill LEAVES the
order in its terminal state (its cancelled remainder is preserved and never reactivated to an
apparently-active PARTIALLY_FILLED). It then advances the CYCLE so it is not stuck: a buy fill →
BUY_FILLED (full) / BUY_PARTIALLY_FILLED (partial), lock held; a sell fill → CLOSED + accounting +
lock released (exposure closed AND no other sell can execute), SELL_REPRICE_PENDING (remaining
exposure, no other active/unresolved sell), or SELL_PARTIALLY_FILLED (another live sell active). A
NEEDS_RECONCILE other sell REFUSES the correction (cycle stays NEEDS_RECONCILE, lock held). Because
the correction resolves the WHOLE cycle out of NEEDS_RECONCILE, it uses the WHOLE-CYCLE
active-request guard (round-3 blocker 2): an active request — mutating OR read-only GET_ORDER — on
ANY order in the cycle (or on the cycle directly) refuses the correction and no fill is inserted.
Preview and applied order state always agree.

**attach_exchange_order_id safety, including concurrency (PR21 correction #8 + round-2).** Empty
existing id → attach; same value → idempotent success; a different existing value → reject; an id
already attached to ANOTHER order on the same exchange → reject. The attach locks the authoritative
`exchanges` row `FOR UPDATE` and re-checks uniqueness UNDER that lock before a version-CAS update
(`WHERE id=? AND version=? AND (exchange_order_id IS NULL OR '')` + `RowsAffected`), so two
concurrent attaches of the same venue id are serialized and exactly one wins. A DB-level
**UNIQUE (exchange_id, exchange_order_id) index (migration 035)** is the hard backstop — one real
exchange order id can never bind to two internal orders even under a lost race (multiple NULLs, i.e.
unplaced orders, are permitted). Migration 035 adds the unique index before dropping the old
non-unique one (so the exchange foreign key keeps a covering index) and fails loudly on any
pre-existing duplicate rather than rewriting data.

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

**Audit is atomic with the state change (`reconcile_resolutions`; migrations 021 + 034; PR21
correction #10).** Every applied resolution writes one immutable row inside the SAME transaction
as the state change: operator, timestamp, cycle id, order id, action, old/new cycle + order
states, reason, supplied fill JSON, before/after snapshots, `lock_released`,
`external_resolution_confirmed`, and the `exposure_classification` (migration 034). The apply
re-loads the post-change state for the `after` snapshot and, if that read fails, the WHOLE
transaction rolls back — the mutation and its audit can never diverge (proven by the
`faultBeforeCommit` test hook). Preview writes nothing and is never audited.

**What remains after PR21.** A richer operator UI (this PR ships the JSON API + a static
action catalog), and optionally a read-only exchange status fetch button surfaced in the
detail view (the data path exists via PR20a read-only clients; PR21 does not wire it).

## 16f. Credential provisioning & rotation (PR22 — `internal/credentials.Provisioner`)

PR22 adds the operator path to **create → validate → activate/rotate → disable** exchange
credentials, with the safety focus on never losing exchange access, never interrupting open
trades, never activating an unvalidated credential, and never leaking a secret. Plaintext exists
only in memory; only ciphertext is ever stored; every operation is authorized + audited.

**The safe workflow.** A new credential is created **inactive and unvalidated**, is validated
against the exchange with a read-only request, and only then activated — atomically disabling the
previously active credential. The currently working credential stays active until the new one is
validated AND activated, so an operator can rotate without a window of no access.

**Create is always inactive (requirement 1).** `Provisioner.Create` ALWAYS writes `enabled=0`,
`status='unknown'`, and a **service-assigned** `key_version` (max for the exchange + 1). It does
NOT accept `enabled`/`status`/`key_version` from the caller, so a credential can never be created
already-active. Secrets are encrypted immediately (`secrets.Cipher`, AES-256-GCM,
`nonce ‖ ciphertext ‖ tag`, key = SHA-256(master key)); only ciphertext (VARBINARY) is stored,
never plaintext/logged/returned/audited.

**Validation targets the EXACT credential and is transient-tolerant (requirement 2).**
`Builder.BuildForCredential(id)` decrypts ONLY the selected row (via `Provider.CredentialsByID`) and
builds a client used solely for a read-only `GetBalances` — the auto-selected active credential is
never used, so validating credential X never accidentally validates the old one. The narrow
`BalanceReader` interface has no `PlaceOrder`/`CancelOrder`, so validation can never mutate a venue.
**Validation is fully SERIALIZED against activation/rotation (round-3 blocker 1).** The
`ValidateCredential` transaction locks the credential row and then its exchange row `FOR UPDATE` —
the SAME lock order as `ActivateOrRotate` — and holds them across the (read-only) network check, so
an activate/rotate cannot commit in the middle of a validation. This closes the remaining race
where a DEFINITE auth failure returning after a concurrent activation could mark the just-activated
credential `invalid` and leave the exchange with zero active credentials: now either validation
commits first (marking the credential and the later activation safely refuses a non-`valid`
credential) or the activation commits first (validation then runs against the new current state).
It can never end with zero usable `active` credentials. `ValidateCredential` classifies from the
locked CURRENT status: success → `status='valid'` (validated, not yet active), but if it is already
`active` it stays `active` (a successful re-validation never downgrades the live credential and
orphans the exchange); a DEFINITE auth/permission rejection → `status='invalid'`; a transient error
(timeout / network / rate limit / 5xx) → **status UNCHANGED**; a disabled credential is left as-is.
The status update + the audit row are one transaction (requirement 5). Holding a lock across the
network call is acceptable because credential validation is a rare, operator-initiated action.

**`VALID` is validated, `ACTIVE` is operational — they are not the same.** The live provider loads
ONLY `enabled=1 AND status='active'`. A `valid` credential is proven-good but NOT in use, so it is
never counted as an operational replacement — see Disable below.

**Activation/rotation is atomic and serialized (requirement 3).** `ActivateOrRotate(id)` runs one
transaction: it locks the credential row and then the **exchange row `FOR UPDATE`** (serializing
rotations per exchange), confirms the credential has been validated (`status` must be `valid`/
`active`, else it refuses), disables every other active credential (audited), and activates the
selected one (audited). A DB **active-guard** — a STORED generated column that equals `exchange_id`
exactly when `enabled=1 AND status='active'`, with a UNIQUE key — makes two active credentials per
exchange structurally impossible; the per-exchange lock plus that guard mean two concurrent
rotations can never leave two active (proven by a concurrency test). If any step fails, the previous
credential stays active (proven by a pre-commit fault test: state + audit roll back together).

**Disable protects the last OPERATIONAL credential (requirement 4).** `Disable` refuses to disable
the LAST **operational** (`enabled` + `status='active'`) credential for an exchange while that
exchange has open LIVE risk — an active live session, a non-terminal live cycle, a non-terminal/
NEEDS_RECONCILE live order, or an active live exchange request — so an operator can never remove the
ability to sell inventory, cancel an order, or recover an ambiguous result. A `valid` credential is
NOT operational and does not satisfy the "another usable credential exists" exemption (round-2
blocker 2a); since only one credential can be active per exchange, this effectively means the live
credential is disabled by **rotation** (which swaps atomically), not a bare disable. The
active-request check is **order-authoritative** (round-2 blocker 2b): when a request has an
`order_id`, its real exchange + live/dry mode come from the persisted ORDER's cycle, not the request
row's own (possibly wrong/stale/missing) `exchange_id`/`cycle_id`, so a queue row with bad metadata
cannot hide an active request whose real order is a live order on the exchange being disabled. It is
deliberately narrow: terminal history, dry-run data, and inactive locks never block. The state
change + audit are one transaction.

**Rotation is effective immediately, even for a long-lived Bitpin client (round-2 blocker 3 +
round-3 blocker 2).** Bitpin caches its JWT bound to a **non-secret fingerprint** of the credential
it was minted with (never logged). Three checks make a rotation take effect before any request uses
the old credential:
- **Before reusing a cached token** the client resolves the currently-active credential; if the
  fingerprint differs it drops the cached access/refresh token + throttle window and re-authenticates
  with the new credential.
- **After an authentication response returns** (round-3 blocker 2A) the client re-resolves the active
  credential BEFORE caching the token; if it changed while the auth was in flight, the token is NOT
  cached and a definitely-not-sent error is returned, so a stale auth response can never repopulate
  the cache after a rotation.
- **A prepared mutation carries the prep-time fingerprint** (round-3 blocker 2B): `bitpinPrepared.Send`
  re-resolves the active credential immediately BEFORE transmitting and, if it changed since
  preparation, refuses to send and returns an `execution.IsNotSent(err)==true` error — so a
  place/cancel prepared under credential A is never sent through the rotated-in credential B; the
  executor re-prepares with B (never a blind resend of a possibly-sent request). This applies to both
  `PreparePlace`→`Send` and `PrepareCancel`→`Send`.
So after `ActivateOrRotate` the running executor/reconciler/balance-sync/health-monitor never keeps
operating through the old credential's cached token or an already-prepared request.

**Master key gates writes (encryption).** From the bootstrap config file only (`[security]
master_key`) — never a runtime env var. When absent/invalid the dashboard leaves the provisioner
nil and all credential WRITE endpoints respond safe-disabled (503); live credential loading fails
closed; there is no plaintext fallback. A wrong key cannot decrypt (`ErrDecrypt`).

**Dashboard (current session model, migration 036).** Endpoints `GET /api/credentials` (metadata
only), `GET /api/credentials/audit`, `POST /api/credentials` (create), `POST
/api/credentials/{id}/validate`, `POST /api/credentials/{id}/activate-or-rotate`, `POST
/api/credentials/{id}/disable`. Every one is gated by the exact-set `requireCredentialCapable`
(`credential_operator` or `admin`) — 401 without a session, 403 for any other role;
`credential_operator` is off the config ladder. The operator is taken from the session, never the
body. No endpoint (list or audit) ever selects an `encrypted_*` blob, key material, or plaintext
(proven by test). Migration 036 adds the `valid` status, the active-guard unique key, and the
`credential_operator` role value. The `credential_audit` table (migration 022) records
exchange/credential/operator/action(`create`/`validate`/`activate`/`rotate_disable_old`/`disable`)/
old+new status/old+new key_version/reason — no secret material.

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
kill switch), so a market tick does not invalidate an ack but a config change does. The
hash is **versioned**, and the PR20 correction bumped it **`v1` → `v2`** while dropping the
two daily-cap components (they no longer exist as live inputs). Operational consequence:
**every acknowledgement recorded under `v1` no longer matches and is therefore inert** — an
operator must re-run preflight and re-acknowledge before the next live buy. That is the
intended fail-closed direction (a changed safety model invalidates prior sign-off).
`checkCaps` likewise requires only `max_open_cycles`, `max_order_notional`, `max_base_qty`,
`max_consecutive_failures`, `max_unresolved_reconcile` — a missing daily cap no longer
fails `caps_configured`. `POST
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
    master key and auth headers are never logged. This extends to rate-limit
    observability: exchange, reason (category/code), source, and cooldown-until only —
    never credentials, tokens, signatures, or response bodies.
11. **Config changes are versioned and auditable.**
12. **A rate limit never causes a blind mutating retry (PR20).** `PlaceOrder`/
    `CancelOrder` may be re-sent after a throttle ONLY when the adapter can prove from the
    venue's documented response that the request was rejected **before execution**;
    otherwise the outcome is AMBIGUOUS and follows §10d (persist, read-only probe, lock
    held, never re-sent). **HTTP 200 is not success**: a 200 carrying a throttle body is
    neither a completed mutation nor a licence to retry.
13. **Live safety evaluation fails closed (PR20).** In live mode a nil `Guard` denies and
    sends nothing; ANY database error while evaluating a live safety condition denies the
    operation (a condition that cannot be evaluated is never read as "0, therefore fine" —
    and an unresolvable market is never market id 0); and an allowed real `PlaceOrder`
    requires its audit row to be **committed before the send** — if the audit cannot be
    persisted, the order is not sent. The one deliberate asymmetry: a risk-reducing `CANCEL`
    is never blocked by an audit outage.
14. **Every terminal disposition derives the cycle from the ORDER and never strands or touches
    an unrelated cycle (PR20).** Buy/sell/cancel denial, malformed/pre-handler failure, and retry
    exhaustion all go through `orders.DisposeDeniedMutation`, which reads the order+cycle
    `FOR UPDATE`, derives the authoritative cycle from `order.cycle_id` (NEVER the queue's claimed
    `cycle_id`), and proves the request↔order↔cycle↔exchange relationship. It releases the lock
    ONLY for an entry buy with proven zero exposure AND a consistent relationship; otherwise it
    holds the lock and marks the ACTUAL order+cycle NEEDS_RECONCILE. An inconsistent request can
    never fail/reconcile/unlock an unrelated cycle. A definitely-not-sent PRE-NETWORK failure
    (bad credentials/symbol/request, a Bitpin auth/token failure, or a cancelled send context) is
    never an ambiguous outcome (no probe): a transient one re-queues via the atomic,
    status-guarded requeue (`FOR UPDATE`, requires CLAIMED or IN_FLIGHT, one increment) whose
    exhaustion applies the operation-specific disposition in the same transaction; a temporary
    LOCAL failure uses exponential backoff (not the venue cooldown), a proven rate limit uses the
    cooldown deadline, and a permanent one is not retried — logs preserve the real reason.
15. **What is about to be mutated is PROVEN from the database (PR20).** Before a real send the
    guard proves identity/ownership/state from DB rows, never from the queue payload: an entry
    buy's role/state/mode/cycle/exchange/market/symbol, an exit sell's acquired market, and a
    cancel's exact `exchange_order_id` + ownership + cancellable state. The payload's own
    values (quantity/price/type/TIF/client-id/side) must EQUAL the persisted order exactly —
    two internal values that disagree mean we do not know what we are placing.
15a. **Mutating requests require `cycle_id` AND `order_id`, and every stale mutation is finalized
    (PR20).** Enqueue rejects a mutating request missing either id; Claim refuses malformed rows;
    a malformed/stale mutating row is finalized (DEAD + cycle NEEDS_RECONCILE + lock held) rather
    than stranded or left IN_FLIGHT forever. `MarkInFlight` marks a mutation "maybe sent" only at
    the real network boundary — for Bitpin, after token acquisition — so a crash during
    preparation is definitely-not-sent, and an auth rate limit never lets the order endpoint be
    called.
16. **Startup loads before it sends, and every reload is validated (PR20).** Exchange tuning
    and durable cooldowns are loaded and validated synchronously before the first claim; any
    failure aborts startup. Every PERIODIC reload is validated too, and an invalid one keeps the
    last known-good snapshot — an invalid reload never silently disables pacing. No real mutation
    may use uninitialized tuning; no restart may resume sending to a venue whose cooldown is
    still active.
17. **The guard runs twice around pacing; the timeout starts after it (PR20).** An early guard
    pre-filters before pacing; a FINAL guard re-checks all time-sensitive conditions immediately
    before `MarkInFlight`, so state that went stale in the pacer (kill switch, session, enable
    flags, credential, order/cycle state) is caught with zero exchange calls. The exchange
    timeout is created only at the network boundary (after pacing/final guard/MarkInFlight), so
    a request delayed only by internal pacing is never made ambiguous.
18. **Nothing safety-critical blocks an exchange response (PR20).** Cooldown durability is
    written by a separate worker with bounded retries; a slow write must never delay a
    successful mutation's response and thereby make a confirmed outcome ambiguous. Durability
    health is per exchange: a persistence outage on one exchange disables only THAT exchange's
    entry buys (proven exits and cancels stay available), never another exchange. Graceful
    shutdown flushes pending parks within a bounded timeout; a hard crash cannot be made
    perfectly durable and the loss is re-detected on the next throttle.
19. **Exits are proven, not assumed (PR20).** A sell is treated as risk-reducing only when
    DB state proves it closes existing exposure (ownership, legal state, filled inventory,
    no oversell/duplicate — counting fills from CANCELLED sells, which removed inventory
    permanently). Proven exits stay available when entries are stopped.

## 18. Known limitations (current)

- **Residual hard-crash window at the send boundary (all three adapters).** As of round 8, Nobitex,
  Wallex and Bitpin all implement the two-stage `MutationPreparer`: every fallible pre-network step
  (credentials, symbol, payload, AND the final `http.Request`) runs during preparation, before
  `MarkInFlight`. After `MarkInFlight` commits, the only remaining work is binding the send context
  to the pre-built request and calling `http.Client.Do` — no fallible step in between. The residual
  is therefore reduced to a single, irreducible window: a hard process crash (SIGKILL/power loss)
  in the microseconds BETWEEN the `MarkInFlight` commit and the `http.Do` return. Because the
  request may or may not have hit the wire, that row is correctly left `IN_FLIGHT` and resolved by
  the read-only recovery probe (place → look up by client-order-id; cancel → look up by
  exchange-order-id) — never by a blind resend. This window is inherent to "persist intent, then
  send" and cannot be closed without a second network round-trip; it is the ONLY remaining
  ambiguity source and it is handled, not silently ignored.
- **Three `exchange_configs` fields are still dead config.** PR20 wired
  `rate_limit_per_sec` + `retry_backoff_ms`, but `max_concurrent_requests`,
  `request_timeout_ms`, and `max_retries` are stored and editable while nothing reads them
  (§13 table): the executor's per-exchange claim limit is hardcoded to 1 because
  `Config.LimitFor` is not wired in `cmd/order-executor`, and per-request timeout/retries
  come from `symbol_configs` at enqueue time. Nothing unsafe follows (1 is the most
  conservative limit and the symbol-level values ARE honored), but an operator editing
  those three fields today changes nothing. Wiring them was outside PR20's mandate and is
  pending work.
- **A cooldown is durable only after the persistence worker writes it (bounded hard-crash
  window).** Arming is in-memory and immediate (by design — it runs on the exchange response
  path), so there is a small window in which a park is active in-process but not yet in
  `exchange_cooldowns`. A GRACEFUL shutdown flushes pending parks (bounded by
  `ShutdownFlushTimeout`), so normal restarts lose nothing. A HARD crash (kill -9, power loss)
  inside the window can still lose that one park; it is re-detected and re-parked on the next
  throttle (safe, not silent). The window is otherwise bounded by the worker's retry cadence
  (250 ms → 10 s), and staying un-persisted past the grace period disables that exchange's entry
  buys (per exchange; proven exits and cancels continue). Making the write synchronous is NOT an
  acceptable fix (it would risk turning a confirmed mutation into an ambiguous one).
- **Proactive pacing (not the cooldown) is in-memory and per-process.** Rate-limit COOLDOWNS
  are now durable (`exchange_cooldowns`, migration 033) and reloaded before the first claim, so
  a restart keeps honoring a throttled venue. The proactive pacer's send-slot bookkeeping is
  still per-process memory — a restart resets the budget window, which can burst up to
  `rate_limit_per_sec` once. That is a rate budget rather than a safety deadline, and any
  resulting throttle is detected and parks the exchange. Neither structure coordinates across
  processes: acceptable ONLY because of the single-instance decision (§16c).
- **`definite_rejection` is proven for Nobitex only.** Only Nobitex publishes a documented
  contract we can rely on (`status:"failed"` + `TooManyRequests`), so it is the only venue
  where a throttled mutation is re-queued. Every other venue's throttled PLACE/CANCEL takes
  the conservative AMBIGUOUS path (read-only probe, lock held) — correct but slower to
  resolve. Adding a venue means proving its contract, not pattern-matching text.
- **The dashboard has no live/preflight/reconcile/session/credential surfaces in this
  lineage.** Sections §14/§14a describe what the dashboard actually serves (read-only
  trading views + login + config editing). The `/api/live`, `/api/live/preflight`,
  `/api/live/acknowledge`, `/api/live/session`, `/api/reconcile*`, and `/api/credentials*`
  endpoints described in §16c/§16e/§16f/§16g/§16h and their PR rows in §20 were part of an
  earlier lineage and are **not present in the current code** — the rebuilt read-only
  dashboard (PR16) never re-added them. The underlying packages (`internal/preflight`,
  `internal/opreconcile`, `internal/credentials`, `live` session/ack) DO exist and are
  enforced by the guard; only their HTTP surfaces are missing, so those operations are
  SQL/admin-side today. Re-exposing them is pending work, tracked here so no reader
  assumes an endpoint that does not exist.
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

- **PR13 (correction) — balance-sync is real + per-exchange cadence is config-driven**: cut from
  accepted PR12; PR10–PR12 fixes preserved (full sweep). Clarified/confirmed that `balance-sync`
  is not a skeleton — `cmd/balance-sync` wires real DB-decrypted **read-only** balance clients
  (`Name`+`GetBalances` only; a missing master key idles safely). Wired the per-exchange,
  rate-limit-aware cadence end-to-end: `balance.Config.IntervalFor` + `MinInterval` floor +
  per-exchange due-tracking (already implemented) are now driven from the DB via
  `exchange_configs.balance_poll_interval_seconds` (migration 026 + `ExchangeConfig`), so venues
  poll on their own cadence (0/unset → the global default — backward compatible). The safety
  properties are unchanged: failed sync never zeros prior balances, a missing asset is never
  zeroed, history rows only on change, `last_seen_at` bumped each observation, decimals
  throughout, snapshots carry `exchange_id`+timestamp. Tests: `TestPerExchangeCadence` (different
  intervals / floor / default), `TestEffectiveIntervalFloor` (5s override, 20s MinInterval →
  20s), `TestFailureIsolationKeepsOthersAndPrevious`, `TestMissingAssetNotZeroed`, + configstore
  load of the new column.

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

- **PR20 correction — owner decisions**: NO daily order-count/quote limits (columns remain,
  never read, never block); SINGLE bot instance (no multi-instance cap coordination added).
- **PR20 correction — rate limits are detected comprehensively and never from 429 alone**:
  per-venue documented signals (status/code/body/headers) normalized into structured
  `RateLimitInfo` (retry_after/source/definite_rejection/code); iranArb-proven rules (nobitex
  backOff seconds + 15m cap; bitpin Retry-After + body regex); HTTP-200 throttle bodies are
  rate limits, never successes and never CatBadRequest; text matching is a tight fallback that
  cannot fire on "limit order".
- **PR20 correction — a throttled exchange is parked, alone**: per-exchange extend-only
  cooldown gates the claim loop (all request paths deferred pre-network), venue wait preferred
  then configured retry_backoff_ms then 60s, bounded 15m, safe logs, no busy loop.
- **PR20 correction — rate limits never blindly retry mutations**: only a venue-PROVEN
  pre-execution rejection re-queues the persisted mutation after the cooldown; everything else
  is ambiguous → read-only probes, lock held.
- **PR20 correction — rate_limit_per_sec/retry_backoff_ms are wired**: proactive per-exchange
  pacing + reactive fallback cooldown from the live configstore cache (fields are no longer
  dead config).
- **PR20 correction (round 2) — a denial must never strand a cycle**: a refused request is
  resolved through the official state path by RISK, not uniformly: entry buy → FAILED + lock
  released (nothing sent, no exposure); exit sell → NEEDS_RECONCILE + lock held (inventory may
  exist); cancel → DEAD + NEEDS_RECONCILE + lock held (the venue order may be open). Failing
  only the queue row would leave order=QUEUED + cycle open + lock held forever.
- **PR20 correction (round 2) — prove it from the database, never from the payload**: one
  authoritative join proves an entry buy's role/state/mode/ownership/market/symbol/live-flags,
  and the cancel path proves the exact `exchange_order_id` + ownership + a cancellable state.
  An unresolvable market is an ERROR, never market id 0 (which skipped the symbol-level check).
- **PR20 correction (round 2) — cancelled sells still consumed inventory**: oversell math counts
  `filled_quantity` from CANCELLED sells (a partial fill before cancellation is gone forever)
  plus the unfilled remainder of still-active sells; excluding an order by its final state
  permitted an oversell.
- **PR20 correction (round 2) — cooldowns are durable**: the park deadline lives in
  `exchange_cooldowns` (extend-only in SQL) and is reloaded before the first claim, so a restart
  cannot resume sending to a still-throttled venue. Pacing stays in-memory (a budget, not a
  safety deadline).
- **PR20 correction (round 2) — a successful response can still throttle**: exhausted-quota
  headers on an HTTP 200 pause FUTURE requests via a transport-level sink while the completed
  operation stays successful with its result intact; a 200 whose BODY says not-performed is not
  a success at all. The sink is a standalone object so the Executor's only exported method stays
  `Run` (rule #1's reflection guard).
- **PR20 correction (round 2) — pacing sits at the network boundary**: after guard/audit so a
  locally-denied request never consumes a pacing slot, but BEFORE `MarkInFlight` so a crash
  while pacing leaves the row CLAIMED (nothing sent) rather than IN_FLIGHT (maybe-sent).
- **PR20 correction (round 7) — mutating requests require cycle_id AND order_id**: Enqueue/
  EnqueueScheduled reject them (`ErrMalformedMutation`), Claim refuses them, and existing malformed
  rows are finalized DEAD + cycle NEEDS_RECONCILE + lock held (never stranded).
- **PR20 correction (round 7) — MarkInFlight at the real network boundary**: a two-stage
  `MutationPreparer` (Bitpin) does token/auth in Prepare BEFORE MarkInFlight, so a crash during
  token preparation leaves the row CLAIMED (definitely-not-sent), never a false ambiguity; prepare
  and send are paced separately (two HTTP calls → two slots).
- **PR20 correction (round 7) — Bitpin auth rate limit blocks the order**: an auth 429 (even with a
  cached token) or an auth 200 with X-RateLimit-Remaining:0 fails preparation as a
  definitely-not-sent rate limit — the order endpoint is never called, the cooldown is armed, and
  the retry waits for it.
- **PR20 correction (round 7) — every stale mutating IN_FLIGHT reaches a terminal decision**: a
  probe, or DEAD + cycle NEEDS_RECONCILE + lock held; temporary recovery failures are bounded by
  the recovery hard limit; one finalizer per row — none can sit IN_FLIGHT forever.
- **PR20 correction (round 7) — the lookup-by-client-id capability doc matches the code**: all
  three private venues implement it (Nobitex via recent-orders match, Wallex keyed by client id,
  Bitpin via the identifier endpoint) — the earlier "Wallex only" claim was wrong.
- **PR20 correction (round 8) — the two-stage mutation boundary is implemented for ALL THREE
  adapters, not just Bitpin**: Nobitex and Wallex now implement `PreparePlace`/`PrepareCancel` too
  (credentials, symbol, payload and the final `http.Request` built during preparation), so a crash
  during their preparation leaves the row CLAIMED, never a false IN_FLIGHT. The earlier round-7
  claim that only Bitpin needed it was wrong — the boundary property must hold for every adapter.
- **PR20 correction (round 8) — Bitpin builds the final request BEFORE MarkInFlight**: the fallible
  `http.NewRequestWithContext` moved into `PreparePlace`/`PrepareCancel`; `bitpinPrepared.Send`
  only binds the send context to the pre-built request and calls `http.Do`. No token/credential/
  symbol/payload/request construction happens after MarkInFlight (the type holds no material from
  which a request could be rebuilt).
- **PR20 correction (round 8) — pacing counts actual HTTP calls, not preparation-method calls**: the
  executor stopped pacing before `PreparePlace`; the adapter reserves an auth slot only when it
  actually makes the auth/refresh call (via the `WithNetworkPacer` hook). Fresh cached-token /
  in-memory-credential mutations consume ONE slot (order only); auth-or-refresh + order consumes
  TWO. Bitpin auth rate limits still stop the mutation endpoint (unchanged from round 7).
- **PR20 correction (round 8) — stale recovery proves request/order/cycle/exchange ownership**:
  `orderRecoveryInfo` loads the order's OWN cycle_id and exchange_id; `recoverOneStaleMutation`
  requires the queue row's claimed cycle (when present) and exchange to equal the order's before
  scheduling any probe. A mismatch — or a NULL claimed cycle_id with a valid order_id — is resolved
  by `orders.DisposeDeniedMutation` (Kind=Unknown → conservative): the cycle is DERIVED from the
  order, the request goes DEAD, the ACTUAL order + cycle go NEEDS_RECONCILE, the lock is HELD, and
  no unrelated cycle/lock is ever touched. No probe is ever built from mixed ownership.
- **PR20 correction (round 9) — stale/malformed candidate discovery is order-authoritative and
  mode-scoped in the REAL production flow**: `recoverStaleMutating` JOINs the persisted order and
  its cycle and scopes by the ORDER's cycle mode (not `er.exchange_id`/`er.cycle_id`), so a NULL
  claimed cycle, an unwired/foreign claimed exchange, or a cross-mode claimed cycle can no longer
  hide a row or route it to the wrong-mode executor. `sweepMalformedMutations` loads `order_id`,
  catches malformed AND inconsistent rows, derives cycle/exchange/mode from the order (else the
  claimed cycle), and only the executor whose mode matches the AUTHORITATIVE cycle finalizes a row
  — a live and a dry-run executor can never mutate each other's cycles or locks. A row with no
  trustworthy ownership is DEAD-only (no cycle touched). Previously these ran off the untrusted
  queue metadata, so a valid-`order_id`/NULL-`cycle_id` row left the order/cycle unchanged and a
  cross-mode row could be mutated by the wrong executor.
- **PR20 correction (round 9) — the Bitpin auth rate-limit deadline is propagated**:
  `bitpinAuthRateLimited` populates `RateLimitInfo.RetryAfter` (parsed 429 duration / remaining
  `authThrottledUntil` / `X-RateLimit-Reset`), so the executor schedules the queue retry at the real
  venue deadline instead of a 1s fallback that would exhaust `max_retries` before the throttle
  expired; an active window returns its remaining time without re-authenticating.
- **PR20 correction (round 10) — the malformed-sweep mode filter runs inside SQL before LIMIT**:
  with the filter in Go after `LIMIT n`, n rows of the other mode could fill the window on every
  sweep and permanently starve this executor's own rows; the authoritative-mode predicate (order's
  cycle when the order exists, else the claimed cycle, else mode-independent request-only) is now
  part of the query, with deterministic `ORDER BY er.id`.
- **PR20 correction (round 10) — stale finalization never touches the claimed cycle when a
  persisted order exists**: discovery RETAINS `o.cycle_id`/`o.exchange_id`/cycle mode on the
  candidate; if `orderRecoveryInfo` fails persistently past the recovery window,
  `finalizeStaleAuthoritative` resolves request → DEAD + ACTUAL order/cycle → NEEDS_RECONCILE +
  lock HELD from those retained values — the untrusted claimed cycle_id is never mutated.
- **PR20 correction (round 10) — no unclaimable recovery probe**: before scheduling a GET_ORDER
  probe, the executor verifies the order's exchange has a wired client AND `enabled=1` (Claim
  refuses disabled exchanges); otherwise it finalizes conservatively (DEAD + reconcile + lock held)
  instead of queueing a probe that no claim loop would ever pick up.
- **PR20 correction (round 11) — no recovery probe can remain permanently unclaimable**: a
  creation-time capability check cannot cover an exchange that is disabled / loses its credential /
  is not re-constructed AFTER a GET_ORDER probe is already QUEUED (esp. across a restart).
  `sweepUnclaimableRecoveryProbes` (startup + periodic) finalizes such probes on the AUTHORITATIVE
  order + cycle (DEAD + NEEDS_RECONCILE + lock HELD), mode-scoped inside SQL before LIMIT, order-
  authoritative, idempotent; and `recoverAmbiguousPlace`/`recoverAmbiguousCancel` re-check the
  recovery path before scheduling a probe. No unclaimable GET_ORDER stays QUEUED.
- **PR20 correction (round 6) — one authoritative disposition, cycle derived from the order**:
  `orders.DisposeDeniedMutation` handles buy/sell/cancel denial, malformed/pre-handler failure,
  and retry exhaustion; it reads the order+cycle `FOR UPDATE`, derives the cycle from
  `order.cycle_id` (never the queue's claimed cycle_id), proves request↔order↔cycle↔exchange, and
  can never fail/unlock an unrelated cycle when the claimed relationship is inconsistent.
- **PR20 correction (round 6) — pre-handler failures never strand**: a transient `order.role`
  read error re-queues (`RequeueClaimedUnsent`, still CLAIMED); a malformed CANCEL payload goes to
  NEEDS_RECONCILE + lock held — never a bare queue-row FAILED.
- **PR20 correction (round 6) — Bitpin auth/token failures are definitely-not-sent**: a
  `bearerToken` failure (auth 429, token timeout, invalid response) is wrapped `ErrNotSent`, the
  order endpoint is never called, and a 429 preserves its rate-limit so the cooldown is armed —
  no ambiguous order probe.
- **PR20 correction (round 6) — retry exhaustion resolves request+order+cycle+lock atomically**:
  an executor `onExhaust` callback runs `DisposeDeniedMutation` inside the requeue tx — entry-buy
  zero-exposure releases the lock, exit-sell/cancel hold it; the cycle is never left inconsistent.
- **PR20 correction (round 6) — temporary local not-sent uses real backoff**: exponential backoff
  with jitter (from `retry_backoff_ms`) for a temporary LOCAL failure, the cooldown deadline for a
  proven rate limit, no retry for a permanent one; `last_error` preserves the true reason.
- **PR20 correction (round 5) — a denied buy releases the lock only with proven no-exposure**:
  `orders.OnBuyDenied` reads the order+cycle `FOR UPDATE` and takes the clean-failure/lock-release
  path ONLY when order=QUEUED + filled=0 + exchange_order_id NULL + cycle pre-send; any
  uncertainty (a recovery advanced the order during pacing, a fill, an exchange id, a DB error)
  holds the lock and marks NEEDS_RECONCILE.
- **PR20 correction (round 5) — nothing synchronous runs between the final guard and
  MarkInFlight**: the PR24 first-order checklist (synchronous DB work) moved to AFTER the send,
  so the final guard is genuinely the last pre-send check.
- **PR20 correction (round 5) — pre-network failures are definitely-not-sent, never ambiguous**:
  `execution.ErrNotSent` (temporary vs permanent) marks credential/symbol/request pre-network
  failures; the executor also checks `sendCtx.Err()` before the client call; a temporary not-sent
  re-queues (proven-unexecuted), a permanent one fails terminally with the entry/exit disposition
  — no recovery probe either way.
- **PR20 correction (round 5) — RequeueProvenUnexecuted is atomic and status-guarded**: one
  transaction, `FOR UPDATE`, requires `IN_FLIGHT`, one retry increment, exhaustion → DEAD +
  order NEEDS_RECONCILE in the same tx; a terminal request can never be resurrected and two
  concurrent handlers produce exactly one retry.
- **PR20 correction (round 5) — the PR history statuses reflect the real accepted lineage**:
  PR1–PR19 are the accepted committed chain (parent `7ced7f5` = accepted PR19), PR20 is in
  review, and PR21+ are planned (rebuilt onto accepted PR20 later) rather than mislabelled
  accepted.
- **PR20 correction (round 4) — every tuning reload is validated**: the periodic refresh
  validates each snapshot with the same rules as startup and keeps the last known-good on
  failure; `RunValidated` also drops the redundant immediate reload after the validated startup
  load. An invalid/incomplete reload can never silently zero out pacing.
- **PR20 correction (round 4) — the guard runs again after pacing**: the initial guard can go
  stale in the pacer, so a FINAL fail-closed guard runs immediately before `MarkInFlight`,
  re-checking every time-sensitive condition at send time and writing the durable allow-audit
  there; the early guard is a cheap pre-filter that audits only denials.
- **PR20 correction (round 4) — the exchange timeout starts at the network boundary**: created
  after pacing/final-guard/MarkInFlight so the pacing wait never consumes the network timeout and
  a request delayed only inside the process is never mis-classified as an ambiguous mutation.
- **PR20 correction (round 4) — cooldown durability health is per exchange**: a persistence
  outage on exchange A disables only A's entry buys (proven exits and cancels stay available) and
  never touches exchange B; A auto-recovers when its writes catch up.
- **PR20 correction (round 4) — graceful shutdown flushes pending cooldowns**: a bounded final
  flush writes armed-but-unwritten parks so a restart restores them; a hard crash cannot be made
  perfectly durable (documented), and the loss is re-detected on the next throttle.
- **PR20 correction (round 4) — the payload proof is complete**: the exit-sell symbol is
  mandatory (empty is denied), time_in_force NULL semantics are exact (DB-null ⇒ payload must be
  empty), and the EXACT adapter-normalized client-order-id is verified non-empty, persisted, and
  proven by the final guard as the value that will be sent — no transformation after the guard.
- **PR20 correction (round 3) — the architecture doc must not describe superseded plans**: the
  §16c "deferred to PR20a / no real client" split was written before PR20a landed and had become
  actively misleading — it told a reader the guard was a harness when it is the last check in
  front of real money. Superseded plan text is a bug in its own right.
- **PR20 correction (round 3) — a successful venue response must never wait on our bookkeeping**:
  arming a cooldown is pure memory (the sink runs on the adapter's HTTP path); a slow durability
  write would delay a successful mutation's response, and a caller-side timeout would turn a
  CONFIRMED fill into an ambiguous outcome. Durability is a separate worker with bounded retries.
- **PR20 correction (round 3) — persistence retry is tracked by state, not by event**: pending is
  `persistedUntil != until`, never "this call extended the deadline" — otherwise a failed write
  followed by equal/shorter signals is never retried and a restart silently loses the park. When
  durability cannot be restored within the grace period, live sends are DISABLED (cancels are
  not) rather than continuing while pretending the cooldown is durable.
- **PR20 correction (round 3) — startup loads synchronously before it can send**: exchange tuning
  (loaded AND validated — in live mode a missing `exchange_configs` row is a startup failure, not
  "no pacing") and durable cooldowns are loaded before the first claim; failure aborts the
  binary. Only the periodic refresh is async.
- **PR20 correction (round 3) — the payload must equal the persisted order**: identity is not
  enough. Quantity/price/type/TIF/client-id/side of the request we are about to SEND are proven
  equal to the registered order (exact, no tolerance — unlike venue-response matching), and an
  exit must be routed to the market where the inventory was acquired.
- **PR20 correction (round 3) — pacing is exactly once and abortable**: one slot per request (a
  duplicate halves the rate budget), and a cancellation during pacing sends nothing and never
  marks a definitely-unsent request as ambiguous.
- **PR20 correction — guard hardening**: live+nil Guard denies; every safety query fails
  closed on DB errors; exits are DB-proven risk-reducing and exempt from entry controls;
  the allow-audit is durable before a real PLACE (failure ⇒ deny) while a cancel is never
  blocked by an audit outage.
- **PR20 — limited live is a safety PR with the final gate in the executor**: the
  `live.Guard` is the load-bearing check immediately before each real PLACE/CANCEL (mode/
  AllowLiveExecution/not-dry-run/exchange+symbol live-enabled/caps/credentials/kill-switch/
  state); the engine's `AllowNewBuyCycle` is a first check, not the only one. A denial
  fails the request without sending and is audited (`live_audit`).
- **PR20 — safe by default at every layer**: mode must be explicitly `live`; the kill
  switch defaults engaged (1); every REQUIRED cap in `live_controls` must be set (any
  missing → denied); `exchanges.live_enabled` + `exchange_markets.live_enabled` default 0.
  Required caps (migration 020): open-cycles, order-notional, base-qty,
  consecutive-failures, unresolved-reconcile. (The `max_daily_orders`/`max_daily_quote`
  columns from migration 020 are RETAINED but no longer read — see the owner decision
  above.)
- **PR20 — kill switch is asymmetric**: it blocks new buy cycles + new buy PLACEs (new
  exposure) but allows DB-PROVEN exit sell PLACEs (inventory exit — see the correction
  entry above), cancels, and status polls so open cycles stay safely managed.
- **PR20 — the guard sits in front of REAL sends (superseded split)**: PR20 was originally
  written expecting credential decryption to land later (PR20a), i.e. `live` wired no real
  client. That is no longer the case: the accepted parent contains PR20a's real-client
  wiring and PR22's provisioning, so `live` builds real clients with decrypted credentials
  and CAN send. The guard is the last thing between the queue and a real venue mutation —
  every decision below is enforced against real money, not a harness. What keeps live safe
  is operational readiness (master key, credential, live flags, caps, kill switch,
  preflight + ack + session), not the absence of wiring.
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
- **PR19 correction — rebuilt on accepted PR18 `a8a7036`** (branch `pr19-dry-run`, single
  commit); all PR14–PR18 work preserved (full sweep green).
- **PR19 correction — execution.mode is strictly validated**: exactly off/dry_run/live;
  empty → off; a typo (e.g. `dryrun`) is a hard startup error, never silently `off`. Both
  config examples ship an explicit `[execution] mode = "off"`.
- **PR19 correction — `off` creates no executable state**: the engine is wired
  `PrepareBuyCycles=false` in off mode, so it is strictly signal-only (no cycle/order/
  PLACE_ORDER-request/symbol-lock; never calls CreateBuyCycle).
- **PR19 correction — strict dry-run/live separation (two guards)**: `queue.Claim` filters
  by the owning cycle's `dry_run` (mode-scoped in-flight count too), so a dry-run executor
  never claims a live request and vice-versa; a final pre-send check (`abortOnModeMismatch`)
  refuses any mismatched send and leaves the request untouched (sweeper reverts it).
- **PR19 correction — simexec is PERSISTENT (migration 030 `sim_exchange_orders`)**: state
  survives restarts and is shared across instances, so a follow-up GET_ORDER on another
  instance resolves deterministically (no spurious NEEDS_RECONCILE). In dry-run the
  reconciler is wired READ-ONLY simexec clients so dry-run cycles are reconciled, not skipped.
- **PR19 round 2 — a timeout is an UNKNOWN outcome, recovered read-only, never blindly
  retried**: an ambiguous PLACE/CANCEL DEAD-letters the mutating request and schedules a
  read-only GET_ORDER probe (`ambiguous_place_probe` looked up by `client_order_id`;
  `ambiguous_cancel_probe` by the known exchange id) that determines the real state, records
  the actual filled qty, and continues the state machine; a still-open cancel is re-issued only
  after a read-only check PROVES it open, bounded by `maxRecoveryAttempts`; only after bounded
  recovery fails does it go to NEEDS_RECONCILE. Recovery is a persisted queue row, so it
  survives a restart and is safe across instances; fills stay idempotent (`UNIQUE(order_id,
  exchange_fill_id)` + deterministic id).
- **PR19 round 2 — simexec models accepted-but-timed-out place/cancel**: the accepted-timeout
  scenarios persist the order FIRST (with real state/fill) then return `ErrAckTimeout` without
  the exchange id (recover by client_order_id); cancel-timeout scenarios mutate state then time
  out; `GetOrder` resolves by exchange_order_id OR client_order_id; a re-placed identical
  client_order_id is immutable/idempotent, a different payload is a conflict
  (`UNIQUE(exchange_code, client_order_id)`), never an overwrite.
- **PR19 round 2 — reconciler never mixes real and dry-run clients**: it holds BOTH sets and
  routes strictly by `cycle.dry_run` (sim-only for dry-run cycles, real-only for real cycles);
  absent client → skip safely, no cross-mode query.
- **PR19 round 2 — the final live guard fails closed on cycle-mode uncertainty**: `cycleDryRun`
  errors on any DB error / missing cycle / invalid-or-NULL dry_run, and the pre-send check +
  live gate then refuse to send and leave the request recoverable ("cannot confirm mode → do
  not call the exchange").
- **PR19 round 3 — a first "not found" is never proof of non-placement**: after a place timeout the
  probe does bounded read-only retries with backoff (eventual-consistency: the sim's `hidden_probes`
  hides an accepted order for the first lookups); the lock stays HELD and the cycle is NOT failed on
  an early miss; only a `ReliableNotFound` venue lets an exhausted probe conclude provably-not-placed
  (else NEEDS_RECONCILE).
- **PR19 round 3 — lookup-by-client-id is a distinct capability from client-id-on-place**:
  `LookupByClientOrderID` (Nobitex via recent-orders match, Wallex keyed by client id, Bitpin via
  the `identifier` endpoint — all three private venues) vs `ClientOrderID`; recovery/reconciler
  probe by client id ONLY when the venue's capability is set, never passing a client id to an
  exchange-id-only endpoint.
- **PR19 round 3 — crash-after-send is recovered read-only**: the sweep converts a stale IN_FLIGHT
  PLACE/CANCEL (crashed after the exchange accepted, before the response) into a persisted read-only
  recovery probe, never a blind resend.
- **PR19 round 3 — the EXACT sent client id is committed before the network call**: the executor
  persists `client_order_id_sent = adapter.ClientOrderIDForSend(local)` (e.g. Nobitex's 32-char
  truncation) BEFORE sending, sends exactly that, recovers by it, and refuses to attach a recovered
  order whose immutable fields disagree.
- **PR19 round 3 — the simulator is deterministic across instances**: CancelOrder/GetOrder follow the
  order's OWN persisted scenario + stored immutable fields, never the current client's configured
  scenario.
- **PR19 round 3 — a mutating request never reaches the exchange without a cycle + order**: enforced
  at runtime (mode-scoped claim clause + pre-send fail-closed guard), keeping the generic queue
  decoupled from trading FKs; terminal recovered states are recorded directly (no redundant
  cancel/probe). The claim index is justified on the real MariaDB 10.6 `EXPLAIN` (§16b).
- **PR19 round 4 — stale-mutation recovery is ATOMIC and single-owner**: `recoverStaleMutating`
  claims each stale IN_FLIGHT PLACE/CANCEL with `SELECT … FOR UPDATE SKIP LOCKED` + a status
  re-check, schedules the read-only probe, and marks the mutation DEAD in one tx; the generic
  `SweepStuck` no longer touches mutating IN_FLIGHT, so the two paths cannot race (exactly one probe,
  never a premature reconcile).
- **PR19 round 4 — real-venue client-id recovery via `ClientOrderLookup`**: a dedicated
  `GetOrderByClientOrderID` interface (never `GetOrder`, which takes an exchange id). Wallex maps it
  to its client-id-keyed GetOrder; Bitpin to `GET /odr/orders/?identifier=`; Nobitex lists recent
  orders and matches the reliable `clientOrderId` (unique per user). `LookupByClientOrderID` is true
  iff a venue implements it, verified in adapter tests.
- **PR19 round 4 — the simulator looks up first, then replays the PERSISTED scenario**: a repeated
  PlaceOrder resolves an existing order by client id and returns a result derived ONLY from the
  stored order + stored scenario (conflict on a different payload) — a restart / a second instance
  with a different default cannot change an accepted order's behaviour.
- **PR19 round 4 — recovered-order identity is reliable-id + symbol + side (not exact quantity)**:
  the order is found BY a reliable identifier, and symbol/side must agree; quantity is NOT an
  exact-identity check (venues round; partial fills are smaller), so it never rejects the correct
  order.
- **PR19 round 4 correction — the recovery window is RUNTIME-configured, never hard-coded**:
  `[execution.recovery]` (+ `per_exchange` partial overrides) is parsed/validated at startup
  (invalid → hard error; hard safety bounds ≤100 attempts / ≤1h max_delay / ≤24h total) and wired
  into the executor by cmd/order-executor; partial overrides inherit the CONFIGURED global in both
  layers.
- **PR19 round 4 correction — every unresolved cancel-probe outcome uses the recovery window**:
  transient lookup failures (timeout/network/rate-limit/5xx/context) consume window attempts with
  jittered exponential backoff + `FirstProbeAt` TotalTimeout — never the generic queue retry
  policy; exhaustion = DEAD probe + NEEDS_RECONCILE in ONE transaction, lock held.
- **PR19 round 4 — recovery timing is configurable per exchange**: `RecoveryConfig`
  (max_attempts / initial_delay / max_delay / total_timeout) with bounded EXPONENTIAL backoff +
  jitter; the window closes on either bound → NEEDS_RECONCILE, lock HELD, no blind resend. The exact
  sent client id is persisted before the network call and the persist verifies EXACTLY ONE row
  (else fail closed).

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
- **PR18 correction — rebuilt on accepted PR17 `a2938db`** (branch `pr18-retention-worker`,
  single commit); all PR14–PR17 work preserved (full sweep green).
- **PR18 correction — one pinned connection**: GET_LOCK + settings + counts + DELETEs +
  the app_logs report all run on the SAME pinned `*sql.Conn` (RELEASE before close), so the
  lock and the deletes never split across pooled connections — correct with
  `SetMaxOpenConns(1)` (no self-hang), no two-worker overlap, lock released on every exit
  (incl. errors).
- **PR18 correction — safe bounds, no overflow, no default-on-invalid**: retention_days
  [1,3650], batch_size [1,50000], max_batches_per_run [1,10000], pause_ms [0,60000] —
  validated at runtime (invalid → per-table error, no DELETE, others continue) + DB CHECK
  (migration 029); cutoff via `AddDate(0,0,-days)` (no Duration overflow).
- **PR18 correction — lock-skip is a reported, logged outcome**: a lock-blocked run returns
  a completed report (`LockAcquired=false`, `SkippedReason`) and writes an app_logs entry; a
  run with any table error logs at `warn`, not `info`.

- **PR17 correction — rebased onto accepted PR16 `58d2429`** (branch
  `pr17-config-editing-login`); all PR16 read-only/deploy-safety fixes preserved.
- **PR17 — real login/session auth (not bearer tokens)**: `dashboard_users` (PBKDF2-SHA256
  password hash, role, active) + `dashboard_sessions` (only the sha256 hash of an opaque
  256-bit token; never plaintext) (migration 028). `POST /login`/`POST /logout`; HttpOnly,
  SameSite, Secure-when-HTTPS cookie. **Every** route except `/healthz` + `/login` (UI,
  `/api/*` reads, mutations, `/ws`) requires a valid session → 401. Roles
  viewer/config_operator/admin; editing needs config_operator+ (viewer → 403). Bootstrap:
  `dashboard -create-user user:role` (password from a hidden prompt/stdin, never a CLI arg).
- **PR17 — optimistic concurrency (no lost updates)**: every edit carries
  `expected_config_version`; the tx locks the active config_version `FOR UPDATE` and 409s
  (`ErrStaleConfigVersion`) on mismatch — two concurrent editors are serialized, exactly one
  wins, the loser gets 409 (deadlock on the hot row is retried so it never 500s).
- **PR17 — sell-management disable guard**: disabling `enabled_for_sell_manage` while the
  market has open exposure (BUY_FILLED/…/SELL_*/CANCEL_PENDING/NEEDS_RECONCILE cycle) → 409
  (`ErrSellManageExposed`). The two high-risk transitions (enable trading / disable
  sell-manage) require the **admin** role.
- **PR17 — every config edit is one versioned+audited+validated tx**: activate a new
  config_version, update only provided fields, write a config_change_audit row per field
  (real old/new/operator/reason). **`changed_by` is the authenticated session user, never
  client input** (a body `changed_by` is rejected as an unknown field). A **non-empty reason
  is mandatory** (else 400). Validation rejections → 400; no-op edits → 400.
- **PR17 — strict JSON**: `DisallowUnknownFields()` + exactly one object (second decode must
  be io.EOF) → trailing object/garbage/unknown field all 400.
- **PR17 — enable-flag hierarchy enforced** (trading ⊆ signal ⊆ collection) **plus
  trading ⟹ sell_manage** (correction round 3): trading can't stay on while sell management
  is off. Disabling *trading alone* still keeps managing open cycles' sells; only turning
  sell management off (which needs trading off too) is guarded by the exposure check.
- **PR17 — `exchange_markets` has no config_version column**, so flag edits stamp the
  version on config_versions + audit only; other config tables stamp it on the row.
- **PR17 — no order/cycle/queue/lock/credential mutation**: editable surfaces are only
  symbol/market-flags/exchange/fee config via the versioned configstore path; regime-basket
  editing + a credential-editing surface are deferred to their own later PRs.
- **PR17 — hot reload via existing periodic reloads** (configstore.Cache); no service
  restart for normal config edits. Active cycles keep their stamped config_version.
- **PR17 correction — sessions reflect LIVE user state**: session resolution joins
  `dashboard_users` and reads the current `role`, requiring `active=1` — disabling a user
  breaks their existing sessions (401) immediately, and a role change applies on the next
  request without a re-login (no stale session-row snapshot is trusted).
- **PR17 correction — password never on the command line**: `-create-user user:role` reads
  the password from a hidden TTY prompt (or stdin), so it can't leak via history/`ps`/logs;
  it is never in `os.Args`, never logged, and stored only as a PBKDF2 hash.
- **PR17 correction — exposure guard includes queued/submitted buys**: disabling
  sell-manage is blocked for `BUY_REQUEST_QUEUED`/`BUY_SUBMITTED` too (a queued/submitted
  buy can fill and strand inventory), alongside the filled/sell/reconcile states.
- **PR17 correction — multiple active versions fail safe**: the edit lock selects ALL
  active `config_version` rows FOR UPDATE and returns `ErrMultipleActiveVersions` (409) on
  2+, refusing to edit against an ambiguous config (no version/config/audit mutation).
- **PR17 correction — `UpsertFee` hardening**: real previous-fee read errors are returned
  (only `sql.ErrNoRows` is "no previous fee"), and a market-specific fee's
  `exchange_market_id` must belong to its `exchange_id` (else 400) — no version/audit on
  either rejection.
- **PR17 correction (round 3) — trading ⟹ sell-management invariant**: `UpdateMarketFlags`
  rejects (400) an effective state of `enabled_for_trading=true` + `enabled_for_sell_manage=false`
  (so a market can never take new buys the sell manager ignores).
- **PR17 correction — `CreateBuyCycle` re-checks the LIVE flags on the SAME row**: as step 0
  in its tx it re-reads `exchange_markets FOR UPDATE` (the same row `UpdateMarketFlags` locks),
  requiring both trading+sell-manage true else `ErrMarketNotTradable` (clean engine no-op) —
  serializing the two so a stale cached config can't open a buy after the DB flags were
  disabled, and a disable can't slip past an in-flight buy.
- **PR17 correction — offset upper bounds**: `sell_offset_bps` and `maker_price_offset_bps`
  bounded to [0, 10000) (≥10000 would make the price zero/negative and break sell/maker-buy
  creation on already-acquired inventory).
- **PR17 correction — fee audit identifies the market**: exchange-wide default fee audits as
  `exchange_default_fee`/`exchange_id`; a market-specific fee as `exchange_market_fee`/
  `exchange_market_id` (distinct rows per market of one exchange).
- **PR17 correction — docs**: dashboard package/UI text no longer claims the whole dashboard
  is read-only; it states PR16 reads are read-only and PR17 adds authed/audited config edits.
- **PR17 correction (round 4) — consistent lock order (no ABBA deadlock)**: `UpdateMarketFlags`
  now locks the `exchange_markets` row FIRST, then the active config version — the SAME order
  `CreateBuyCycle` uses (market row FOR UPDATE, then the `config_versions` FK lock via
  `cycles.config_version`). A concurrency test runs the REAL `UpdateMarketFlags` against
  `CreateBuyCycle` over many rounds (both orderings occur) with no deadlock reaching either
  caller and no orphan cycle/order/request/lock.
- **PR17 correction — fee no-op is `ErrNoChanges`**: `UpsertFee` compares old vs new fees by
  DECIMAL value (0.001 == 0.00100000); if neither changed it returns `ErrNoChanges` (400) BEFORE
  activating a version — no version/fee/audit. Audit rows are written per CHANGED field only
  (maker-only edit → one maker_fee row, etc.).

- **PR16 correction — rebased onto accepted PR15 `1996b07`** (branch
  `pr16-dashboard-readonly-errors`); PR14/PR15 fixes and the invariant scripts are
  preserved (full gated sweep green). PR16 is reconstructed as the STRICTLY read-only
  dashboard — the config-editing / auth / live / preflight / session / credential-admin
  handlers are **removed from PR16** and belong to their own later PRs (config editing =
  PR17, with explicit safety controls), so PR16's `Handler()` registers only GET routes.
- **PR16 — dashboard is read-only by construction**: the `Server` holds only a
  `*sql.DB` (no exchange client/queue — reflection guard) and registers GET-only routes,
  so any POST/PUT/PATCH/DELETE is 405 and there is no mutating/config-editing path at all.
- **PR16 correction — DB errors never become a partial 200**: `/api/cycles/{id}` and
  `/api/config` error-check EVERY sub-query (orders/fills/requests/events/locks/logs, and
  markets/exchanges/fees/regime/version) and return 500 on any failure — never a 200 with
  partial data. Only the cycle-lookup ErrNoRows is a 404.
- **PR16 — generic `jsonRows`** turns read-only SELECTs into JSON (decimals/JSON/text →
  strings, ints → numbers, NULL → null), so endpoints are thin SELECTs; lists take a
  defaulted + hard-capped `?limit=`; missing data → empty array (no panic).
- **PR16 correction — secrets never reach the browser (both log views masked)**:
  credentials table never read; BOTH the `api-logs` AND `app_logs` views (incl.
  cycle-detail logs) re-mask (defence in depth) sensitive key/values — a secret can't
  reach the browser even if one was written to app_logs upstream.
- **PR16 — queue display uses `step_kind`** (RETRY_SCHEDULED + retry_count 0 →
  scheduled_next_step, >0 → retry) so a planned simulated-IOC/reprice step isn't shown
  as a failed retry; balances expose a clean `stale` boolean (value never zeroed on
  absence); cycle detail carries a `fee_note` (realized_quote nets quote fees only).
- **PR16 — WebSocket is snapshot-only**: it sends a safe periodic snapshot and takes no
  commands from the socket — incoming messages are drained and ignored (no command
  handler exists), so live updates never control trading.
- **PR16 correction — WebSocket is same-origin only**: `CheckOrigin` (`sameOriginOnly`)
  no longer returns `true` unconditionally — a foreign `Origin` is rejected (403) so a
  cross-site page in the operator's browser can't read the live snapshot (matters on
  `0.0.0.0`); a missing `Origin` (non-browser client) is allowed and documented; a
  malformed/opaque origin is rejected. Configurable allowlist + auth are PR17.
- **PR16 correction — WebSocket snapshot never sends partial data**: `snapshot()` returns
  an error and every query is checked; on any failure the socket sends a generic
  `snapshot_error` event (raw DB details logged, not exposed) instead of a normal snapshot
  with silently-empty sections — the same "no partial success" rule as the HTTP endpoints.
  `stale` is a boolean, consistent with HTTP `/api/balances`.
- **PR16 correction — doc.go/architecture no longer claim PR16 edits config**: the package
  doc and the dashboard binary row state PR16 is strictly read-only and config editing /
  operator actions are PR17 (with explicit auth/authz/audit/validation).
- **PR16 correction — read-only ≠ safe to expose (deployment security)**: the PR16 dashboard
  is UNAUTHENTICATED, so `production.example.toml`/`config.example.toml` bind it to
  `127.0.0.1:8080` (not `0.0.0.0`), the docker-compose host port is published loopback-only
  (`127.0.0.1:8080:8080`), and the misleading "auth is via DB-stored bearer tokens" comment
  is removed. DEPLOY.md §3a + RUNBOOK.md document: keep it on localhost / behind a VPN or
  authenticated reverse proxy; never expose port 8080 to an untrusted network. Bearer tokens
  gate only the PR17+ mutating endpoints. `config.TestProductionExampleDashboardIsLoopbackOnly`
  enforces the loopback default; the WS also sets a bounded inbound frame read limit.

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
  (`state_hash`, migration 016 on current): direction, level, confidence, score,
  per-timeframe scores, per-symbol contributions, stale_reason, config_version — so
  confidence/score evolution (same label) and `UNKNOWN`-with-changed-reason are captured,
  while a genuinely identical regime is idempotent. A stale-data `UNKNOWN` is written to
  current (with reason), never a fabricated regime. History is FK-light + timestamp-indexed.
  (Clarification 1+2; see `regime.TestHistoryCapturesFullEvolution`.)
- **PR15 correction — rebased onto accepted PR14 `9f76cc4`** (branch
  `pr15-market-regime-history-validation`); all PR11–PR14 safety fixes and the invariant
  scripts are preserved (full gated sweep green).
- **PR15 correction — history is now self-describing** (migration 027 added `state_hash`
  + `stale_reason` to `market_regime_history`; `WriteResult` inserts both). Previously
  history omitted them, so it showed *that* the regime changed but not *what* changed
  (e.g. a changed `UNKNOWN` reason). A history row's `state_hash` now equals current's at
  that point. (`TestHistoryStoresStateHashAndStaleReason`.)
- **PR15 correction — `WriteResult` no longer swallows `SELECT state_hash` errors**: only
  `sql.ErrNoRows` means "no previous → changed"; any other DB/scan error is returned, so a
  transient failure can't produce a misleading history insert.
  (`TestWriteResultReturnsRealSelectError`.)
- **PR15 correction — invalid regime config is rejected in both layers**: migration 027
  CHECK constraints (weights/seconds > 0, thresholds non-negative + ordered) block bad
  inserts, and `Basket.Validate` (called by `LoadBaskets`) returns `ErrInvalidBasketConfig`
  so bad config never yields a regime result. (`TestBasketValidate`,
  `TestRegimeConfigCheckConstraints`, `TestLoadBasketsRejectsInvalidConfig`.)
- **PR15 — hosted in the trade-engine** (it already has the Redis client); the engine
  provides the `PriceSource` (Binance mid/bid from `price:` keys with the venue time).
  Engine consumption of the regime is deferred.

- **PR14 correction — rebased onto accepted PR13 `3b110ed`** (branch
  `pr14-health-monitor-readonly-private`); all PR11/PR12/PR13 safety fixes and the
  invariant scripts (`scripts/check-critical-invariants.sh`, `scripts/local-dryrun-check.sh`)
  are preserved (full gated sweep green).
- **PR14 — read-only health by construction**: the Monitor only invokes caller-supplied
  `ProbeFunc`s and holds no order client; a reflection test asserts nothing it holds can
  place/cancel. Probes are public `GetMarkets` (and a read-only balance call for private
  when creds exist).
- **PR14 — private health is wired read-only (Option A)**, via the same credential path
  as balance-sync: `Builder.BuildPrivate` → `Provider.ProbePrivateHealth(code, BalanceReader)`.
  The narrow `BalanceReader` makes place/cancel unreachable from the private probe (tested
  by `credentials.TestProbePrivateHealthIsReadOnly`). No exchange with a missing credential
  is ever forced unhealthy — it falls back to public-only, `private_status` UNKNOWN.
- **PR14 — continuous private probing never invalidates a credential on a transient error.**
  The health-monitor uses `ProbePrivateHealth`, not the strict one-shot `Provider.Validate`:
  it marks `status='invalid'` ONLY on a definite auth error (`isDefiniteAuthError` = the
  `execution.ErrAuthFailed` sentinel or a `NormalizedAPIError` category `auth`). Timeout /
  network / rate-limit / HTTP 429 / exchange 5xx / unknown leave the credential `active` and
  untouched — surfacing only as private health status — so a brief exchange incident can't
  permanently disable a valid credential (`BuildPrivate` builds only from `active` rows).
  Tested by `TestProbePrivateHealthCredentialPolicy` + `TestProbePrivateHealthSuccessAfterTemporaryFailure`;
  `Validate` stays strict for manual operator checks (`TestValidateStillStrictForManualCheck`).
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
  that exchange falls back to public-only and never panics — a deliberate safe fallback,
  not an accidental omission. No secrets logged/stored (messages truncated; adapter errors
  pre-masked). Migration 014 added the normalized status + failure-tracking columns.
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

**Accepted lineage (this branch).** The current branch `pr20-limited-live` is ONE commit (PR20,
**in review**) on top of the accepted committed chain PR1…PR19 (its parent is `7ced7f5`, the
accepted PR19). So PR1–PR19 are **accepted**, PR20 is **in review**, and PR21–PR27 are
**planned** designs to be rebuilt onto accepted PR20 in later branches — they are ahead of this
lineage and are NOT yet accepted here, despite being described in full below.

| PR | Branch | Status | Summary |
|---|---|---|---|
| PR1 | `pr1-project-skeleton` | **accepted** | Project skeleton & shared foundation: module layout, all 9 binaries bootable, **file-only bootstrap config** (no env; `-config` flag; secret redaction), slog logging, `db.Store`+pool+`WithTx`, Redis wrapper, in-code migration runner (GET_LOCK + checksum + DDL/DML rules) with `schema_migrations` + `001_app_meta`, scaffold packages, tests, this document. No trading logic. |
| PR2 | `pr2-database-schema` | **accepted** | Full trading schema (migrations `002`–`007`, 29 tables): reference/discovery, encrypted credentials + audit, versioned config + audit, trading core (cycles/orders/fills/events, composite-scope symbol_locks, exchange_requests queue), observability (balances/health/logs/comparison/signals), market_discovery_runs. Offline SQL unit tests + gated MariaDB integration tests (tables/indexes/FKs/uniques/enum/no-plaintext-creds/active-lock uniqueness). Schema only — no behaviour. |
| PR3 | `pr3-state-machine` | **accepted** | `internal/state`: CycleState/OrderState/RequestStatus enums, authoritative transition maps (no self-loops, no terminal exits, NEEDS_RECONCILE entry-only), `Validate*Transition`, `Apply{Cycle,Order}Transition` (tx + version-guarded CAS + atomic event insert + replay/stale/mismatch/missing disambiguation). Minimal `internal/models` (Cycle/Order/StateEvent). Table-driven transition tests + sqlmock Apply tests + real-MariaDB integration test. No trading behaviour; functions not yet wired into services. |
| PR4 | `pr4-exchange-abstraction` | **accepted** | Exchange abstraction layer (copy & adapt from iranArb): normalized `domain`/`execution` models, split `exchanges.PublicClient`/`PrivateClient` interfaces, `Capabilities`, `CredentialProvider`, `NormalizedAPIError`, factory registry, centralized secret-masking IO logger (+ migration `008`), tuned HTTP client. Adapters: Binance (public), Nobitex/Wallex/Bitpin (public+private), Ramzinex/Tabdeal/Exir (public). WS deferred for Iranian venues (capability flags honest). Fake private client for tests/dry-run. 77 exchange test funcs (httptest only, no live calls) + masking proof. No trading behaviour; adapters not wired into services. |
| PR5 | `pr5-collector-ws-reconnect` | **accepted** | Redis market-data layer + collector. `internal/events` (BookSnapshot/PriceSnapshot/MarketEvent with timestamps), `internal/redis` market store (orderbook:/price: keys + TTL, `market_events` pub/sub, ErrNotFound), `internal/collector` (Collector using only PublicClient; WS-or-poll; DB-driven targets; DB health recorder; `MarketStore`/`HealthRecorder` interfaces), `FakePublicClient`, cmd/collector wired. **Correction:** an unexpected WS close while ctx is active reconnects with capped exponential backoff (never silently abandons a target; only ctx-cancel stops it; counted as a health failure + `WSFailureCount`); `market_event` is published ONLY after both `SaveOrderBook` and `SavePrice` succeed; REST `received_at` is stamped after a successful `GetOrderBook`. Tests: events, collector (fakes: poll/WS/health/shutdown/public-only, **ws-reconnect-on-unexpected-close**, **no-publish-when-save-book/price-fails**), sqlmock targets+health, gated real-Redis round-trip. Redis stays cache-only; collector uses only PublicClient; no trading/order/cycle/credential code. |
| PR6 | `pr6-config-fee-scope-validation` | **accepted** | `internal/configstore`: DB-backed versioned trading config. `Snapshot` (MarketConfig merging exchange_markets flags + symbol_configs params, ExchangeConfig, fees, retention, active version), `Store.LoadSnapshot`/`ActiveVersion`, copy-on-write `Cache` + background `Run` reloader (non-blocking; keeps good config on reload failure), `ActivateVersion` + audited `UpdateMinSpreadBps` (version+audit in one tx, no secrets), validation (value sanity + enable-flag hierarchy), version-stamping helpers. **Corrections:** default fees are scoped per exchange (`DefaultFeesByExchangeID` keyed by exchange_id + `FeesByMarketID` keyed by exchange_market_id) with `Snapshot.FeeFor(exchangeID, exchangeMarketID)` (market override → THIS exchange's default, never another's) — replaces the unsafe single map where every default collided at key 0; `UpdateMinSpreadBps` validates BEFORE the tx (negative spread activates no version / mutates no symbol_config / writes no audit); `ActiveVersion` returns `ErrMultipleActiveVersions` instead of silently picking the latest; integration tests use per-run suffixes (repeat-safe). Tests: sqlmock loaders/version/audit, cache COW/reload/concurrent-read, validation, FeeFor scoping/priority (offline), gated fee-scoping/invalid-write-rejected/multiple-active-rejected + repeat-safe full-path. File-only bootstrap unchanged; no env config; not yet wired into a binary. |
| PR7 | `pr7-queue-recovery-guards` | **accepted** | `internal/queue` (DB-backed priority queue): Enqueue (idempotency-rejected), cross-process-safe Claim (GET_LOCK + count + FOR UPDATE SKIP LOCKED; priority/next_retry_at/per-exchange-limit/enabled/type filters), MarkInFlight, MarkSucceeded/Failed/Dead, ScheduleRetry (capped backoff→DEAD), conservative SweepStuck (read-only requeue / mutating→DEAD+order NEEDS_RECONCILE). `internal/executor` (order-executor): claim+dispatch loop, read-only & mutating handlers, conservative ambiguous→DEAD+reconcile, atomic complete+order-transition (rollback-safe), `AllowLiveExecution` guard (default off), NO direct-send path. **Corrections:** `SweepStuck` also recovers stale `CLAIMED` (never sent → requeued to QUEUED, claim cleared); `MarkInFlight` checks `RowsAffected` → `ErrRequestNotClaimed` (executor does not send); `MarkSucceeded/Failed/Dead` are status-guarded (`WHERE status IN ('CLAIMED','IN_FLIGHT')` + `RowsAffected`) → `ErrRequestNotActive` on a conflicting newer status, idempotent no-op on same status; definite `PlaceOrder` rejection moves the order out of `QUEUED` to `FAILED` via `ApplyOrderTransition` (already correct); ambiguous → `DEAD` + order `NEEDS_RECONCILE` (already correct). Tests: queue sqlmock + gated MariaDB (concurrent claimers, **stale-CLAIMED recovery**, **MarkInFlight zero-row**, **terminal status guards + idempotency**), executor classifiers + reflection no-send guard + gated end-to-end with fake clients (**MarkInFlight-failure-blocks-send**, definite-rejection-out-of-QUEUED, ambiguous-NEEDS_RECONCILE). Order/cycle state only via `internal/state`; nothing trades yet. |
| PR12 | `pr12-reconciler-safeclose-guards` | **accepted** | Cut from accepted PR11 (`pr11-ambiguous-lifecycle-safety`, `1333f09`); PR6–PR11 fixes preserved (FeeFor, queue guards, DB-role dispatch, buy/sell validation, empty-id safety, sell-rejection-keeps-lock, guarded PnL close — full sweep green). **Corrections:** (#2) exchange status `REJECTED` is NO LONGER advanced to a clean terminal — it is an execution anomaly → `NEEDS_RECONCILE` (never safe-close/lock-release; sell rejection keeps the lock); (#3) `safeClose` now checks, in the close tx, for any active `exchange_request` (QUEUED/CLAIMED/IN_FLIGHT/RETRY_SCHEDULED) and refuses to close / release the lock when one exists; (#4) `applyOrderOutcome` never silently skips an illegal transition — it diverts the order to `NEEDS_RECONCILE` (report never claims a non-advance). `internal/reconciler` (read-only; never auto-sends — holds a `ReadOnlyClient` with no Place/Cancel): `ReconcileStartup` + idempotent `RunPeriodic`; pure decision matrix (`decide.go`); capability-based known/unknown-exchange-order-id paths (unknown→never resend, positively-identify-or-NEEDS_RECONCILE); cycle decisions Continue/SafeClose/NEEDS_RECONCILE; **clean zero-fill safe-close → CANCELLED (NO_FILL) + lock release, NOT FAILED** (correction); missing/unknown order ≠ proof of no fill; decisions logged to app_logs; state via state machine. `internal/symbollock` read/release helpers (Acquire is PR9). cmd/reconciler wired (no clients). Tests: pure decide unit + gated MariaDB (decision matrix, safe-close+lock-release, ambiguous-keeps-lock, client-id attach, idempotent repeat, stuck-reporting, rollback, no-mutating-call guard). Completes the safety core (PR1–PR7 + PR12). |
| PR8 | `pr8-engine-signal-only` | **accepted** | `internal/engine` (trade-engine signal loop): subscribe `market_events`; read Redis books/prices + configstore snapshot; **owner-defined spread implemented as planned** = (Binance best bid − Iranian best ask)/ask×10000, fee-adjusted (taker buy + maker sell); USDT direct / IRT-IRR convert via same-exchange `USDT/IRT` rate (missing/stale → no signal); freshness + enable-flag + config-v0 gating; write `comparison_events` (every computable comparison) + `signals` (passed), config-version stamped, quote_unit + reference_rate audited. **SIGNAL-ONLY: the only writes are `comparison_events` + `signals` — no exchange calls, no cycle/order/exchange_request/symbol-lock writes, EVEN for a trading-enabled market with a passing signal.** Buy-cycle preparation is gated behind `Config.PrepareBuyCycles` (default FALSE) and is PR9's transactional `buyflow`. **The `cmd/trade-engine` binary leaves `PrepareBuyCycles: false` in PR8 — the real executable is signal-only; PR9 enables it.** **Corrections:** removed the unconditional `prepareBuy`/`buyflow` call from the signal path (now flag-gated, off by PR8 default); set `cmd/trade-engine` `PrepareBuyCycles: false` + a static invariant (script check #7 + `audit.TestTradeEngineSignalOnlyInPR8`) that fails the build if the binary enables it; fees via `Snapshot.FeeFor(exchangeID, exchangeMarketID)` (per-exchange default, no key-0 leak); `market_events` subscription resilient — an unexpected close while ctx is active resubscribes with capped backoff and only stops on ctx-cancel (never silently returns nil); a `USDT/IRT` quote-rate tick re-evaluates all signal-enabled rial-quoted markets on the same exchange; corrected the stale doc/comments that claimed PR8 refreshes pending buy intent. Migration 009 (audit columns); `MarketConfig.ExchangeID`. cmd/trade-engine wired (no private clients; **PrepareBuyCycles off — signal-only**). Tests: offline spread/quote/targets-quote-rate-dependents/subscription-reconnect/no-client/**trade-engine-signal-only-static-invariant** + gated MariaDB+Redis (USDT signal, below-threshold, stale/missing, disabled-for-signal, IRT conversion, fee-adjusted, per-exchange-default-fee + override, **signal-only-EVEN-when-trading-enabled (0 cycles/orders/requests/locks)**, USDT/IRT-reevaluates-dependent-IRT, config-stamp; PR9-gated cycle-creation tests enable the flag). |
| PR9 | `pr9-buyflow-refresh-guards` | **accepted** | `internal/buyflow` (+ `symbollock.Acquire`): first code that creates trading rows. Cut from accepted PR8 (`pr8-engine-signal-only`); `cmd/trade-engine` now sets `PrepareBuyCycles: true` (PR9 enables buy prep; the engine library still defaults it false as the gate). Fees come from `Snapshot.FeeFor(exchangeID, exchangeMarketID)` (per-exchange default, no key-0 leak — PR6). **Corrections:** (5) `RefreshActiveCycleBuy` now refreshes the FULL cycle signal snapshot (signal_time/prices/spread/fee_adjusted/buy_size/config_version), so cycle+order+request describe the same intent; (6) refresh is guarded on lock ACTIVE + cycle BUY_REQUEST_QUEUED + order QUEUED + request QUEUED (SELECT … FOR UPDATE), each guarded UPDATE re-asserts state and checks RowsAffected==1; (7) non-positive price/qty rejected (CreateBuyCycle errors, Refresh no-op) — venue tick/step/min validated at send, rejection handled cleanly by executor (PR7); (8) originating signal linked to the created/refreshed cycle (`signals.cycle_id`) in-tx. Removed the PR8-only "cmd must not enable PrepareBuyCycles" static invariant; tightened invariant #3 / `TestNoDirectStateUpdates` to flag state ASSIGNMENTS only (not the new guarded WHERE-clause state checks). On an accepted signal for a trading-enabled, fresh market it runs ONE transaction — insert cycle (config-stamped + signal context + execution mode) → acquire symbol lock (dup scope → `ErrSymbolLocked` → rollback, no orphan) → insert entry_buy order (`local_client_order_id`, limit, TIF NULL) → state machine cycle `NEW→SIGNAL_DETECTED→BUY_REQUEST_QUEUED` + order `NEW→REGISTERED→QUEUED` → enqueue `PLACE_ORDER` (deterministic idempotency key, full intent payload) → commit. Owner-defined maker-first/taker-fallback decision (`buyflow.Decide`, pure): maker limit below ask by `maker_price_offset_bps`, taker at ask after `maker_attempts_before_taker` maker attempts within `maker_signal_window_seconds`; persists intended mode/attempt/offset/ask. One shared attempt counter advances on create AND on refresh of the active scope (resets on window expiry). No-duplicate via the lock; the active cycle's still-QUEUED buy is **refreshed in place and re-decided** (so the SAME request escalates MAKER_FIRST→MAKER_RETRY→TAKER_FALLBACK without a duplicate); cycle-tied requests never deleted; CLAIMED/IN_FLIGHT never mutated. **Executes nothing** (no private client, no place/cancel/query, no fills, no lock release). Migration 010 (symbol_configs maker/taker cols + orders/cycles exec-mode cols); configstore loads the policy. Tests: offline Decide + gated (atomic create, rollbacks, dup-lock-blocks, maker→retry→taker across cycles, window reset, refresh-advances-attempt-and-escalates, refresh-window-expiry-resets, refresh-no-dup, CLAIMED/IN_FLIGHT untouched, idem-key unique, config stamp, flags/stale block, state-machine events, no private client). |
| PR10 | `pr10-place-validation-fill-safety` | **accepted** | `internal/orders` (buy-side order/fill processing) + executor wiring. Cut from accepted PR9 (`pr9-buyflow-refresh-guards`), preserving PR1–PR9 fixes (FeeFor, queue guards, subscription reconnect, buyflow refresh guards — verified by the full sweep). **Corrections:** (5) `BuyIntentPayload.Validate()` runs BEFORE MarkInFlight/PlaceOrder — a malformed/zero price/qty, wrong side/type/non-IOC, or empty client id is never sent (→ clean `OnPlaceRejected`: request+order+cycle FAILED, lock released); (6) a place ack with empty `ExchangeOrderID` schedules NO blind cancel/status → order+cycle NEEDS_RECONCILE, lock HELD; (7) a full/partial fill needs a usable cost basis — `usableAvgPrice` derives `ExecutedQuote/FilledQty` when `AvgPrice`≤0, and a full fill with neither is Ambiguous→NEEDS_RECONCILE (lock held, no fill row with zero price); (8) scheduled CANCEL/GET_ORDER keep `retry_count=0` (planned step, not a retry) — documented + tested. **Round 2:** (#1) an UNDECODABLE buy payload (with order/cycle context) now resolves via `OnPlaceRejected` (request+order+cycle FAILED, lock RELEASED) instead of only failing the request — no more stuck order/cycle/lock; (#2) PLACE_ORDER is dispatched by the **DB order role** (`entry_buy`/`exit_sell`), never `payload.side` — a wrong-side payload on a buy order routes to the buy handler and is rejected by `Validate()`, never slipping into the sell handler; `PayloadSide` removed; sell gets its own `SellIntentPayload.Validate` (bad sell → FAILED + NEEDS_RECONCILE, lock held). Simulated IOC as queued work (no worker sleeps): PLACE ack → `OnPlaceAck` (order QUEUED→SUBMITTED→ACKED, cycle →BUY_SUBMITTED, schedule CANCEL at `now+maker_wait`) → CANCEL ok/definite-reject → `OnCancelResult` (order →CANCEL_PENDING, schedule GET_ORDER) → `ProcessFinalStatus` (classify → fills + transitions + lock). Pure `Classify` (full/partial/zero/ambiguous); missing order ≠ zero fill; zero-fill → CANCELLED (`SIMULATED_IOC_ZERO_FILL`, lock released) not FAILED; partial → continue filled qty (lock held); full → BUY_FILLED (lock held); ambiguous (incl. ambiguous cancel/place) → order+cycle NEEDS_RECONCILE (lock held, never re-sent); definite place-rejection → `OnPlaceRejected` (FAILED + lock released). Fill accounting (filled/remaining/avg/quote/fee/fee_asset/`actual_execution_mode`/`fill_result`/`last_normalized_status`) + idempotent aggregate `fills` row (deterministic id). All state via `internal/state`; queue+state+fill+lock in one tx (never SUCCEEDED if state failed). Native IOC never forced (TIF empty). `queue.EnqueueScheduled`; `execution.OrderStatus.Liquidity`; migration 011; `BuyIntentPayload` moved to `internal/orders`. Tests (fake clients only): offline Classify matrix + gated (place→cancel→final scheduling, zero/partial/full, missing-not-zero, ambiguous-cancel→reconcile, place-rejected-clean, fee/avg, maker/taker, idempotent repeat, rollback) + executor end-to-end IOC loop. |
| PR11 | `pr11-sell-validation-pnl-safety` | **accepted** | Cut from accepted PR10 (`pr10-place-validation-fill-safety`, `622668b`); PR6–PR10 fixes preserved (FeeFor, PR7 queue guards, PR8 subscription reconnect + USDT/IRT, PR9 buyflow refresh guards, PR10 DB-role dispatch + buy validation + empty-id + cost-basis — all green in the full sweep). **Corrections:** (#2/#3) sell `PLACE_ORDER` is routed by DB order role (not `payload.side`) and the sell payload is validated before MarkInFlight/PlaceOrder (`SellIntentPayload.Validate`); invalid/undecodable/wrong-side sell → request FAILED + order/cycle NEEDS_RECONCILE, lock held. (#4) empty sell `ExchangeOrderID` → NEEDS_RECONCILE, lock held, no blind follow-up (also guarded in `ensurePoll`/`RepriceSell`). (#5) sell fill needs a usable cost basis (derive `ExecutedQuote/FilledQty`, else ambiguous → NEEDS_RECONCILE, no fill). (#6) `closeCycleWithPnL`/`writeCloseAccounting` check all query errors + validate buy/sell qty+quote positive + qty tolerance (`ErrIncompleteCloseAccounting`) → don't close with missing/invalid accounting (automatic path diverts to NEEDS_RECONCILE). (#7) `RepriceSell` with an empty resting-sell `exchange_order_id` → NEEDS_RECONCILE, lock held, no blind `CancelOrder("")`. `internal/sellflow` (exit sell create/reprice/Manager) + `internal/orders` sell processing + executor routing + engine driver. Sell on the ACTUAL filled inventory (`bought − sold`, step-floored), never the requested qty; partial buys sell their filled part (`BUY_PARTIALLY_FILLED→SELL_REQUEST_QUEUED`). Price `floor(binanceRef×(1−sell_offset_bps/10000), tick)`, min-order enforced; offset/tick/step/min are DB config (loaded into `MarketConfig`). `CreateSell` one tx (insert sell order → cycle→SELL_REQUEST_QUEUED + order NEW→REGISTERED→QUEUED → enqueue sell PLACE; rollback on failure; no-duplicate via active-sell guard). Resting place (`OnSellPlaceAck`, no auto-cancel) + Manager-driven `sell_status` poll (`ProcessSellStatus`): partial→SELL_PARTIALLY_FILLED (manage remainder), full→SELL_FILLED→CLOSED + PnL + lock release, ambiguous/missing→NEEDS_RECONCILE. Repricing cancel→replace, interval-gated (`reprice_interval_seconds`/`last_reprice_at`), skipped while a sell place/cancel is CLAIMED/IN_FLIGHT; cancel's final status always read before reselling; ambiguous→NEEDS_RECONCILE. Close writes exit accounting + `realized_quote` (fees netted only when quote-denominated; migration 012). All state via `internal/state`; queue+state+fill+lock atomic; engine never calls exchanges (executor only). Tests (fake clients): pure price/tick/step/min + gated sellflow (create full/partial, no-dup, below-min, tick-snap, rollback, reprice interval/in-flight/no-resting, Manager-creates-sell) + gated orders sell (place-ack-rests, partial-manages, full-closes+PnL, missing-ambiguous, idempotent, reprice-cancel partial/raced-full) + executor end-to-end sell loop. |
| PR13 | `pr13-balance-sync-per-exchange` | **accepted** | Cut from accepted PR12 (`pr12-reconciler-safeclose-guards`, `ac3abdb`); PR10–PR12 fixes preserved (DB-role dispatch, buy/sell validation, empty-id safety, cost-basis ambiguity, sell-rejection-keeps-lock, reconciler safe-close guards incl. stored REJECTED/FAILED — full sweep green). **Corrections:** (a) `balance-sync` is NOT a skeleton — `cmd/balance-sync` wires real DB-decrypted read-only credential clients (idles safely without a master key); (b) **per-exchange, rate-limit-aware cadence wired end-to-end** — `balance.Config.IntervalFor` + `MinInterval` floor + per-exchange due-tracking, driven from the DB via new `exchange_configs.balance_poll_interval_seconds` (migration 026 + `configstore.ExchangeConfig.BalancePollIntervalSeconds`), so venues poll on their own cadence (default for all when unset). `internal/balance` + `cmd/balance-sync`: continuous read-only balance sync. Narrow `BalanceClient` (only `Name`+`GetBalances` — no place/cancel reachable). Per poll, per exchange/asset: content hash `sha256(asset\|available\|locked\|total)` over canonical decimals; `wallet_balance_history` row only when the hash changes (no dup spam); `wallet_balances_current` upserted every observation with fresh `last_seen_at` (migration 013). Decimal end-to-end into `DECIMAL(36,18)` (never float; 18-dp preserved); `total` derived as available+locked when omitted. Bounded concurrency + per-exchange timeout; one exchange's failure/timeout is isolated and NEVER wipes/zeros prior balances; a missing asset is never zeroed/deleted (its row survives, `last_seen_at` goes stale). Changes no cycles/orders/queue. Binary wires no clients yet (credential decryption later) and idles safely; no secrets logged. Tests (fake read-only clients): offline hash + read-only-interface guard + no-clients startup; gated (first-obs current+history, unchanged-no-dup, changed-avail/locked add history, missing-asset-not-zeroed, failure-isolation-keeps-previous, precision, timeout-keeps-previous, context-cancel-stops). |
| PR14 | `pr14-health-monitor-readonly-private` | **accepted** | Cut from accepted PR13 (`pr13-balance-sync-per-exchange`, `3b110ed`); PR11–PR13 fixes preserved (sell-rejection-keeps-lock, executor empty-id boundary, reconciler safe-close guards incl. stored REJECTED/FAILED, per-exchange balance cadence, real read-only balance clients, failed-sync-doesn't-zero, missing-asset-not-zeroed) and invariant scripts (`scripts/check-critical-invariants.sh`, `scripts/local-dryrun-check.sh`) intact — full sweep green. **Correction (#3):** **private health is wired read-only (Option A)**, consistent with PR13 balance-sync — `cmd/health-monitor` builds the authenticated probe via `Builder.BuildPrivate` → `Provider.ProbePrivateHealth(code, BalanceReader)`; the narrow read-only `BalanceReader` makes place/cancel unreachable (tested `credentials.TestProbePrivateHealthIsReadOnly`); an exchange with no active credential / no master key falls back to public-only with `private_status` UNKNOWN — a **deliberate safe fallback, not an accidental omission**; stale/contradictory "private accidentally missing / wired in a later PR" doc + comments removed. **Correction (round 2):** the continuous private probe must NOT invalidate a credential on a transient error — new `Provider.ProbePrivateHealth` marks `status='invalid'` ONLY on a definite auth error (`isDefiniteAuthError` = `execution.ErrAuthFailed` or `NormalizedAPIError` category `auth`); timeout/network/rate-limit/429/exchange-5xx/unknown leave the credential `active` and untouched (surfacing only as private health UNAVAILABLE/DEGRADED/RATE_LIMITED, `last_success_at` preserved), so a brief incident can't permanently disable a valid credential (`BuildPrivate` builds only from `active`). Strict `Provider.Validate` retained for one-shot operator checks. Tests: `TestProbePrivateHealthCredentialPolicy` (auth→invalid; timeout/ack-timeout/network/rate/5xx/unknown→active), `TestProbePrivateHealthSuccessAfterTemporaryFailure`, `TestProbePrivateHealthIsReadOnly`, `TestValidateStillStrictForManualCheck`, offline `TestIsDefiniteAuthError`. `internal/health` + `cmd/health-monitor`: read-only per-exchange health. Monitor invokes only caller-supplied read-only `ProbeFunc`s (public `GetMarkets`; private balance read via `Validate` when creds exist) — no place/cancel reachable (reflection guard `TestMonitorHoldsNoOrderClient`); no cycle/order/queue writes. `Classify(err)` → normalized Status (HEALTHY/DEGRADED/UNAVAILABLE/AUTH_FAILED/RATE_LIMITED/UNKNOWN) + Category (timeout/network/exchange_5xx/exchange_4xx/auth/rate_limit/unsupported/invalid_response/unknown) from execution sentinels + NormalizedAPIError + ErrUnsupported + json errors; `context.Canceled` not recorded. `Recorder` upserts `exchange_health_current` (per-kind status, latency, last_success/failure, consecutive_failures reset-on-success, error/timeout/rate/auth counters, last_error_category/message) + appends `exchange_health_samples` (no FK, timestamp-indexed). Public/private tracked separately; auth error → api_key_status invalid; transient failure never wipes last_success; bounded concurrency + per-probe timeout isolate failures. Migration 014 (normalized status + failure-tracking cols); no secrets logged/stored. Tests (fake read-only probes): offline Classify matrix + read-only guard + no-targets startup; gated (healthy public, timeout/auth/rate/network/5xx/invalid classified+counted, private-auth→key-invalid, failure isolation, consecutive-then-reset, last-success preserved, context-cancel-stops). |
| PR15 | `pr15-market-regime-history-validation` | **accepted** | Cut from accepted PR14 (`pr14-health-monitor-readonly-private`, `9f76cc4`); PR11–PR14 fixes preserved (sell-rejection-keeps-lock, executor empty-id, reconciler safe-close incl. stored REJECTED/FAILED, per-exchange balance cadence + real read-only clients, health-monitor read-only private health that never invalidates a credential on a transient error) + invariant scripts intact — full sweep green. **Corrections:** (#2) `market_regime_history` now stores `state_hash` + `stale_reason` (migration 027) and `WriteResult` inserts them, so history is self-describing (a changed UNKNOWN reason is visible, not just "something changed"); a history row's `state_hash` equals current's at that point (`TestHistoryStoresStateHashAndStaleReason`). (#3) `WriteResult` no longer ignores `SELECT state_hash` errors — only `sql.ErrNoRows` → changed; any other error is returned, never a misleading insert (`TestWriteResultReturnsRealSelectError`). (#4) regime config validated in BOTH layers — migration 027 CHECK constraints (symbol/timeframe weight>0, timeframe seconds>0, interval/neutral>=0, moderate>=neutral, strong>=moderate) + `Basket.Validate` called by `LoadBaskets` returning `ErrInvalidBasketConfig` so invalid config yields no regime (`TestBasketValidate`, `TestRegimeConfigCheckConstraints`, `TestLoadBasketsRejectsInvalidConfig`). `internal/regime` + migration 015/016/027 + trade-engine wiring: market-regime calculation from Binance prices read ONLY from Redis (no Binance calls; structural guard asserts no order client; no cycle/order/queue/lock writes). DB-configurable baskets (`market_regime_baskets`/`_basket_symbols`/`_timeframes`): symbols+weights, timeframes+weights, neutral/moderate/strong thresholds, update interval, config version. `regime.Calculate` (pure, decimal math): multi-timeframe momentum from a rolling per-symbol price series — per-symbol bps change vs ~T-ago reference, weighted across symbols then timeframes → score; direction (BULLISH/BEARISH/NEUTRAL/UNKNOWN) + level (STRONG/MODERATE/WEAK/FLAT/UNKNOWN) from thresholds; confidence = fresh-symbol-frac × timeframe-coverage-frac. Stale/missing symbol excluded (lower confidence); no fresh data → UNKNOWN + stale_reason (never fabricated); Redis miss records nothing (no crash). `market_regime_current` upserted (idempotent); `market_regime_history` appended per distinct FULL-FIELD `state_hash` (direction/level/confidence/score/timeframe_scores/symbol_contributions/stale_reason/config_version); both config-version-stamped, FK-light + timestamp-indexed. Calculator samples Redis into the series + recomputes per basket interval; engine hosts it + provides the PriceSource (binance mid/bid). Tests: offline calc matrix + config-validation matrix + no-order-client guard; gated (load config, current-upsert + history-on-change + full-evolution + state_hash/stale_reason stored, select-error-returned, config CHECK rejections, LoadBaskets rejects invalid, calculator samples+persists, redis-miss no-crash/no-fabricate). |
| PR16 | `pr16-dashboard-readonly-errors` | **accepted** | Cut from accepted PR15 (`pr15-market-regime-history-validation`, `1996b07`); PR14/PR15 fixes preserved (health-monitor read-only private health that never invalidates a credential on a transient error; regime history state_hash/stale_reason + WriteResult select-error handling + config validation + migration 027) and invariant scripts intact — full sweep green. **Reconstructed as STRICTLY read-only:** the config-editing/auth/live/preflight/session/credential-admin handlers are **removed from PR16** (they belong to later PRs; config editing = PR17 with explicit safety controls), so `Handler()` registers ONLY GET routes. **Corrections:** (#6) `/api/cycles/{id}` error-checks EVERY sub-query (orders/fills/exchange_requests/cycle_state_events/order_events/symbol_locks/app_logs) → 500 on any failure, never a partial 200 (cycle-not-found → 404). (#7) `/api/config` error-checks every config query (markets/exchanges/fees/regime_baskets/active-version) → 500 on any failure, never incomplete config with 200. (#8) `app_logs` are masked before display (message + fields + any string), same defence-in-depth as `api_call_logs` — both the `/api/logs` endpoint and cycle-detail `logs`. `internal/dashboard` + `cmd/dashboard`: Server holds only a `*sql.DB` (no exchange client/queue — reflection guard); all routes GET-only so any POST/PUT/PATCH/DELETE is 405; no place/cancel/cycle/order/queue/lock/config mutation. GET JSON endpoints: cycles open/closed/{id}-detail, orders, fills, requests, signals, comparisons, balances, health, regime, logs, api-logs (masked), config (read-only snapshot incl. regime baskets), `/ws`, index, healthz. Generic `jsonRows` (SELECT→JSON); `?limit=` defaulted+capped; missing data→empty array (no panic). Queue `step_kind` (RETRY_SCHEDULED rc==0→scheduled_next_step, rc>0→retry). Balances `stale` flag (never zeroed on absence). **Round 2:** (1) `doc.go` + architecture no longer claim PR16 edits config — strictly read-only, config editing = PR17 (auth/authz/audit/validation). (2) WebSocket `CheckOrigin` is **same-origin only** (`sameOriginOnly`): foreign Origin → 403, missing Origin (non-browser) allowed + documented, malformed/opaque rejected — no more `CheckOrigin=true`. (3) `snapshot()` returns an error and checks EVERY query; any failure → generic `snapshot_error` event (raw DB details logged, never exposed), never a partial normal snapshot; `stale` normalized to bool (consistent with HTTP). WebSocket snapshot-only (open cycles/health/regime/balances) — drains + ignores incoming messages, no command handler. Separate binary (restart isolates). Tests: offline (no-order-client guard, exhaustive POST/PUT/PATCH/DELETE→405 over all routes, same-origin matrix, step_kind, mask-secrets) + gated (endpoints missing-data 200, seeded cycle detail + maker/taker + 404, cycle-detail 500-on-each-subquery-failure, /api/config 500-on-each-query-failure, app_logs masked + cycle-detail logs masked, ws foreign-origin-403/same-origin/missing-origin, ws snapshot_error-on-each-query-failure, ws stale-is-bool, ws-ignores-commands-no-mutation, retry-vs-scheduled, balances stale + value-preserved + api-log masking, pagination limit, WebSocket snapshot). |
| PR17 | `pr17-config-editing-login` | **accepted** | Cut from accepted PR16 (`pr16-dashboard-readonly-errors`, `58d2429`); all PR16 read-only/deploy-safety fixes preserved. `internal/dashboard` (auth/admin) + `internal/configstore` (admin) + migration 028: **real login/session auth** + authenticated, authorized, versioned, audited, validated, **concurrency-safe** config EDITING. Still no trading: no place/cancel, and no cycle/order/queue/lock/credential mutation route. **Login:** `dashboard_users` (PBKDF2-SHA256 password hash — never plaintext — role, active, last_login_at) + `dashboard_sessions` (only sha256(token) stored, never the token; expiry/revoke). `POST /login`/`/logout`; HttpOnly + SameSite + Secure-when-HTTPS cookie. EVERY route except `/healthz`+`/login` (UI, `/api/*`, mutations, `/ws`) needs a valid session → 401. Roles viewer/config_operator/admin; editing needs config_operator+ (viewer → 403). Session resolution **joins dashboard_users** for the LIVE role + `active=1`, so disabling a user breaks their existing sessions (401) and role changes apply without a re-login. Bootstrap via `dashboard -create-user user:role` (**password read from a hidden prompt/stdin, never a CLI arg**). **Optimistic concurrency + single-active:** every edit body carries `expected_config_version`; the tx locks ALL active config_version rows FOR UPDATE (no LIMIT) + the target row FOR UPDATE, requiring exactly one active (0→ErrNoActiveVersion, **2+→ErrMultipleActiveVersions/409**, refusing to edit) and 409s on version mismatch (`ErrStaleConfigVersion`) — concurrent editors serialized (exactly one wins, loser 409; deadlock retried so never 500). **Sell-manage guard:** disabling `enabled_for_sell_manage` with open exposure (**BUY_REQUEST_QUEUED/BUY_SUBMITTED**/BUY_PARTIALLY_FILLED/BUY_FILLED/SELL_*/CANCEL_PENDING/NEEDS_RECONCILE) → 409 (`ErrSellManageExposed`); high-risk switches (enable trading / disable sell-manage) require admin. **Fees:** `UpsertFee` returns real previous-fee read errors (only ErrNoRows = none) and validates `exchange_market_id` belongs to `exchange_id` (else 400) — no version/audit on rejection. **Audit:** each edit = ONE tx: activate new config_version + update provided fields + config_change_audit per field (real old/new/reason/`changed_by`=authenticated session user, NEVER client input); **non-empty reason mandatory** (else 400). **Strict JSON:** DisallowUnknownFields + exactly-one-object (2nd decode io.EOF) → trailing/garbage/unknown → 400. Validation (min_spread≥0, buy_size>0, unit∈{base,quote}, offsets/intervals/retries sane, taker_mode=ASK, fees≥0) → 400; enable-flag hierarchy trading⊆signal⊆collection; no-op → 400. Editable: symbol config, market flags, exchange config, fees; read `GET /api/audit`, `/api/me`. exchange_markets has no config_version col → version on config_versions+audit. Hot reload via configstore.Cache; active cycles keep stamped config_version. Regime + credential editing deferred to later PRs. Tests: offline (password hash round-trip, role ranking, 405 matrix, same-origin) + gated (login valid/wrong-pw/disabled/unknown, logout invalidates, all routes 401 w/o login, WS 401 w/o login, viewer read-not-edit, secrets-not-plaintext; versioned+audited edit w/ session changed_by + real old value, stale→409 + rollback leaves active, concurrent→1 ok/1 conflict + 1 active version, sell-manage disable blocked-by-exposure(3 states)/allowed-no-exposure, high-risk-requires-admin, reason mandatory + can't-impersonate, strict-JSON trailing/garbage/unknown; disabled-user-loses-session, role-change-applies-to-existing-session, multiple-active-versions→409-no-mutation, UpsertFee-returns-read-error, fee-market-ownership-validated; trading-requires-sell_manage(400), offset-bps-upper-bound[0,10000), fee-audit-identifies-market vs exchange, and buyflow CreateBuyCycle-rejects-when-trading/sell-manage-disabled + real-UpdateMarketFlags-concurrency (both orderings, no deadlock/orphan over 25 rounds); fee no-op→ErrNoChanges (decimal-equal) + per-changed-field audit). |
| PR18 | `pr18-retention-worker` | **accepted** | Rebuilt on accepted PR17 (`a2938db`); all PR14–PR17 work preserved (full sweep green). `internal/retention` + `cmd/retention-worker` + migrations 018/029: controlled retention of high-volume operational tables. Fixed whitelist (api_call_logs/comparison_events/exchange_health_samples/app_logs/wallet_balance_history/market_regime_history, all created_at); permanent tables (cycles/orders/fills/signals/symbol_locks/exchange_requests) absent → never deletable. **Correction #2 — one pinned `*sql.Conn` for the whole run:** GET_LOCK + load settings + counts + batch DELETEs + app_logs report all run on the SAME pinned connection, with RELEASE_LOCK before close — correct under `SetMaxOpenConns(1)` (no self-hang) and lock stays tied to the deleting connection (no two-worker overlap); lock released on every exit incl. errors. **Correction #3 — safe bounds:** retention_days∈[1,3650], batch_size∈[1,50000], max_batches_per_run∈[1,10000], pause_ms∈[0,60000], validated at runtime (out-of-range → per-table validation error, no DELETE, others continue — never defaulted) + DB CHECK (migration 029); overflow-safe cutoff via `AddDate(0,0,-days)`. **Correction #4 — lock-skip reported + logged:** a lock-blocked run returns a completed report (`LockAcquired=false`, `SkippedReason`) AND writes an app_logs entry; a run with any table error logs at `warn` not `info`. Missing/NULL/<1 days → not configured (do nothing); disabled → skip. Batched DELETE…LIMIT (short-batch exit, optional pause) — never one huge delete. Dry-run reports cutoff + estimated rows, deletes nothing. No Redis, no exchange calls; file-config + `-dry-run` flag only (no runtime env). Binary runs once then every 6h. Tests: offline whitelist/permanent guard + bounds-validate matrix (boundary accept + all out-of-range reject); gated missing/disabled/dry-run no-op, batch-only-old + recent-preserved, batch/max honored, permanent-never-targeted, one-table-failure→warn+continue, invalid-setting→skip+continue, DB-bounds-reject, MaxOpenConns(1)-no-hang, two-workers-no-overlap, lock-released-on-error, lock-skip-reported+logged, run-recorded, ctx-cancel-clean. |
| PR19 | `pr19-dry-run` | **accepted** | Rebuilt on accepted PR18 (`a8a7036`); all PR14–PR18 work preserved (full sweep green). `internal/simexec` + migrations 019/030 + config `[execution] mode` + engine/buyflow/executor/queue/dashboard/reconciler wiring: dry-run trading mode runs the FULL lifecycle through the REAL queue/executor/order-processing/sellflow boundaries against a SIMULATED client — no real PlaceOrder/CancelOrder ever sent. **Correction #5 — mode strictly validated:** exactly off/dry_run/live; empty→off; a typo (dryrun/DRY_RUN/…) is a hard startup error (config.Validate), never silently off; both config examples ship explicit `[execution] mode="off"`. **Correction #3 — off creates no executable state:** engine wired `PrepareBuyCycles=false` in off mode → strictly signal-only (records comparison/signal but NO cycle/order/PLACE_ORDER-request/symbol-lock; never calls CreateBuyCycle). **Correction #2 — strict dry/live separation (two guards):** `queue.Claim` takes a dry_run filter joining cycles (mode-scoped in-flight count too), so a dry-run executor claims only dry_run=1 requests and a live executor only dry_run=0; a final pre-send `abortOnModeMismatch` refuses any mismatched PlaceOrder/CancelOrder and leaves the request untouched (sweeper reverts). **Correction #4 — simexec PERSISTENT (migration 030 sim_exchange_orders):** order state survives restarts + is shared across instances (keyed by SIM-<client_order_id> + stored scenario), so a follow-up GET_ORDER on a new instance resolves deterministically instead of ErrOrderUnknown→NEEDS_RECONCILE; in dry_run the reconciler is wired READ-ONLY simexec clients so dry-run cycles are reconciled (not skipped). `simexec.Client` (no network) satisfies exchanges.PrivateClient; scenarios full/partial/zero/ambiguous/rejected/place_timeout/cancel_race. Engine never closes cycles directly. Dashboard surfaces dry_run on cycles/orders/requests/fills. **Round 2 — ambiguous execution (7 blockers): (1)** simexec models accepted-but-timed-out PLACE (`place_timeout_accepted_{open,partial_fill,full_fill}` persist first then ErrAckTimeout WITHOUT the exchange id; `place_timeout_not_accepted` persists nothing) with an explicit mutable lifecycle (`status`,`filled_quantity`); **(2)** GetOrder resolves by exchange_order_id OR `client_order_id` (migration 030 adds `UNIQUE(exchange_code, client_order_id)`); **(3)** an ambiguous PLACE/CANCEL DEAD-letters the mutating request and schedules a read-only GET_ORDER recovery probe (`ambiguous_place_probe` by client_order_id → resume ack flow / provably-not-placed→clean-fail; `ambiguous_cancel_probe` → record actual fill & continue), NEVER a blind retry, NEEDS_RECONCILE only after bounded recovery; **(4)** cancel-timeout scenarios (`cancel_timeout_{but_canceled,still_open,partial_then_canceled,filled_before_cancel}`) mutate then time out; still-open → bounded proven re-cancel; **(5)** reconciler holds BOTH real+sim client sets and routes strictly by `cycle.dry_run` (never mixes; absent client → skip safely); **(6)** the final live guard fails closed on any cycle-mode DB error/missing/NULL (`cycleDryRun (bool,error)`) → send nothing, request stays recoverable; **(7)** immutable simexec idempotency (identical re-place → same order, no second row; different payload → conflict). Migration 031 `idx_cycles_dry_run_state`. Fills idempotent (`UNIQUE(order_id,exchange_fill_id)` + deterministic id) → no double-apply across instances. Tests: offline + gated — simexec accepted/cancel-timeout matrix, idempotency, client-id lookup; executor place/cancel-timeout recovery (full/open/partial/not-accepted, canceled/filled/partial/still-open-bounded), cross-instance recovery, no-double-fill, fail-closed live guard; reconciler routes-by-dry-run-never-mixes + skips-when-mode-client-absent. **Round 3 — ambiguous-execution hardening (8 blockers, migration 032; verified on MariaDB 10.6): (1)** a first `ErrOrderUnknown` is NOT proof of non-placement — bounded read-only retries with backoff (sim `hidden_probes` models eventual-consistency delayed visibility), lock HELD, cycle NOT failed; only a `ReliableNotFound` venue after bounded probes → provably-not-placed (else NEEDS_RECONCILE); **(2)** new capability `LookupByClientOrderID` (Wallex-only) distinct from `ClientOrderID` (place) — recovery/reconciler probe by client id only when GetOrder truly accepts one, never into an exchange-id-only endpoint; **(3)** crash-after-send recovery: the sweep converts a stale IN_FLIGHT PLACE/CANCEL into a persisted read-only probe (`recoverStaleMutating`), never a blind resend; **(4)** the EXACT `adapter.ClientOrderIDForSend(local)` (e.g. Nobitex 32-char truncation) is committed to `client_order_id_sent` BEFORE the send, recovered by it, and a mismatched recovered order (side/qty/symbol) is never attached; **(5)** simexec CancelOrder/GetOrder use the order's OWN persisted scenario + stored immutable fields (order_type/tif), deterministic across instances; **(6)** a mutating request never reaches an exchange without a cycle+order — mode-scoped claim clause + pre-send fail-closed guard (runtime, not a CHECK, keeping the generic queue decoupled); **(7)** terminal recovered states recorded DIRECTLY (`RecoverBuy/SellPlace|Cancel`) — no redundant cancel/GET_ORDER; **(8)** claim index justified on the real 10.6 `EXPLAIN` (idx_exreq_claim serves the per-status index-ordered scan; the OR-filesort measured ~0.1ms/3334 rows, split 1.06x → left as-is; idx_cycles_dry_run_state proven used by the dry_run subquery). Tests: delayed-visibility recovery, reliable-vs-unreliable negative, crash-place/crash-cancel recovery, persist-client-id-before-send, mismatched-order-not-attached, deterministic-cancel-across-instances, immutable-fields-include-type/tif, cycleless-mutating-fails-closed. **Round 4 — recovery hardening (5 blockers; verified on MariaDB 10.6): (1)** stale-mutation recovery is ATOMIC (`SELECT … FOR UPDATE SKIP LOCKED` claim + status re-check + schedule probe + mark DEAD in one tx) and `SweepStuck` no longer touches mutating IN_FLIGHT — exactly one probe, never a premature reconcile (concurrency test with two recoverers + the sweeper); **(2)** real-venue recovery via a dedicated `ClientOrderLookup.GetOrderByClientOrderID` (Wallex client-id GetOrder; Bitpin `?identifier=`; Nobitex list-recent-orders + match the reliable `clientOrderId`), `LookupByClientOrderID` true iff implemented (adapter tests for Bitpin + Nobitex); **(3)** the simulator looks up FIRST and replays the order's OWN persisted scenario (not the current instance's) — a different-instance re-place follows the original scenario, no second row; **(4)** recovered-order identity = reliable id + symbol + side (quantity is NOT an exact-identity requirement); **(5)** per-exchange configurable recovery timing (`RecoveryConfig` max_attempts/initial/max/total) with bounded exponential backoff + jitter — window-expiry → NEEDS_RECONCILE, lock HELD, no resend; and `client_order_id_sent` persistence verifies EXACTLY ONE row updated (else fail closed). Tests: two-instance-recovery-plus-sweeper-no-race, cross-instance-persisted-scenario, Bitpin/Nobitex client-id lookup, wrong-side-not-attached, configurable window-expiry. **Round 4 correction (4 fixes; verified on MariaDB 10.6): (1)** the recovery window is RUNTIME-configured — `[execution.recovery]` + `[execution.recovery.per_exchange.<code>]` in the bootstrap TOML (documented safe defaults 6/1s/30s/5m), validated at startup (max_attempts>0, initial>0, max>=initial, total>0, safety bounds ≤100/≤1h/≤24h; negatives/violations = hard error), wired via `recoveryFromConfig` into `executor.Config`; partial per-exchange overrides inherit the CONFIGURED global (config resolution AND the executor merge — never hard defaults); **(2)** transient cancel-probe lookup failures (timeout/network/rate-limit/5xx/context) consume attempts of the SAME persisted window (attempt + FirstProbeAt + jittered exp backoff + TotalTimeout), never `q.ScheduleRetry`/queue max_retries (probe rows keep retry_count=0); **(3)** window exhaustion = DEAD probe + NEEDS_RECONCILE in ONE tx (`deadReconcile`), no crash window; **(4)** `SweepStuck` doc updated (mutating IN_FLIGHT is skipped for `recoverStaleMutating`, never dead-lettered here). Tests: config parse/defaults/validation matrix, cmd wiring incl. partial-inherit, transient-window-not-queue-retries (probe-row count, retry_count=0, 1 CANCEL_ORDER only, lock ACTIVE, atomic DEAD+NEEDS_RECONCILE), TotalTimeout expiry, executor-side partial inherit + backoff cap/jitter bounds. Full `go test -p 1 ./...`, `go vet`, invariants all green on MariaDB 10.6. |
| PR20 | `pr20-limited-live` | **in review** | `internal/live` (Guard) + migration 020 (`live_controls` singleton + `exchanges`/`exchange_markets`.live_enabled + `live_audit`) + migration 033 (`exchange_cooldowns` — durable rate-limit park deadlines) + executor/engine/dashboard/cmd wiring: the limited-live SAFETY layer. Real live orders allowed ONLY under explicit caps + a global kill switch + per-exchange/per-symbol live flags + credential availability + valid state, with the FINAL gate INSIDE order-executor (not only the engine). Safe by default: mode must be explicitly `live`; kill switch defaults engaged (1); every required cap must be set (any missing → denied); live_enabled flags default 0. Required caps (after the correction below): max open cycles / order notional / base qty / consecutive failures / unresolved reconcile. Executor `gatePlace`/`gateCancel` run `live.Guard.CheckPlace`/`CheckCancel` (an early pre-pacing pre-filter + a FINAL guard immediately before MarkInFlight — round 4) before each real PLACE/CANCEL (mode/AllowLiveExecution/not-dry-run/exchange+symbol live/caps/credentials/kill-switch/state); deny → request FAILED without sending + audited; allow → sent + audited. Kill switch is asymmetric: blocks new buy cycles + buy PLACEs, allows DB-PROVEN exit sell PLACE (inventory exit) + cancel + status. Engine `AllowNewBuyCycle` is the first check (kill switch + open-cycle cap). No-blind-resend preserved (ambiguous live PLACE → order/cycle NEEDS_RECONCILE, request DEAD). Live visibility is `live_audit` + `app_logs` + the startup safety summary (mode/kill-switch/caps/live scope/credential STATUS only — no key material); the `GET /api/live` view described in the original PR20 is NOT part of this lineage's dashboard (§18). **The guard fronts REAL sends:** the accepted parent already contains PR20a's real-client wiring + PR22 provisioning, so `live` builds real private clients from decrypted credentials and can perform real venue mutations once every guard passes; tests stay venue-free (rule #3) using simexec/fake clients, and no real order has run end-to-end yet (operational readiness, not missing wiring — see §16c "Current limitations"). Tests: gated live guard (allowed-baseline+audit, denies matrix [dry-run/kill-switch/not-configured/exchange-not-live/symbol-not-live/no-credentials/oversized-notional/oversized-qty], kill-switch-allows-proven-exit+cancel, cancel-needs-creds, AllowNewBuyCycle caps) + gated executor live-gate (allow→fills+audit, kill-switch→blocked+FAILED+deny-audit, no-credentials→refused, ambiguous→NEEDS_RECONCILE+DEAD-no-resend). **Correction (rebuilt on accepted PR19 `7ced7f5`; owner decisions + 8 fixes; verified on MariaDB 10.6): (1)** NO daily trading limits — max_daily_orders/max_daily_quote removed from Guard/Configured()/LoadControls/preflight caps+hash (columns remain, never read; test proves they no longer block); **(2)** SINGLE-INSTANCE design — no multi-instance cap reservation/locking added, deliberately; **(3)** comprehensive rate-limit detection ported from iranArb — per-venue signals (Nobitex 429 AND HTTP-200 `{"status":"failed","code":"TooManyRequests","backOff":sec}` envelope [definite pre-execution rejection], Bitpin Retry-After+DRF body regex, Wallex status+headers+conservative 200-body fallback, publics 429+`Retry-After`/`X-RateLimit-Remaining:0`+Reset) normalized into `RateLimitInfo{retry_after, source, definite_rejection, code}` on NormalizedAPIError; tight phrase fallback that cannot fire on "limit order"; **(4)** per-exchange reactive COOLDOWN — a throttle parks ONLY the affected exchange (claim loop skips it; an in-batch claimed request is Released to QUEUED unsent), extend-only deadlines (never shortened), venue wait preferred, per-exchange `retry_backoff_ms` else 60s fallback, 15m bound, mutex-guarded, poll-driven (no busy loop), safe logs (exchange/reason/source/until — no secrets); **(5)** rate limits never blindly retry mutations — venue-PROVEN pre-execution rejection re-queues the same persisted request after the cooldown (`RequeueProvenUnexecuted`, bounded, dead-letters when exhausted); anything else incl. HTTP-200 throttle bodies is AMBIGUOUS → never success, read-only recovery probe, lock HELD; **(6)** structured rate-limit metadata (#3's `RateLimitInfo`); **(7)** `rate_limit_per_sec` (proactive pacer before every send) + `retry_backoff_ms` (reactive fallback) WIRED from the live configstore cache via `Config.ExchangeTuningFor`; **(8)** guard hardening — live+nil-Guard DENIES (place and cancel); every safety query (`openCycles`/`unresolvedReconcile`/`consecutiveFailures`/`credentialsAvailable`/`live`) returns errors and ANY DB error denies; entry/exit separation with DB-PROVEN risk-reducing exits (`checkExitSell`: ownership, QUEUED state, payload==registered qty, dry_run=0 routing, filled inventory, no oversell/duplicate) exempt from kill-switch/entry caps; durable allow-audit BEFORE a real PLACE (audit failure ⇒ DENY) with the opposite conservative policy for cancels (audit outage never blocks risk reduction, logged instead). **Round 2 (6 blockers):** **(1)** a guard denial never strands a cycle — official state paths by risk (buy → FAILED+lock RELEASED; sell → NEEDS_RECONCILE+lock HELD; cancel → DEAD+NEEDS_RECONCILE+lock HELD), never a bare queue-row failure; **(2)** entry buys are PROVEN by one authoritative join (role/state/dry_run/cycle+exchange ownership/market-belongs-to-exchange/resolved-market-match/symbol-match/live flags) and `orderMarket` fails closed (market id 0 was a fallback that skipped the symbol-level check); **(3)** cancels prove the exact registered `exchange_order_id` + exchange/cycle ownership + a cancellable state (never trusting the payload id); **(4)** oversell counts `filled_quantity` from CANCELLED sells + the unfilled remainder of active sells (bought 1.0, cancelled-filled 0.4 → new 1.0 DENIED, 0.6 allowed); **(5)** cooldowns are DURABLE (migration 033 `exchange_cooldowns`, extend-only via GREATEST, reloaded before the first claim — a restart cannot resume sending to a throttled venue); **(6)** throttle headers on a SUCCESSFUL response park FUTURE requests via a transport sink while the completed operation keeps its result (a 200 throttle BODY remains not-a-success); plus pacing moved to the network boundary (after guard/audit, before MarkInFlight). **Round 3 (6 blockers):** **(1)** the doc's superseded "deferred to PR20a / no real client" text removed everywhere (§16c intro/split/what-remains, decisions log, PR row, and the binary's own comments) — the accepted parent HAS the real-client wiring, so the guard fronts real venue mutations; **(2)** cooldown persistence is ASYNC — arming is pure memory on the adapter's HTTP response path, durability is a separate bounded-retry worker, so a slow DB write can never delay a successful mutation's response into a caller timeout / false ambiguity; **(3)** persistence retry is tracked by `persistedUntil != until` (not by deadline-extension), so a failed write followed by equal/shorter signals is still retried; if durability stays broken past `CooldownPersistGrace` (30s default) live PLACES are disabled (cancels exempt) and re-enable automatically; **(4)** startup loads exchange tuning (+validates: live mode requires an `exchange_configs` row per wired exchange, rejects negatives) and durable cooldowns SYNCHRONOUSLY before the first claim, failure aborts Run — only the periodic refresh is async; **(5)** the SENT payload is proven equal to the persisted order for buys AND sells (quantity/price/order_type/TIF/client-id/side, derived from the actual `execution.OrderRequest` via `buildPlacePayload` so it cannot drift) and an exit must be routed to the market where the inventory was acquired; **(6)** pacing consumes exactly ONE slot per request (a duplicate GET_ORDER reservation was halving the budget) and `paceSend` returns an error so a cancellation during pacing skips MarkInFlight and every client call — a definitely-unsent request is never made ambiguous. Tests: async persistence (successful response never waits, park immediate, pending tracked, worker persists), failed-write retried when nothing extends + still durable after restart, durability grace policy (disables live places, not cancels; auto-recovers), startup (slow loader claims/sends nothing until done; load failure → zero mutations; cooldown-load failure aborts), payload mismatch matrices (buy: qty/price/client-id/type/side/zero-values/NULL-price/TIF; sell: price/qty/client-id/type/side/symbol/foreign-exchange market/not-the-acquired market; equivalent decimal forms still match), pacing (exactly one slot per GET_ORDER — mutation-verified to fail on a duplicate; cancellation → 0 place/cancel/get calls, never IN_FLIGHT). Tests: detection matrix (429+Retry-After, non-429, 200+code, 200+backOff, remaining=0+reset, malformed, fallback, no false positives on "limit", iranArb-copied nobitex 698s + bitpin 29s shapes), round-2 (13 entry-buy identity denials incl. DB error/missing row/wrong ownership/foreign market/disabled symbol+exchange/symbol mismatch/dry-run/bad role+state; 9 cancel-target denials incl. another order's id/foreign cycle+exchange/terminal/QUEUED/no registered id/DB error; oversell matrix incl. the reviewer's 0.4-cancelled cases + multi partial/active + NEEDS_RECONCILE-counts-as-active; denied buy leaves no stranded cycle + lock released; denied cancel → DEAD+NEEDS_RECONCILE; cooldown survives restart [persisted, reloaded, no call before deadline, B unaffected, resumes after] + SQL extend-only; per-adapter 200+remaining=0 keeps the order result AND parks, 200+throttle-body not success, "limit order" text not throttled, healthy quota parks nothing), cooldown (A-parks-not-B, nothing-reaches-A, resume, extend-only/no-shorten, deferred reads, safe observability), mutation safety (rate-limited place/cancel → probe not retry, 200-body not success, proven rejection requeued-then-retried exactly once, restart-no-duplicate, lock held), live guard (nil-Guard sends nothing, closed-DB denies place/cancel/new-cycle + every helper errors, daily caps no longer block, kill switch blocks buys, proven exit passes under kill switch + exhausted entry caps, oversell/duplicate/mismatch/ownership denied, audit outage blocks place but not cancel). **Round 4 (6 blockers):** **(1)** EVERY tuning reload validated before swap (`Cache.RunValidated`) — invalid/incomplete reload keeps the last known-good snapshot, and no redundant immediate 2nd load after the validated startup load; **(2)** early guard (pre-pacing; audits denials only) + FINAL guard immediately before MarkInFlight (re-checks all time-sensitive conditions at send time, writes the durable allow-audit, no-stranding disposition on deny); **(3)** exchange timeout (`sendContext`) created only at the network boundary — after pacing/final-guard/MarkInFlight — so pacing never consumes it and a pacing-delayed request is never made ambiguous; **(4)** per-exchange cooldown durability (`durabilityFailed[code]`): A's outage disables only A's entry buys, B unaffected, A auto-recovers; **(5)** graceful-shutdown bounded flush of pending cooldowns (hard-crash residual documented); **(6)** payload proof completed — mandatory exit-sell symbol, exact `time_in_force` NULL semantics, and the adapter-normalized client-order-id validated non-empty + persisted + proven by the final guard + sent unchanged. Durability policy blocks ENTRY BUYS only; proven exit sells and cancels stay available. Round-4 tests: configstore reload validation (missing-wired-exchange/negative rejected + last-good kept + no immediate reload), final-guard-after-pacing (kill-switch & cycle-state change during pacing → 0 PlaceOrder + no stranded cycle; both mutation-verified), timeout-after-pacing (short timeout + long pacing → fresh non-expired deadline, not ambiguous; mutation-verified), per-exchange durability (A denied / B allowed / A recovers), graceful-shutdown flush (armed→flush→restart restores; bounded when DB down), normalized client-order-id (normalizer changes id → final guard sees it + DB stores it + exact value sent; empty → no send), live payload matrix (empty sell symbol denied, TIF NULL semantics, sent-id vs persisted). **Round 5 (5 blockers):** **(1)** a denied buy releases the lock ONLY when `orders.OnBuyDenied` atomically proves zero exposure (order QUEUED + filled 0 + exchange_order_id NULL + cycle pre-send, read `FOR UPDATE`); otherwise it holds the lock + marks order/cycle NEEDS_RECONCILE; **(2)** the PR24 first-order checklist moved OUT of the final-guard→MarkInFlight window to after the send (`recordFirstOrderChecklist`) — nothing synchronous runs between the final guard and MarkInFlight; **(3)** pre-network failures are `execution.ErrNotSent` (temporary vs permanent) — the adapters (nobitex/wallex/bitpin) mark credential-load + invalid-symbol failures with ZERO network calls; the executor also checks `sendCtx.Err()` before the client call and classifies `IsNotSent` BEFORE any ambiguous handling (temporary → proven-unexecuted requeue; permanent → terminal entry/exit disposition; never a recovery probe); **(4)** `RequeueProvenUnexecuted` is now one transaction with `FOR UPDATE` + a `status='IN_FLIGHT'` guard + exactly-one-row `RowsAffected` + single retry increment + atomic exhaustion→DEAD+reconcile (a terminal request can never be resurrected; two concurrent handlers ⇒ one retry); **(5)** the PR-history statuses corrected — PR1–PR19 accepted (parent `7ced7f5`), PR20 in review, PR21+ planned. Round-5 tests: OnBuyDenied matrix (QUEUED-no-exposure→clean+lock-released; ACKED/SUBMITTED/PARTIALLY_FILLED/exchange-id/filled>0→NEEDS_RECONCILE+lock-held; mutation-verified), final-guard cycle-state-change during pacing → lock held + NEEDS_RECONCILE, checklist recorded only after the send, pre-network not-sent (temporary→requeue no-probe; permanent buy→clean-fail no-probe), expired send context → 0 adapter calls + requeue, per-adapter credential-failure + invalid-symbol → ErrNotSent + 0 network calls, RequeueProvenUnexecuted status guard (DEAD/FAILED/SUCCEEDED not resurrected; IN_FLIGHT→RETRY_SCHEDULED one increment; exhaustion→DEAD+reconcile; concurrent→one retry; mutation-verified). **Round 6 (5 blockers):** **(1)** pre-handler failures never strand — a transient `order.role` read error re-queues (`RequeueClaimedUnsent`, still CLAIMED), a malformed CANCEL payload → NEEDS_RECONCILE + lock HELD (never a bare queue-row FAILED); **(2)** ONE authoritative disposition (`orders.DisposeDeniedMutation`) for buy/sell/cancel denial + malformed + exhaustion — reads order+cycle `FOR UPDATE`, derives the cycle from `order.cycle_id` (never the queue's claimed cycle_id), proves request↔order↔cycle↔exchange, and can never fail/unlock an unrelated cycle B; **(3)** Bitpin auth/token failures (`bearerToken`: auth 429, token timeout, invalid response) wrapped `ErrNotSent` → order endpoint 0 calls, no probe, a 429 arms the cooldown; all private adapters classify pre-network failures; **(4)** retry exhaustion runs an executor `onExhaust` inside the requeue tx applying the operation disposition atomically — entry-buy zero-exposure → order/cycle FAILED + lock RELEASED, exit-sell/cancel → NEEDS_RECONCILE + lock HELD; **(5)** temporary local ErrNotSent uses bounded exponential backoff w/ jitter (from `retry_backoff_ms`), a proven rate limit uses the `cooldown_until` deadline, a permanent one is not retried, and `last_error`/logs preserve the real reason (never "rate-limited" for a credential blip). Round-6 tests: inconsistent-ownership (cycle B untouched + lock B not released; actual cycle A → NEEDS_RECONCILE + lock held; request DEAD; mutation-verified), cycle-derived-from-order, exhaustion-by-kind (buy release / sell+cancel+unknown hold; mutation-verified), malformed-cancel → NEEDS_RECONCILE + lock held, temporary order-role failure recoverable (RETRY_SCHEDULED, future retry), temporary-not-sent backoff (future retry_at, reason preserved, not labelled rate-limit), proven-rate-limit uses cooldown deadline, Bitpin auth-429 not-sent + 0 order calls + rate-limit preserved, Bitpin token-timeout not-sent + 0 order calls, per-adapter credential/invalid-symbol not-sent + 0 network. **Round 7 (4 blockers):** **(1)** mutating requests require `cycle_id` AND `order_id` — `Queue.Enqueue`/`EnqueueScheduled` reject (`ErrMalformedMutation`), `Claim` refuses malformed rows, and existing malformed rows are finalized DEAD + cycle NEEDS_RECONCILE + lock HELD (`sweepMalformedMutations` + `orders.DisposeMalformedMutation`, one finalizer via `FOR UPDATE SKIP LOCKED`); **(2)** `MarkInFlight` at the REAL network boundary via `exchanges.MutationPreparer` (Bitpin `PreparePlace`/`PrepareCancel` do token/auth before MarkInFlight; `prepared.Send` is the only order call) — a crash during token prep leaves the row CLAIMED (not "maybe sent"), and prepare/send are paced as two slots; **(3)** Bitpin `bearerToken` STRICT for mutations — an auth throttle window / fresh 429 / 200-with-`X-RateLimit-Remaining:0` fails preparation as a definitely-not-sent rate limit (order endpoint never called, cooldown armed, retry after cooldown); **(4)** every stale mutating IN_FLIGHT reaches a terminal decision — probe, or `finalizeStaleConservative` (DEAD + cycle NEEDS_RECONCILE + lock held), with temporary `orderRecoveryInfo` failures bounded by the recovery hard limit — none sit IN_FLIGHT forever. Also: the lookup-by-client-id doc now matches the code (all three private venues, not "Wallex only"). Round-7 tests: Enqueue/EnqueueScheduled reject missing order/cycle; Claim refuses malformed (mutation-verified); malformed-sweep + stale-null-order/null-cycle → DEAD+reconcile+lock; finalizeStaleConservative; staleRecoveryExpired bound; Bitpin auth 429/200-remaining:0 → not-sent + 0 order calls (mutation-verified) + cancel; preparer failure leaves not-IN_FLIGHT + no probe; preparer paces two slots. **Round 8 (4 blockers):** **(1)** the two-stage `MutationPreparer` boundary is implemented for ALL THREE private adapters — Nobitex `PreparePlace`/`PrepareCancel` (in-memory API-key + multipart body + request built during prep), Wallex likewise (X-API-Key + JSON body + request), Bitpin as before — so a crash during ANY adapter's preparation leaves the row CLAIMED, never a false IN_FLIGHT; `PlaceOrder`/`CancelOrder` are thin `Prepare().Send()` wrappers; **(2)** Bitpin now builds the FINAL `http.Request` inside `PreparePlace`/`PrepareCancel` (fallible `http.NewRequestWithContext` moved before MarkInFlight); `bitpinPrepared.Send` only binds the send context (`req.WithContext`) and calls `http.Do` via the shared `doPreparedRequest` — no token/credential/symbol/payload/request construction after MarkInFlight (the type holds only `req`+`decode`); **(3)** pacing counts actual HTTP calls — the executor stopped pacing before prepare and instead threads a `WithNetworkPacer` hook that the adapter invokes ONLY when it makes an auth/refresh call; fresh cached-token / in-memory-credential mutations consume ONE slot (order only), auth-or-refresh + order consumes TWO; Bitpin auth rate limits still stop the mutation endpoint; **(4)** stale recovery proves request/order/cycle/exchange ownership — `orderRecoveryInfo` loads the order's OWN cycle_id/exchange_id; a mismatch, or a NULL claimed cycle_id with a valid order_id, is resolved by `orders.DisposeDeniedMutation` (Kind=Unknown, conservative): cycle derived from the order, request DEAD, ACTUAL order+cycle NEEDS_RECONCILE, lock HELD, unrelated cycle/lock untouched, NO probe from mixed ownership. Round-8 tests: Nobitex/Wallex prepare makes 0 HTTP + Send makes 1 (place & cancel); prepare credential failure → NotSent + 0 HTTP (both adapters, place & cancel); Bitpin prepared request pre-built before Send (method/URL/auth-header/body present, 0 order hits during prep); Bitpin adapter paces auth only when networked (fresh=1, cached=0; place & cancel); cancelled auth pacing → NotSent + 0 calls; executor end-to-end pacing (expired token=2 slots + auth=1, cached=1 slot + auth=0); two-stage credential failure never IN_FLIGHT + 0 HTTP + 0 probe (Nobitex/Wallex, place & cancel); stale ownership mismatch (order cycle A / claim cycle B) → no probe, B+lockB unchanged, order+cycle A NEEDS_RECONCILE + lock A held, request DEAD (place & cancel; mutation-verified); stale NULL-cycle valid-order → cycle derived from order, DEAD + reconcile + lock held (place & cancel); consistent stale still probes. **Round 9 (4 production-path blockers):** **(1)** valid `order_id` + NULL `cycle_id` is handled in the REAL sweep flow: `recoverStaleMutating` now JOINs the order (so a NULL claimed cycle no longer hides the row from the `EXISTS(cycles WHERE id=er.cycle_id)` filter) and `sweepMalformedMutations` loads `order_id` and derives the cycle FROM the order — request DEAD, ACTUAL order+cycle NEEDS_RECONCILE, lock HELD (never finalizing only the queue row); **(2)** stale candidate discovery no longer depends on untrusted `er.exchange_id`/`er.cycle_id` — the order-JOIN + mode scope find rows with an unwired/foreign claimed exchange (can't stay IN_FLIGHT forever) or a cross-mode claimed cycle, and route each to the executor that owns the ORDER's cycle mode; **(3)** `sweepMalformedMutations` is scoped by AUTHORITATIVE execution mode — a live executor finalizes only live-cycle rows, a dry-run executor only dry-run-cycle rows (mode taken from the order's cycle when present, else the claimed cycle), a no-ownership row is DEAD-only; live and dry-run can never mutate each other's cycles/locks (`recoverySkip` for `off`; `DenialParams.BroadTerminal` DEADs still-QUEUED rows); **(4)** `bitpinAuthRateLimited` carries the real `RateLimitInfo.RetryAfter` (parsed 429 / remaining `authThrottledUntil` / `X-RateLimit-Reset`), so the queue retry waits the venue deadline not the 1s fallback, and an active window returns its remaining time without re-authenticating. Round-9 tests: Bitpin auth fresh-429/active-window/200-quota carry RetryAfter (adapter) + executor retry_at ≈ 30s not 1s; prod sweep NULL-cycle-valid-order → DEAD+reconcile+lock (live & dry_run × place & cancel); stale unwired claimed exchange still recovered (place & cancel); cross-mode claimed cycle isolation (dry-run ignores live order; live finalizes; B unchanged — mutation-verified mode scope); malformed sweep dry-run-skips-live / live-skips-dry-run / valid-order-live-claims-dry-run.**Round 10 (3 production blockers):** **(1)** the malformed-sweep execution-mode filter moved INSIDE the SQL, BEFORE `LIMIT`, with `ORDER BY er.id` — mode derived from the order's cycle when the order exists, else the claimed cycle, else mode-independent request-only — so n other-mode rows can never fill the LIMIT window and starve this executor's rows (mutation-verified: dropping the SQL clause fails the starvation test); **(2)** stale discovery RETAINS the authoritative `o.cycle_id`/`o.exchange_id`/cycle-mode on each candidate; a persistent `orderRecoveryInfo` failure past the recovery window is finalized by `finalizeStaleAuthoritative` on those RETAINED values (request DEAD, ACTUAL order+cycle NEEDS_RECONCILE, lock HELD, claimed cycle UNTOUCHED — mutation-verified: switching to the claimed cycle fails the test); ErrNoRows reconciles the retained actual cycle; **(3)** no unclaimable probe — `usableRecoveryClient` (wired client AND `exchanges.enabled=1`, fail-closed on DB error) is checked before any GET_ORDER probe; without it the row is finalized conservatively, so no probe can sit QUEUED forever behind a missing client, missing credential, or disabled exchange. Round-10 tests: 50 dry-run rows + 1 live row → one live sweep (limit 50) processes the live row, fillers untouched (+ inverse direction); persistent-orderInfo-failure expired → DEAD + ACTUAL order/cycle A reconciled + lock A held + claimed cycle/lock B unchanged (place & cancel) + cross-mode variant (dry-run executor never sees the live order's row); no-recovery-client (place & cancel) and disabled-exchange → 0 probes + DEAD + reconcile + lock held. **Round 11 (1 production blocker):** already-QUEUED `GET_ORDER` recovery probes that BECOME unclaimable after creation (exchange disabled / credential removed / client not re-constructed across a restart) are finalized by `sweepUnclaimableRecoveryProbes` — run at STARTUP and PERIODICALLY — on the AUTHORITATIVE order + cycle (probe DEAD, order+cycle NEEDS_RECONCILE, lock HELD), mode-scoped inside SQL before `LIMIT`, order-authoritative (never the probe's claimed cycle_id/exchange_id), idempotent (`FOR UPDATE SKIP LOCKED` + re-verify unusable); `recoverAmbiguousPlace`/`recoverAmbiguousCancel` also re-check `usableRecoveryClientByCode` before scheduling a probe and reconcile instead when the path was lost. Round-11 tests: queued probe + exchange later disabled → DEAD + reconcile + lock held (place-probe & cancel-probe); queued probe + restart WITHOUT the client → same; usable-exchange probe left QUEUED; ambiguous place & ambiguous cancel losing recovery capability → 0 probes + DEAD + reconcile + lock held (mutation-verified both); cross-mode isolation (dry-run executor never touches a live order's probe; live finalizes; claimed dry-run cycle B unchanged); and (round-10 strengthening) disabled-exchange stale recovery now covers CANCEL_ORDER, and a valid-order/claims-unrelated-cycle row behind 50 fillers is reached and resolved on the ORDER's cycle under the production limit. |
| PR20a | `pr20a-credential-decryption` | **accepted** | `internal/secrets` + `internal/credentials` + executor/balance-sync/health/reconciler/dashboard wiring: real credential decryption + real private-client wiring, gated by the unchanged PR20 guard. `secrets`: AES-256-GCM, stored `nonce||ciphertext||tag`, AES key = SHA-256(master key); only AES-256-GCM supported; Encrypt/Decrypt symmetric; empty master key → ErrNoMasterKey (safe-disable); decrypt failure → ErrDecrypt (no plaintext). `credentials.Provider` (an `exchanges.CredentialProvider`): selects the single enabled+active, highest-key_version credential, decrypts api_key/secret/passphrase IN MEMORY; disabled/non-active/old-version ignored; unsupported-algo/decrypt-failure → mark row status='error' (non-secret note) + error with no plaintext; never writes back/logs/returns plaintext. `credentials.Builder.BuildPrivate` builds via the FACTORY (`exchanges.NewPrivateClient`) injecting the Provider as Creds + DB symbol map; active-credential-only; unsupported exchange → no client; no per-exchange hardcoding; no network at construction. `Provider.Validate` = read-only balance check ONLY (BalanceReader interface; never place/cancel), stamps active/invalid. Executor (live) builds real clients for live-enabled+active-credential exchanges, AllowLiveExecution=true, guard unchanged; no/invalid master key → no clients, nothing sent. balance-sync/health-private-probe/reconciler build credentialed clients held through narrowed non-mutating interfaces (BalanceClient/BalanceReader/ReadOnlyClient). Dashboard `GET /api/credentials` (+ /api/live block): STATUS ONLY (exchange/label/status/enabled/key_version/algorithm/last_checked/non-secret-note) — never key material or blob. Master key is config-file only (no runtime env). Tests: offline crypto (roundtrip, wrong-key→ErrDecrypt-no-leak, missing-key, truncated/corrupt, algorithm guard) + narrowed-interface compile+reflection guards (no Place/Cancel) + gated credentials (decrypt-valid, wrong-master-key-marks-error, missing-key-disables, unsupported-algo-marks-error, disabled-ignored, active-over-non-active, highest-key_version-selected, factory-injects-decrypted-creds, build-refuses-without-credential, validate-is-read-only-never-place/cancel) + gated dashboard `/api/credentials` (status-only, no secret fields, blob bytes absent). No real network in any test; no PlaceOrder/CancelOrder during validation. Remaining: provisioning/rotation UI + encrypt-and-insert CLI. |
| PR21 | `pr21-operator-reconcile-v2` | **in review (rebuilt on accepted PR20 `c85e0e2`)** | `internal/opreconcile` (operator resolution for NEEDS_RECONCILE) + `internal/state` resolution/terminal-correction + `internal/dashboard/reconcile.go` (authenticated API) + migration 034. A LOCAL tool: the `Resolver` holds only a DB handle (reflection-guarded — no PlaceOrder/CancelOrder/GetOrder), so it can NEVER touch a venue; every change goes through `internal/state`; a lock is released only on proven-zero exposure. Two-phase: Preview (read-only, returns the exact changes + a `state_hash`) then Apply (one tx: lock cycle+orders+active-requests+symbol-lock FOR UPDATE → re-validate → verify fingerprint → mutate via the state machine → record fill → release lock if proven → write audit). **10 safety corrections over the v1 that shipped in the PR20 base:** **(1)** exposure is classified PROVEN_ZERO/OPEN/UNKNOWN/INCONSISTENT (a recorded `filled_quantity=0` is NOT proof — an order that reached or may have reached the venue is UNKNOWN); lock release / cancel_zero_exposure / mark_buy_zero_filled require PROVEN_ZERO, refuse OPEN, and refuse UNKNOWN unless `external_resolution_confirmed`+reason. **(2)** all active related exchange requests (mutating AND GET_ORDER, matched by cycle_id OR the cycle's order ids — order-authoritative) are detected + FOR UPDATE-locked; resolving out of NEEDS_RECONCILE is refused while any can still execute. **(3)** order role/side validated from the persisted order (a buy action can't touch a sell; another cycle's order rejected) — enforced in targetOrder AND validateFill. **(4)** order state and cycle state computed SEPARATELY from cumulative fills — mark_buy_filled requires the fill to complete the order (else mark_buy_partially_filled), mark_sell_partially_filled refuses a fill that closes the whole exposure (else mark_sell_filled); a sell can fully fill while the cycle holds inventory and the cycle can close while its sell is only partial. **(5)** a fill on a TERMINAL order is refused; `correct_terminal_order_fill` → `state.CorrectTerminalOrder` (version-guarded, audited terminal→PARTIALLY_FILLED/FILLED) records a discovered fill, preview and apply agree. **(6)** apply REQUIRES a matching preview: a single-use, operator-bound, TTL preview token carries the `state_hash`; apply 409s on any state/payload drift (recomputed inside the locked tx), 400 without a token. **(7)** the preview shows EVERY mutation: order_changes[], request_changes[], fill_to_insert, accounting_changes, lock_change, exposure_before/after, exposure_classification, execution_mode. **(8)** attach_exchange_order_id is idempotent (same value) / conflict-checked (different existing) / uniqueness-checked (same id on another order) with a version CAS + RowsAffected. **(9)** cycle + all orders + all active requests + active lock locked in ONE tx; fills/exposure computed only from locked rows. **(10)** a post-change audit-read failure rolls the whole tx back (state + audit atomic; `faultBeforeCommit` proves it). **Auth (current session model, migration 034):** every endpoint (incl. read-only list/detail/audit) gated by `requireReconcileCapable` = EXACT-set {reconcile_operator, admin}; `reconcile_operator` is off the config `roleRank` ladder so it can resolve but NOT edit config; operator from session, never body; live/dry-run visible via cycle `dry_run`. Endpoints: GET /api/reconcile (list), /api/reconcile/{id} (full context, every sub-query error → 500 not partial-200), /api/reconcile/audit; POST …/preview + …/apply. Actions: cancel_zero_exposure, attach_exchange_order_id, mark_buy_partially_filled, mark_buy_filled, mark_buy_zero_filled, mark_sell_partially_filled, mark_sell_filled, mark_order_cancelled_zero_fill, correct_terminal_order_fill, keep_needs_reconcile, mark_failed (open/unknown exposure refused unless external-confirmed). Audit `reconcile_resolutions` (+ `exposure_classification`, migration 034). Migration 034 (role CHECK + audit column). Tests (venue-free / gated MariaDB 10.6): opreconcile — exposure UNKNOWN/PROVEN_ZERO/OPEN/INCONSISTENT, cancel/buy-zero refused-for-unknown + allowed-with-external, buy partial/complete classification, mark_buy_filled-rejects-partial, sell-partial-closing-rejected, sell-closes-cycle-order-only-partial, role gates (buy-on-sell, sell-on-buy, another-cycle, attach-wrong-role), active-request-blocks-close (QUEUED/CLAIMED/IN_FLIGHT/RETRY_SCHEDULED × mutating+GET_ORDER), request-owned-via-order-not-claimed-cycle, terminal-fill-refused + correct_terminal_order_fill (CANCELLED/FAILED, preview==apply), attach empty/idempotent/conflict/dup, apply-without-preview, stale-fingerprint-409, concurrent-fill-409, crash-rolls-back, oversell/dup-fill/invalid-qty, keep/mark_failed matrices — 4 major guards mutation-verified (exposure, active-request, role, preview-fingerprint); dashboard — 401 without session on every endpoint, 403 for viewer/config_operator, reconcile_operator-cannot-edit-config, preview→apply happy path + operator audited, apply-without-token 400, wrong-operator-token 403, state-drift 409, detail-subquery-failure 500, list-shows-execution-mode. |
| PR22 | `pr22-credential-provisioning-v2` | **in review (rebuilt on accepted PR21 `616e3b0`)** | `internal/credentials.Provisioner` (create/validate/activate-or-rotate/disable) + `Provider.CredentialsByID` + `Builder.BuildForCredential` + `internal/dashboard` credential endpoints + migration 036. Safe workflow: create INACTIVE -> validate the EXACT credential (read-only) -> activate (atomically disabling the old one) -> optionally disable. **Five safety corrections over the v1 in the base:** **(1)** `Create` ALWAYS writes enabled=0/status='unknown'/service-assigned key_version — it never accepts enabled/status/key_version from the body, so a credential can never be created active (mutation-verified). **(2)** validation targets the EXACT credential_id: `BuildForCredential` decrypts only that row (`CredentialsByID`) and does a read-only `GetBalances` (the `BalanceReader` interface exposes no PlaceOrder/CancelOrder); success->'valid', DEFINITE auth rejection->'invalid', transient (timeout/network/rate-limit/5xx)->status UNCHANGED (mutation-verified); status+audit in one tx. **(3)** `ActivateOrRotate` locks the exchange row FOR UPDATE (per-exchange serialization), confirms the credential is validated, disables other active creds + activates the selected one in ONE tx; a DB active-guard (STORED generated column = exchange_id when enabled=1 AND status='active', UNIQUE) makes two-active structurally impossible; concurrent rotations leave exactly one active (tested); on any failure the old credential stays active (pre-commit fault test: state+audit roll back together). **(4)** `Disable` refuses to disable the LAST usable (enabled + active/valid) credential while the exchange has open LIVE risk — active live session / non-terminal live cycle / non-terminal or NEEDS_RECONCILE live order / active live request — never removing the ability to sell/cancel/recover; narrow (terminal history, dry-run, inactive locks never block; allowed once another usable cred exists or risk clears; mutation-verified). **(5)** every credential change writes its audit row in the SAME tx; audit/API never contain plaintext, encrypted blobs, or the master key. Encryption unchanged (PR20a `secrets.Cipher` AES-256-GCM); master key from config only, absent/invalid -> credential WRITE endpoints 503 (no plaintext fallback), live loading fails closed. Dashboard: current session/cookie model, exact-set `requireCredentialCapable` {credential_operator, admin} on EVERY endpoint (401/403), operator from session; `credential_operator` off the config ladder. Migration 036 (status enum +'valid', active-guard UNIQUE, dashboard role +'credential_operator'). Tests (venue-free / gated MariaDB 10.6): created-inactive+unusable+service-key-version; CredentialsByID-exact-row; validate read-only marks valid; definite-auth->invalid; transient->unchanged; activate/rotate atomic + refuses-unvalidated; concurrent-rotations-one-active; rotation-rolls-back-state+audit; disable-last-under-open-risk-refused + allowed-when-another-usable/risk-cleared; no-plaintext-stored-or-audited; dashboard 401/403 on every endpoint + writes-disabled-503-without-master-key + create-inactive-no-secret + list-no-secret-columns. **Round 2 (3 blockers):** **(1)** `ValidateCredential` now applies its result under a FOR UPDATE re-read of the CURRENT status (the network read is unlocked), so a concurrent activate/rotate is never overwritten from a stale snapshot — a successful re-validation of an ACTIVE credential keeps it ACTIVE (never downgraded to 'valid'), transient errors preserve state exactly, and a disabled/rotated credential is left as-is (mutation-verified). **(2a)** the Disable open-risk guard counts only `enabled+status='active'` as operational — a 'valid' credential is NOT a live replacement, so the last active credential can't be disabled under risk with only a validated-but-inactive spare (mutation-verified); **(2b)** the active-request open-risk check is order-authoritative — when a request has an order_id its exchange/mode come from the persisted order's cycle, not the request row's (wrong/missing) exchange_id/cycle_id, so a bad queue row can't hide an active request on a real live order (mutation-verified, isolated via terminal cycle+order). **(3)** the Bitpin token cache is bound to a non-secret credential fingerprint; before reusing a cached token the client resolves the active credential and, on a fingerprint change (rotation), drops the token/refresh/throttle and re-authenticates with the new credential (mutation-verified). All four round-2 guards mutation-verified. **Round 3 (2 blockers):** **(1)** `ValidateCredential` now holds the credential + exchange rows `FOR UPDATE` (same lock order as `ActivateOrRotate`) for the DURATION of the read-only network check, fully serializing validation against activation/rotation — so even a DEFINITE-auth-failure validation can no longer overwrite a concurrently-activated credential and leave zero active (mutation-verified: removing the lock deadlocks/leaves zero active); **(2)** Bitpin rotation is enforced at two more points: after an auth response returns, the client re-resolves the active credential and refuses to cache a token minted for a since-rotated credential (2A), and `bitpinPrepared.Send` re-checks the active-credential fingerprint immediately before transmitting and returns `execution.IsNotSent` if it changed since preparation — a prepared place/cancel is never sent through the rotated-in credential and is re-prepared (2B). Round-3 tests: validation-serialized-against-concurrent-activation (deterministic; A stays active, B invalid, one active); bitpin prepared-place + prepared-cancel refused-after-rotation (order/cancel endpoint 0 calls, NotSent, re-prepare uses B); bitpin auth-response-rejected-after-rotation (token A never cached, next auth uses B, no wallets call with token A). All three round-3 guards mutation-verified. |
| PR23 | `pr23-live-preflight` | **planned (rebuilds onto accepted PR20)** | `internal/preflight` + `internal/live` (canary ack gate) + dashboard endpoints + migration 023 (`live_controls` freshness/canary cols + `live_acknowledgements`): strict read-only live readiness checklist + an explicit operator acknowledgement the guard enforces, so live trading can't start accidentally even with creds/caps/controls. `preflight.Checker` (DB handle only — reflection guard: no place/cancel/balance/order; no exchange import; mutates nothing): `Run` produces a Report (per-check pass/fail/warn, failures/warnings, ready, config_hash). Checks: execution-mode-live, kill-switch known+disengaged, exchange+symbol live-enabled, caps configured+sane (+canary max_open_cycles=1), credential exists/enabled/active/validated + validation-fresh (credential_validation_max_age_minutes), private-health ok (WARN accepted when health_required=0), balance recent, market-data fresh (recent comparison_event ⇒ Binance+Iranian fresh), reconcile within cap, no stuck IN_FLIGHT mutating, no stale lock, no DEAD mutating on real cycles, recent dry-run CLOSED for the exchange/symbol, auth path (enabled token), audit path (live_audit). `ConfigHash` covers config-relevant inputs only (caps/live-flags/canary/credential-identity/freshness/mode/ack-req — excludes market freshness/balances/kill-switch). `POST /api/live/acknowledge` (admin) re-runs preflight, refuses unless ready (409), records `live_acknowledgements` bound to the config hash (operator/exchange/symbol/credential/caps/reason), deactivating any prior. Guard (require_canary_ack default 1): a live BUY must be within the canary exchange/symbol scope AND covered by an active ack whose preflight_hash == current ConfigHash AND not expired (canary_ack_max_age_minutes) AND pass a dynamic re-check (credential/market/balance freshness, reconcile cap, stuck IN_FLIGHT, dangerous queue) — missing/out-of-scope/stale/expired/dynamic-fail → deny; sells+cancels unaffected. `GET /api/live/preflight` + `GET /api/live/acknowledgements` (with an `expired` flag) read-only. No exchange mutation; preflight places/cancels nothing. PR20 guard remains mandatory. Tests: offline (checker-holds-no-exchange-client) + gated preflight (passes-when-ready, fails on credential-missing/validation-stale/kill-switch/caps-missing/market-stale/balance-stale/reconcile-over-cap/stuck-inflight/no-recent-dry-run, does-not-mutate, ack-records-operator+hash, ack-refused-when-not-ready, config-change-invalidates-ack) + gated live (canary-ack-required+stale-after-config-change, canary-scope-restricts-to-one-market, ack-required-but-scope-unset) + gated dashboard (preflight-read-only, acknowledge-requires-admin [401/403 viewer+config+credential+reconcile, 200 admin records operator+hash], not-ready→409). No real network in any test. Correction: a config-only hash is not the sole gate — the live-BUY guard also enforces ack EXPIRY (canary_ack_max_age_minutes) + a dynamic re-check (preflight.DynamicRecheck) before each buy; tests: expired-ack-denies, stale-credential/market/balance-after-ack-denies, new-reconcile/stuck-inflight-after-ack-denies, kill-switch-reengaged-denies, sell/cancel-unaffected. |
| PR24 | `pr24-canary-session` | **planned (rebuilds onto accepted PR20)** | `internal/live` (run sessions) + executor + dashboard endpoints + migration 024 (`live_run_sessions` + `live_audit` correlation cols): first real canary live-run instrumentation — makes the first order observable, correlatable, and stoppable WITHOUT broadening scope (still one exchange/symbol/cycle/tiny notional, gated by the PR23 ack). `live_run_sessions` records operator/exchange/symbol/credential/preflight-hash/ack-id/caps/status/start+stop reasons/first-order-checklist. `StartSession`: verifies canary scope + a current (hash-matching, non-expired) acknowledgement + dynamic readiness + no active session, then inserts ACTIVE; contacts no exchange. The guard's live-BUY path now ALSO requires an ACTIVE session (added to canaryAckOK after ack/expiry/dynamic), so `StopSession` blocks new buys immediately while sell/cancel/status stay allowed (per-run audited complement to the kill switch). `POST /api/live/session/start|stop` require admin (start re-runs preflight → 409 if not ready; 400 on scope/ack/readiness; 409 if already active); `GET /api/live/session` read-only shows session + order count/quote used/open cycles/last order/last deny/kill switch/mode/ack status. Every live_audit row tagged with live_session_id + acknowledgement_id + preflight_hash. First real buy of a session writes a one-time first-order checklist (mode/exchange/symbol/caps-remaining/credential-status/ack/session/kill-switch/request+order+cycle ids) — no secrets, idempotent. Tests: gated live (start-requires-valid-ack, out-of-scope-rejected, failed-readiness-rejected, succeeds+recorded+single, stop-blocks-buys+allows-sell/cancel+audited, stop-without-active, no-active-session-denies-buy, live_audit-includes-session+ack+hash, first-order-checklist-written+no-secrets+idempotent) + gated dashboard (start requires admin [401/403], requires ack [400], failed-preflight [409], start→view-ACTIVE→stop→no-active + second-stop 409). No real network in any test; scope stays single-canary. |
| PR25 | `pr25-canary-runbook` | **planned (rebuilds onto accepted PR20)** | `RUNBOOK.md` + `internal/live` (startup summary, dry-run gate) + `internal/preflight` (RecentDryRunOK) + cmd (startup log) + dashboard (warnings + audit export) + the asInt fix: real-canary execution runbook & production hardening, no scope change (still one exchange/symbol/cycle/tiny notional, gated by preflight+ack+session). RUNBOOK.md: provision→validate→configure caps/scope→dry-run→preflight→acknowledge→disengage kill switch→start session→watch first order→stop→kill switch→inspect/export audit→resolve NEEDS_RECONCILE, + an emergency-stop-by-cycle-state table. `live.BuildSafetySummary` (read-only, no secrets): execution mode/live-enabled/kill-switch/canary exchange+symbol/caps-configured/credential STATUS/active-session/new-live-buys-allowed (full guard verdict) — logged at startup by order-executor + trade-engine. `StartSession` now also requires a recent successful dry-run (preflight.RecentDryRunOK) for the exchange/symbol. `GET /api/live/warnings` (read-only, severity-tagged): live-mode-enabled, kill-switch-disengaged, session-active, first-order-pending/sent, unresolved-reconcile, balance/market/credential-validation stale. Emergency stop (session stop or kill switch) blocks new buys immediately + keeps sell/cancel/status + recalls nothing already sent (documented + tested per cycle state). `GET /api/live/session/export` (read-only, no secrets): session + preflight hash + acknowledgement + caps + requests + orders + allow decisions + denials + first-order checklist + stop reason (via PR24 live_audit correlation). Fixed asInt to parse driver []byte/float64 ids so session counts + export correlation work. Tests: gated live (startup-summary-no-secrets + off-mode, emergency-stop-matrix [stop blocks buys/keeps sell+cancel; kill switch same], start-requires-recent-dry-run) + gated dashboard (warnings appear in live-danger states incl. stale balance/market, audit export includes session/caps/checklist/decisions + no secrets). No real network in any test; scope stays single-canary. |
| PR26 | `pr26-predeploy-audit` | **planned (rebuilds onto accepted PR20)** | `scripts/check-critical-invariants.sh` + `internal/audit/invariants_test.go` + two hardening fixes + focused tests: a read-only critical pre-deploy audit (NO deploy, NO live order, NO API key, NO scope change). Audited all safety layers — runtime-config boundary, exchange-mutation boundary, DB-commit-before-send, queue claim SQL (OR/AND precedence correct + type/enabled/concurrency filters), mutating-retry safety, state-machine enforcement, symbol-lock safety, buy/sell lifecycles, simulated-IOC classification, decimal/precision, live-guard deny matrix, credential/secret masking, dashboard authz, operator reconciliation, audit correlation, crash/restart — partly via independent read-only sub-audits of the riskiest areas. **No critical bug found; every invariant HOLDS.** Static checks (CI + `go test`): fail on runtime V3_* env, PlaceOrder/CancelOrder outside executor/adapters, direct UPDATE cycles|orders SET state outside internal/state, dashboard encrypted-blob reference, secret-named log field, read-only service main holding PrivateClient — all pass. Hardening fixes (defense-in-depth, not bugs): (1) `queue.ScheduleRetry` dead-letters a mutating request (+ order NEEDS_RECONCILE) instead of ever rescheduling — guards a future caller from blind re-send; (2) `exir` order books parse via json.Number→decimal.NewFromString (exact, no float round-trip). Tests added: audit static invariants (6), queue mutating-retry guard, exchanges mask-covers-every-adapter-secret-field (incl secret_key), dashboard asInt driver-type parsing (the PR25 []byte/float64 id bug). Remaining risks documented: a few crash-recovery scenarios are venue/fault-injection-only (conservatively handled by sweeper→DEAD+reconcile). Verification: static script exit 0; offline `go test ./...` ok; full gated `-p 1` green; build/vet/gofmt clean; go.mod unchanged. Correction: added six venue-free crash/rollback fault-injection tests (TEST-ONLY executor.faultAfterSend + opreconcile.faultBeforeCommit seams + a send-counting fake client): commit-before-send recoverable+no-dup, MarkInFlight-crash, place-completion-rollback, cancel-completion-rollback, reprice-cancel-in-flight-crash, reconcile-apply-crash — all conservative (DEAD + NEEDS_RECONCILE, never re-sent, lock held). No live order sent; no real API key used. |
| PR27 | `pr27-deploy-packaging` | **planned (rebuilds onto accepted PR20)** | `Dockerfile` + `docker-compose.yml` + `configs/production.example.toml` + `DEPLOY.md` + `scripts/local-dryrun-check.sh` + safe-default tests: local/staging deployment packaging with NO live trading and NO real credentials (no PlaceOrder/CancelOrder, no API key). Dockerfile builds all 9 binaries (collector/trade-engine/order-executor/reconciler/balance-sync/health-monitor/dashboard/retention-worker/migrate) into one small distroless non-root image — no config/secret baked in. docker-compose: MariaDB 10.6 + Redis 7 (health-checked) + a one-shot migrate + the 8 services, each waiting on migrate via service_completed_successfully and mounting configs/config.toml read-only; dashboard on :8080. production.example.toml: placeholders ONLY — no API key, no plaintext credential, master_key empty, [execution] mode=off (safe default; off/dry_run for local/staging). DEPLOY.md: startup order (DB -> Redis -> migrate -> services -> verify dashboard/market-data/dry-run; services fail fast on pending migrations) + a full dry-run procedure (simulated clients, zero exposure). Tests: offline production-example-is-safe-and-secret-free (mode != live, master_key empty, DSN redacted, no api_key/secret in file) + gated services-refuse-pending-migrations (EnsureCurrent) + kill-switch-defaults-engaged. Existing PR19-PR26 suites cover no-real-mutating-in-dry-run, live-disabled-without-credentials, dashboard-no-secrets. Verification: all 9 binaries build, `docker compose config` valid, scripts bash-clean, static invariant script PASS, offline `go test ./...` ok, full gated `-p 1` green, build/vet/gofmt clean, go.mod unchanged. No live order; no real API key.  |
