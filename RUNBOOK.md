# v3TradeBot — First Real Canary Live Run (Operator Runbook)

> **Scope is intentionally tiny.** A live run is **one exchange, one symbol, one open cycle,
> a tiny notional**, and only proceeds when preflight passes, an acknowledgement is active
> and not expired, and a canary session is started. Nothing here broadens that scope.
>
> All endpoints below are JSON over the dashboard binary. Since PR17 the dashboard requires
> a **login session** (`POST /login` → HttpOnly session cookie); every route except
> `/healthz` and `/login` needs it, and config edits need the `config_operator`/`admin`
> role. Secrets are never shown or logged anywhere in this flow.
>
> **Network exposure (important).** A login is **not** a substitute for network isolation.
> Keep the dashboard bound to `127.0.0.1` (the shipped default) and reach it only over a
> trusted VPN or an authenticated reverse proxy; never publish port `8080` to an untrusted
> network. When served over HTTPS, set `[dashboard] secure_cookies = true`. See DEPLOY.md §3a.

## 0. Pre-requisites (one-time)

- Real **master key** set in the bootstrap config file (`[security] master_key`) — never an
  env var. With no/invalid master key, credential loading + live execution are safe-disabled.
- Migrations applied (`cmd/migrate`); every binary fails fast if a migration is pending.
- **An `exchange_configs` row for every live-enabled exchange.** In `live` mode the
  order-executor loads and validates per-exchange tuning SYNCHRONOUSLY at startup and
  **refuses to start** if a wired exchange has no row (or has negative values) — trading real
  money with uninitialized `rate_limit_per_sec` / `retry_backoff_ms` is not a safe default.
  Symptom: `order-executor: startup load (exchange tuning): exchange "x" has no
  exchange_configs row`. Fix: insert/repair the row, then restart.
- Dashboard users provisioned (PR17): bootstrap the first admin with
  `./bin/dashboard -config <cfg> -create-user admin:admin` — the **password is entered at a
  hidden prompt (or piped via stdin), never on the command line** — then log in via
  `POST /login`. Roles: `viewer` | `config_operator` | `admin` (config edits need
  config_operator+; high-risk flag changes need admin).

## 1. Provision a credential  *(role: credential_operator/admin)*

```
POST /api/credentials
{ "exchange_code":"<ex>", "label":"default", "api_key":"…", "api_secret":"…",
  "passphrase":"…optional…", "enabled":true, "status":"active", "reason":"canary provisioning" }
```
The secrets are encrypted in memory (AES-256-GCM, `nonce‖ciphertext‖tag`, key = SHA-256(master
key)) and only ciphertext is stored. The response returns an id + status — never the plaintext
or the blob. Audited in `credential_audit`.

## 2. Validate the credential (read-only)

A read-only balance check confirms the credential works and stamps `last_checked_at`. The
`health-monitor`'s private probe does this automatically (`credentials.Validate`). Confirm via
`GET /api/credentials` — look for `status=active` and a recent `last_checked_at`. **Validation
never places or cancels orders.**

## 3. Configure caps + canary scope  *(via SQL/admin today)*

Set the `live_controls` singleton: keep `kill_switch=1` for now; set tiny
`max_order_notional` / `max_base_qty`, `max_open_cycles=1` (owner decision: there are NO
daily order-count/quote caps — the historical columns are ignored), `require_canary_ack=1`, `canary_exchange_id` / `canary_market_id` (the single scope), the
freshness windows, and `canary_ack_max_age_minutes`. Mark the exchange + the single market
`live_enabled=1`. Set bootstrap `[execution] mode = "live"`.

## 4. Run a dry-run for the same exchange/symbol

In `[execution] mode = "dry_run"`, let a full simulated cycle reach `CLOSED` for the canary
symbol. A **recent successful dry-run is required** before the first live buy (enforced at
session start + shown in preflight as `recent_dry_run_success`). Then switch mode back to
`live` and restart the live-capable binaries.

## 5. Run preflight (read-only)

```
GET /api/live/preflight?exchange_id=<ex>&market_id=<mk>
```
Confirm `ready=true` with no `failures`. Address any failing check (credential/market/balance
freshness, caps, dry-run, reconcile, stuck/dangerous queue, locks). **Preflight mutates
nothing and contacts no exchange.**

## 6. Acknowledge preflight  *(role: admin)*

```
POST /api/live/acknowledge
{ "exchange_id":<ex>, "market_id":<mk>, "reason":"canary go" }
```
Re-runs preflight, refuses unless `ready` (409), and records a `live_acknowledgements` row
bound to the preflight `config_hash`. **A config change invalidates it; it also expires after
`canary_ack_max_age_minutes`.**

## 7. Disengage the kill switch  *(deliberate)*

Set `live_controls.kill_switch=0`. This is a conscious step; the dashboard shows a **danger**
warning while it is off.

## 8. Start the canary session  *(role: admin)*

```
POST /api/live/session/start
{ "exchange_id":<ex>, "market_id":<mk>, "reason":"first canary run" }
```
Re-runs preflight (409 if not ready) then verifies scope + a current, non-expired,
hash-matching acknowledgement + dynamic readiness + recent dry-run + no active session, and
records an `ACTIVE` `live_run_sessions` row. **A live buy now requires this active session.**

## 9. Watch the first order

- `GET /api/live/session` — session status, order count, quote used, open cycles, last order,
  last deny, kill switch, acknowledgement status.
- `GET /api/live/warnings` — strong live-danger warnings.
- The executor writes a one-time **first-order checklist** onto the session immediately before
  the first real buy is sent (mode/exchange/symbol/caps-remaining/credential-status/ack/session/
  kill-switch/request+order+cycle ids; no secrets).
- Every live decision is in `live_audit`, correlated with `live_session_id` +
  `acknowledgement_id` + `preflight_hash`.

## 10. Stop the session (emergency stop)  *(role: admin)*

```
POST /api/live/session/stop
{ "exchange_id":<ex>, "market_id":<mk>, "reason":"…" }
```
**New buys are blocked immediately.** Risk-reducing **sell / cancel / status** paths keep
working so any open cycle can be exited. The stop is recorded on the session row.

## 11. Engage the global kill switch (broadest stop)

Set `live_controls.kill_switch=1`. Blocks **all** new buy cycles + buy places system-wide;
sells/cancels/status remain allowed. Use this if you cannot identify the canary scope.

## 12. Inspect / export the live audit

- `GET /api/live/audit`-style data is in `live_audit`; per-session export:
  `GET /api/live/session/export[?session_id=<id>]` returns the session, preflight hash,
  acknowledgement, caps, requests, orders, allow decisions, denials, the first-order checklist,
  and the stop reason. **No secrets.**

## 13. Resolve `NEEDS_RECONCILE`  *(role: reconcile_operator or admin)*

```
GET  /api/reconcile                 # list of NEEDS_RECONCILE cycles (with execution mode)
GET  /api/reconcile/{id}            # full context + exposure classification + action catalog
GET  /api/reconcile/audit           # immutable resolution history
POST /api/reconcile/{id}/preview    # exact proposed changes + a preview token (mutates nothing)
POST /api/reconcile/{id}/apply      # apply — requires the preview token
```

**Access.** Every endpoint (including the read-only ones) requires a dashboard **session cookie**
and the exact **reconcile capability** — role `reconcile_operator` OR `admin`. A `viewer` or
`config_operator` gets 403; no session gets 401. A `reconcile_operator` can resolve cases but
**cannot edit configuration** (reconciliation is a separate capability, not a config rank). Create
one with `./bin/dashboard -config <cfg> -create-user <name>:<pass>` then set the role to
`reconcile_operator` (migration 034 permits the value). The audit records the session username —
never a body-supplied name.

**Never blind-close: preview, then apply the SAME operation with the returned token.** Preview
returns `preview_token` and the exact effect: `order_changes[]`, `request_changes[]`,
`fill_to_insert`, `accounting_changes`, `lock_change`, `exposure_before/after`,
`exposure_classification`, `execution_mode`. Apply requires the token (missing → 400) and **409s**
if the cycle/order/request/lock/payload changed since the preview, or the token expired / was used
/ belongs to another operator — re-preview and retry. A `reason` is mandatory.

**Exposure classification — a recorded `filled_quantity = 0` is NOT proof of zero exposure.**
- **PROVEN_ZERO** — net 0 and no order could hold unrecorded inventory (never sent / venue-rejected).
- **OPEN** — recorded net inventory > 0.
- **UNKNOWN** — net 0 but an order reached (or may have reached) the venue with an unconfirmed
  outcome (a timed-out place, a cancel that may have raced a fill). Treat as possible exposure.
- **INCONSISTENT** — recorded sells exceed buys; every resolution is refused until data is fixed.

The symbol lock is released ONLY for PROVEN_ZERO (or a proven full-exit sell). `cancel_zero_exposure`
and `mark_buy_zero_filled` refuse OPEN outright and refuse UNKNOWN unless you confirm the exchange
shows no position with `external_resolution_confirmed=true` + a non-empty `external_resolution_reason`.
`mark_failed` with open/unknown exposure is likewise refused (kept NEEDS_RECONCILE, lock held) unless
externally confirmed.

**Active queue requests block a close.** A cycle cannot be resolved out of NEEDS_RECONCILE (or its
lock released) while any related exchange request — a PLACE/CANCEL **or** a GET_ORDER recovery probe
— is still `QUEUED`/`RETRY_SCHEDULED`/`CLAIMED`/`IN_FLIGHT` (it could still execute). The preview lists
them; let the executor's recovery finish (or resolve those first). Ownership is derived from the
order, so a request with wrong queue metadata still counts.

**Fills and order vs cycle state.** `mark_buy_partially_filled` / `mark_buy_filled` are chosen by
whether the cumulative fill completes the order; `mark_sell_partially_filled` keeps exposure (refused
if it would close it — use `mark_sell_filled`). A sell can fully fill while the cycle still holds
inventory, and the cycle can CLOSE while the sell is only partially filled — order and cycle state are
separate.

**Closing on a sell requires EVERY sell order to be non-executable.** `mark_sell_filled` closes the
cycle + releases the lock only when the chosen sell order is fully filled — a partially-filled sell
whose remainder may still be open on the venue is refused (to avoid a later oversell) unless you
confirm the remainder is cancelled (`external_resolution_confirmed` + reason), in which case that
order is recorded as terminal CANCELLED with its partial fill preserved. It also refuses if ANY OTHER
sell order in the cycle can still execute — one that is active (SUBMITTED/ACKED/PARTIALLY_FILLED) OR
in `NEEDS_RECONCILE` (which may still be open at the venue even with no queue request) — so resolve
every other sell first. When a partial sell instead COMPLETES the sell order but exposure remains and
no other active/unresolved sell exists, the cycle goes to `SELL_REPRICE_PENDING` and the sell manager
creates the next exit sell for the remainder — the remaining inventory is never stranded; if another
sell is in `NEEDS_RECONCILE`, the operation is refused until you resolve it.

**A fill discovered on a TERMINAL order** (a cancel/rejection that raced a venue fill) uses
`correct_terminal_order_fill` (never a normal fill action, which is refused on a terminal order). It
records the fill ONCE and advances the cycle: a FULL discovered fill re-opens the order to FILLED; a
PARTIAL one leaves the order terminal (its cancelled remainder is preserved — never shown as an
active PARTIALLY_FILLED). The cycle then moves to BUY_FILLED/BUY_PARTIALLY_FILLED (buy),
CLOSED (sell that closes exposure), or SELL_REPRICE_PENDING (sell with remaining exposure) — you are
never asked to enter the same fill twice.

**One exchange order id, one internal order.** `attach_exchange_order_id` is serialized per exchange
and backed by a UNIQUE `(exchange_id, exchange_order_id)` index — the same venue id cannot be bound
to two orders, even under concurrent attaches. To find any duplicates before/after:
```sql
SELECT exchange_id, exchange_order_id, COUNT(*) FROM orders
WHERE exchange_order_id IS NOT NULL GROUP BY exchange_id, exchange_order_id HAVING COUNT(*) > 1;
```

**When manual reconciliation is required / operational SQL.**
```sql
-- Cases needing attention + their execution mode:
SELECT id, canonical_symbol, dry_run AS is_dry_run, updated_at FROM cycles WHERE state='NEEDS_RECONCILE';

-- Recorded exposure for a cycle (recorded net = bought - sold; classification also weighs venue risk):
SELECT
  (SELECT COALESCE(SUM(filled_quantity),0) FROM orders WHERE cycle_id=? AND role='entry_buy')
- (SELECT COALESCE(SUM(filled_quantity),0) FROM orders WHERE cycle_id=? AND role='exit_sell') AS recorded_net;

-- Orders carrying venue risk (an id, or a may-have-executed state => a recorded 0 is NOT proof):
SELECT id, role, side, state, filled_quantity, exchange_order_id FROM orders WHERE cycle_id=?;

-- Active requests that BLOCK a close:
SELECT id, request_type, status, order_id FROM exchange_requests
WHERE (cycle_id=? OR order_id IN (SELECT id FROM orders WHERE cycle_id=?))
  AND status IN ('QUEUED','RETRY_SCHEDULED','CLAIMED','IN_FLIGHT');

-- The held symbol lock:
SELECT id, scope, canonical_symbol, state FROM symbol_locks WHERE cycle_id=? AND state='ACTIVE';
```
Resolve manually (via the API preview→apply) when the exposure is OPEN/UNKNOWN and needs an operator
decision, when a fill was discovered on a terminal order, or when data is INCONSISTENT. The DEAD/audit
row is terminal; the resolver never contacts a venue.

---

## 14. Startup failures (the executor refuses to start)

`order-executor` fails fast rather than trading on unknown state. Each of these exits the
binary with a message; none of them can be "waited out" — fix the cause and restart.

| Symptom | Cause | Operator action |
|---|---|---|
| `startup load (exchange tuning): …no exchange_configs row` | a live-enabled exchange has no tuning row | insert the row (`rate_limit_per_sec`, `retry_backoff_ms`), restart |
| `startup load (exchange tuning): …invalid tuning` | negative `rate_limit_per_sec`/`retry_backoff_ms` | correct the values, restart |
| `startup load (exchange tuning): initial exchange-tuning load: …` | the DB was unreachable/unreadable at startup | restore DB access, restart |
| `load exchange cooldowns: …` | `exchange_cooldowns` unreadable | restore the table (migration 033 applied?), restart |

Rationale: an executor that starts with unloaded tuning would send real orders with no pacing;
one that starts without its durable cooldowns would resume hammering a still-throttled venue.

**Invalid PERIODIC reloads do NOT crash the running executor.** After startup, a bad config
reload (a removed `exchange_configs` row for a wired live exchange, or a negative value) is
REJECTED and the last known-good snapshot keeps serving; the executor logs
`config reload rejected (keeping last known-good snapshot)`. Fix the row at leisure — pacing is
never silently disabled by a bad reload. (A bad config is only fatal at STARTUP, above.)

## 15. Cooldown durability failure (per-exchange, entry buys auto-disabled)

A rate-limit **park** is armed in memory immediately and made durable by a background worker
(never inline — a slow write must never delay a successful order response). If the worker
cannot persist a given exchange's park within the grace period (default 30s), that EXCHANGE's
live **entry buys** are disabled and the executor logs (naming the exchange):

```
LIVE ENTRIES DISABLED for exchange: its cooldown could not be persisted within the grace period …
```

This is PER EXCHANGE: an outage persisting exchange A's cooldown does NOT affect exchange B.
What still works even for the affected exchange: **proven exit sells and cancels** (risk-
reducing — a durability outage is a reason to stop CREATING exposure, never a reason to strand
inventory), plus status reads and dry-run/off modes. Nothing is being sent to the throttled
venue either way (the in-process park holds).

Operator action:
1. Check DB health/disk and that `exchange_cooldowns` exists and is writable.
2. Inspect: `SELECT * FROM exchange_cooldowns;` and the `cooldown persistence failed` ERROR logs
   (they name the exchange and how long it has been pending; no secrets are logged).
3. Once writes succeed the executor logs `cooldown durability recovered — live entries re-enabled
   for exchange` and resumes automatically for that exchange. **No restart is required**, and a
   restart during the outage is the thing to avoid — it is exactly when an un-persisted park
   would be lost.

## 15a. Graceful shutdown and the hard-crash limitation

On a GRACEFUL shutdown (SIGTERM/SIGINT) the executor stops claiming, waits for the persistence
worker, and FLUSHES any pending parks to `exchange_cooldowns` within a bounded window
(`ShutdownFlushTimeout`, default 5s), logging `graceful shutdown: pending cooldowns flushed`. So
a normal restart loses no cooldown. If the flush cannot complete (DB down), it logs
`could not flush all cooldowns before the deadline` with the unflushed count and exits anyway —
it never hangs.

**Hard crash (kill -9, power loss):** because persistence is asynchronous, a park armed in the
last moments before a hard crash may not be on disk. This is an accepted limitation — it cannot
be made perfectly durable without risking turning a confirmed order into an ambiguous one. The
loss is self-healing: the venue is still throttling, so the next request re-detects the throttle
and re-parks. Operators need take no action; prefer graceful shutdowns where possible.

## 16. Pre-execution failures, ambiguous vs definitely-unsent, and proven-unexecuted retries

The executor distinguishes requests that DEFINITELY never reached the venue from AMBIGUOUS ones
that might have. This changes how you investigate a stuck request.

**Definitely-not-sent (pre-network) failures.** A live PLACE/CANCEL can fail BEFORE any HTTP
request leaves the process — credential load/decrypt, symbol validation, request/payload build,
auth-token minting, or an already-cancelled send context (e.g. during shutdown). These are
classified `execution.ErrNotSent` and never become ambiguous:
- **Temporary** (e.g. the credential DB was briefly unavailable): the request is re-queued via
  the sanctioned proven-unexecuted path → `RETRY_SCHEDULED`, retried under the normal
  `max_retries` budget. **Safely retryable — no operator action** unless it keeps failing.
- **Permanent** (e.g. an invalid/unmapped symbol, an un-normalizable client id): a terminal
  local failure. A BUY fails cleanly and releases the lock (nothing was placed, no exposure);
  a SELL/CANCEL goes to `NEEDS_RECONCILE` with the lock held. **Fix the root cause** (symbol
  mapping / config) and, for the sell/cancel case, resolve via the reconciliation tool (§13).

**Ambiguous outcomes** (timeout, connection reset, 5xx, unknown response, HTTP-200 throttle
body) mean the mutation MIGHT have executed. These are NEVER retried blindly: the request goes
`DEAD` and a read-only recovery probe (GET_ORDER) resolves the real state; the order/cycle sit
in `NEEDS_RECONCILE` until proven. **Requires reconciliation** (§13), not a resend.

**Telling them apart.** Inspect the request's `last_error` and status:
- `RETRY_SCHEDULED` with a `pre-execution (temporary, not sent)` cause → definitely unsent,
  auto-retrying. Watch that `retry_count` is climbing toward `max_retries`.
- `FAILED` with a `pre-execution (permanent, not sent)` cause → definitely unsent, terminal.
- `DEAD` with an ambiguous cause (timeout/unknown) → maybe-sent; a GET_ORDER probe exists and
  the order is `NEEDS_RECONCILE`.

**A final guard denied an order whose state changed during pacing.** If the kill switch (or
session/preflight/enable-flag/credential/order/cycle state) changed while the request waited in
the pacer, the FINAL guard denies it at send time. A BUY is resolved by
`orders.OnBuyDenied`: if it can prove no exposure (still QUEUED, no fill, no exchange id, cycle
pre-send) the request/order/cycle go `FAILED` and the lock is released; otherwise the lock is
HELD and order+cycle go `NEEDS_RECONCILE` (resolve via §13). Sells/cancels always hold the lock
on denial.

**Backoff — local failure vs venue rate limit.** A temporary LOCAL not-sent (e.g. a credential
DB blip) retries with bounded EXPONENTIAL backoff (seeded by the exchange's `retry_backoff_ms`,
else 1s, jittered) — its `next_retry_at` is seconds-to-minutes out, NOT immediate, so it does
not burn all retries at once. A venue-PROVEN rate limit (including a Bitpin auth-endpoint 429)
retries at the exchange `cooldown_until` deadline instead. The `last_error` always names the real
cause; a credential failure is NEVER logged as "rate-limited".

**Bitpin auth/token failures.** Bitpin acquires/refreshes a JWT before the order call. If the
auth (`authenticate`/`refresh_token`) endpoint 429s, times out, or returns an invalid token, the
ORDER endpoint was never called — it is `ErrNotSent`, not an order ambiguity: an auth 429 arms
Bitpin's cooldown and the request retries after it; a timeout/transient uses local backoff. You
will NOT see an order GET_ORDER probe for these. Look for `auth` in the `last_error`.

**Pre-handler failures never strand.** A transient DB error reading the order role (before the
place is dispatched) re-queues (`RETRY_SCHEDULED`) — the cycle is not touched. A malformed
CANCEL payload (or an inconsistent request whose `cycle_id` does not match the order's real
cycle) resolves the ACTUAL order+cycle to `NEEDS_RECONCILE` with the lock HELD and the request
`DEAD` — an unrelated cycle is never modified or unlocked. Resolve the reconcile via §13.

**Exhausted definitely-unsent retries.** When a temporary not-sent (or a venue-proven
pre-execution rejection) exhausts `max_retries`, `RequeueProvenUnexecuted` atomically moves the
request to `DEAD` and its order to `NEEDS_RECONCILE`. To investigate:
```sql
SELECT id, request_type, status, retry_count, max_retries, last_error, order_id
FROM exchange_requests WHERE status='DEAD' AND request_type IN ('PLACE_ORDER','CANCEL_ORDER')
ORDER BY updated_at DESC;
```
The `last_error` names the repeated pre-execution cause (e.g. a persistent credential/decrypt
failure). Exhaustion resolves the request + order + cycle + lock atomically, by operation:
- **entry buy, still provably unsent (zero exposure)** → request DEAD, order+cycle FAILED, lock
  RELEASED (nothing to reconcile);
- **exit sell** → request DEAD, order+cycle NEEDS_RECONCILE, lock HELD (inventory);
- **cancel** → request DEAD, order+cycle NEEDS_RECONCILE, lock HELD (a venue order may be open).
Fix the root cause; for the sell/cancel cases resolve the owning order via the reconciliation
tool (§13). The request itself is terminal and is never auto-resent.

## 17. Malformed mutating requests and stale-recovery operations

**Identifying a malformed mutating request.** A `PLACE_ORDER`/`CANCEL_ORDER` must have both
`cycle_id` and `order_id`; the queue rejects new ones and refuses to claim old ones that don't.
Find historical/manual malformed rows:
```sql
SELECT id, request_type, status, cycle_id, order_id, last_error
FROM exchange_requests
WHERE request_type IN ('PLACE_ORDER','CANCEL_ORDER') AND (order_id IS NULL OR cycle_id IS NULL);
```
The executor's sweep finalizes them automatically: request → DEAD, and (if the cycle is known)
that cycle → `NEEDS_RECONCILE` with its lock HELD — logged as
`MALFORMED mutating request finalized DEAD`. Resolve the cycle via §13. If neither id is present,
only the request is marked DEAD (there is nothing to reconcile).

**Recovering a stale mutation without an order id.** A stale `IN_FLIGHT` mutation whose order
cannot be identified is finalized conservatively — request DEAD, the identifiable cycle
`NEEDS_RECONCILE`, lock HELD — never left `IN_FLIGHT`. You will see
`stale mutating request finalized conservatively` in the logs.

**Bitpin auth rate limits and queued orders.** Bitpin gets a JWT before each order. If the auth
endpoint is rate-limited (429, or a 200 reporting the quota exhausted), the order is NOT sent that
invocation: the Bitpin cooldown is armed and the request retries after `cooldown_until` — you will
NOT see an order GET_ORDER probe (the order endpoint was never reached). This is expected; no
action beyond watching the cooldown clear.

**Inspecting requests stuck in stale recovery.** A stale `IN_FLIGHT` mutation whose
`orderRecoveryInfo` keeps failing transiently is retried on each sweep, bounded by the recovery
hard limit (its `inflight_at` age vs the recovery TotalTimeout, min 5m). To see them:
```sql
SELECT id, request_type, status, inflight_at, TIMESTAMPDIFF(MINUTE, inflight_at, NOW(6)) AS stale_minutes
FROM exchange_requests
WHERE status='IN_FLIGHT' AND request_type IN ('PLACE_ORDER','CANCEL_ORDER')
ORDER BY inflight_at;
```
If `stale_minutes` exceeds the hard limit the next sweep finalizes the row (DEAD + reconcile).
**Manual reconciliation is required** for any order/cycle left `NEEDS_RECONCILE` — resolve via
§13; the request itself is terminal and never auto-resent.

## 18. The mutation send boundary, Bitpin pacing, and stale-recovery ownership

**How the send boundary prevents false ambiguity (all three private venues).** Every mutating
request goes through a two-stage adapter: `PreparePlace`/`PrepareCancel` does ALL fallible work
(credentials, symbol, payload, and the final HTTP request) while the request is still `CLAIMED`;
only then does the executor commit `MarkInFlight` and immediately call `Send`, which just binds the
context and performs the one order/cancel HTTP call. So:
- a request that is still `CLAIMED` (or was swept back to `QUEUED`) was **definitely not sent** — no
  reconciliation needed;
- a request that is `IN_FLIGHT` **may or may not** have reached the venue — it is resolved by the
  read-only recovery probe, never a blind resend.

**Identifying a false-ambiguity-prevention failure.** The design guarantees no adapter does fallible
work after `MarkInFlight`. If you ever see a mutating request go `IN_FLIGHT` and then fail with a
LOCAL error (bad symbol, credential/token error, request-construction error) rather than a network
error, that is a boundary regression — such errors must occur during preparation while `CLAIMED`.
Check `last_error` on `IN_FLIGHT`/`DEAD` mutating rows:
```sql
SELECT id, request_type, status, last_error
FROM exchange_requests
WHERE request_type IN ('PLACE_ORDER','CANCEL_ORDER') AND status IN ('IN_FLIGHT','DEAD')
ORDER BY inflight_at DESC LIMIT 50;
```
A healthy `IN_FLIGHT` failure reads as a timeout/connection error; a local-validation error on an
`IN_FLIGHT` row should be reported as a bug.

**Inspecting "prepared" vs `IN_FLIGHT`.** Preparation is in-process and leaves no distinct DB state —
a request being prepared is still `CLAIMED`. The only durable, operator-visible mutation states are
`CLAIMED` (claimed, possibly preparing, definitely not sent) and `IN_FLIGHT` (past MarkInFlight,
possibly sent). To see what is in each:
```sql
SELECT status, COUNT(*) FROM exchange_requests
WHERE request_type IN ('PLACE_ORDER','CANCEL_ORDER') GROUP BY status;
```
A `CLAIMED` mutation that is old (not being actively worked) is swept back to `QUEUED`; an old
`IN_FLIGHT` one is recovered (§17).

**How Bitpin pacing differs with cached vs refreshed tokens.** Bitpin needs a JWT before each order.
Pacing reservations equal ACTUAL HTTP calls:
- **fresh cached token** → no auth call → the mutation consumes **one** pacing slot (the order);
- **missing/expired token** → one auth (or refresh) call + the order → **two** pacing slots.
So Bitpin throughput naturally halves for the first order after a token expiry and returns to full
rate while the token stays fresh. This is expected; no action. (Nobitex/Wallex authenticate with an
in-memory header and always consume exactly one slot per mutation.)

**How an ownership mismatch in stale recovery is finalized.** If a stale `IN_FLIGHT` mutation's queue
row names a cycle or exchange that does NOT match the order it points at (or the row has a NULL
`cycle_id` but a valid `order_id`), recovery does NOT probe the claimed cycle. It derives the real
cycle FROM THE ORDER and finalizes conservatively: **request → DEAD, the ACTUAL order + cycle →
`NEEDS_RECONCILE`, the lock is HELD, and any unrelated cycle/lock is left untouched.** You will see
`stale mutating request finalized on ownership mismatch` in the logs. To find the authoritative
cycle for such a request, always read it from the ORDER, not the request:
```sql
SELECT o.id AS order_id, o.cycle_id AS authoritative_cycle, o.exchange_id AS authoritative_exchange,
       er.cycle_id AS request_claimed_cycle, er.exchange_id AS request_claimed_exchange
FROM exchange_requests er JOIN orders o ON o.id = er.order_id
WHERE er.id = ?;    -- the stale/malformed request id
```
When `request_claimed_cycle` differs from `authoritative_cycle`, the order's cycle is the source of
truth and the one that will be in `NEEDS_RECONCILE`.

**When manual reconciliation is required.** Any order/cycle left `NEEDS_RECONCILE` by the above needs
operator resolution via §13. The request row itself is terminal (`DEAD`) and is never auto-resent.
The unrelated (wrongly-claimed) cycle needs NO action — it was deliberately not touched.

## 19. Authoritative ownership: which executor owns a stray mutation, and Bitpin auth cooldowns

**Which executor owns a malformed or stale mutation.** Ownership is ALWAYS the order's, never the
queue row's. The rules the sweeps apply:
- A mutation with a valid `order_id` is owned by the **execution mode of the order's cycle**
  (`orders.cycle_id → cycles.dry_run`). The LIVE `order-executor` finalizes live-cycle rows; the
  DRY-RUN one finalizes dry-run-cycle rows. Neither ever touches the other's cycles or locks.
- A mutation with no `order_id` but a valid `cycle_id` is owned by that claimed cycle's mode.
- A mutation with neither a trustworthy order nor cycle is marked `DEAD` by any live/dry-run
  executor, and NO cycle/lock is touched.
- An `off` executor finalizes nothing.

So if a stray/malformed row is not being cleaned up, check that the executor for its authoritative
mode is running. Find the authoritative mode:
```sql
SELECT er.id, er.status,
       COALESCE(oc.dry_run, rc.dry_run) AS authoritative_dry_run,   -- 0=live owner, 1=dry-run owner
       er.order_id, er.cycle_id AS claimed_cycle, er.exchange_id AS claimed_exchange,
       o.cycle_id AS order_cycle, o.exchange_id AS order_exchange
FROM exchange_requests er
LEFT JOIN orders o  ON o.id = er.order_id
LEFT JOIN cycles oc ON oc.id = o.cycle_id
LEFT JOIN cycles rc ON rc.id = er.cycle_id
WHERE er.id = ?;
```
`authoritative_dry_run = 0` → the LIVE executor owns it; `= 1` → the DRY-RUN executor. If it is NULL,
neither ownership is trustworthy and the row is DEAD-only.

**Recovering a mutation with NULL `cycle_id`.** As long as `order_id` is valid, the sweeps derive the
real cycle from the order. It is finalized by whichever executor matches the ORDER's cycle mode:
request → `DEAD`, the order and its real cycle → `NEEDS_RECONCILE`, lock HELD. You do NOT need to
back-fill the queue row's `cycle_id`; the order is authoritative.

**Unknown / unwired claimed exchange.** A stale mutation whose `exchange_id` points to an unknown,
disabled, or unwired exchange is still discovered through its order (the sweep does not filter by the
queue exchange). It is finalized conservatively (request DEAD, order + cycle `NEEDS_RECONCILE`, lock
HELD). It cannot sit `IN_FLIGHT` forever waiting for an executor that never iterates that exchange.

**Verifying Bitpin's real auth cooldown deadline.** When Bitpin's auth/refresh is throttled, the
queued mutation's `next_retry_at` reflects the VENUE deadline (Retry-After / X-RateLimit-Reset /
remaining window), not the 1s configured fallback. Verify:
```sql
SELECT id, status, retry_count, max_retries,
       TIMESTAMPDIFF(SECOND, NOW(6), next_retry_at) AS seconds_until_retry, last_error
FROM exchange_requests
WHERE exchange_id = (SELECT id FROM exchanges WHERE code='bitpin')
  AND request_type IN ('PLACE_ORDER','CANCEL_ORDER') AND status='RETRY_SCHEDULED';
```
`seconds_until_retry` should be close to the venue's advertised throttle (tens of seconds), not ~1.

**Detecting premature retry-budget exhaustion.** The symptom the round-9 fix removes: a Bitpin
mutation reaching `DEAD` with `retry_count = max_retries` within a few seconds of an auth throttle,
while the venue's throttle window was still open. If you see that pattern, the auth deadline is not
being propagated — treat it as a regression. Normally a throttled mutation consumes ONE attempt then
waits the real window; the exchange is also parked (`exchange_cooldowns`) for the same deadline.

**No unclaimable recovery probe.** A `GET_ORDER` recovery probe is created ONLY when the order's
exchange has a usable read-only recovery path in the running executor: a wired client AND
`exchanges.enabled = 1`. If the exchange has no client after a restart (no credential, disabled for
live, not constructed) or is disabled, the stale mutation is finalized conservatively instead —
request `DEAD`, ACTUAL order + cycle `NEEDS_RECONCILE`, lock HELD — and NO probe is queued. If you
ever find a `GET_ORDER` probe sitting `QUEUED` with no executor claiming it, that is a regression;
by design none can exist. To check:
```sql
SELECT er.id, e.code, e.enabled, er.status, er.created_at
FROM exchange_requests er JOIN exchanges e ON e.id = er.exchange_id
WHERE er.request_type='GET_ORDER' AND er.status='QUEUED'
  AND er.created_at < NOW(6) - INTERVAL 10 MINUTE;
```

**Probes that become unclaimable AFTER creation.** A probe can be created while its exchange is
usable and then be stranded when the exchange is later disabled, its credential is removed, or a
restart does not construct its client. `sweepUnclaimableRecoveryProbes` runs at STARTUP (before the
first claim) and periodically, finds such probes by JOINing the persisted order (ownership is the
order's, never the probe row's), scopes by the order's cycle mode, and finalizes each: probe `DEAD`,
ACTUAL order + cycle `NEEDS_RECONCILE`, lock HELD. So a restart cannot leave an existing probe
stranded, and a mid-life exchange disable is cleaned within one sweep. Detect ownership-vs-claim for
a queued probe the same way as §19's ownership query — always trust `orders.exchange_id`/`cycle_id`,
not the probe row's. The `created_at` query above should return NOTHING once a sweep has run; if it
returns rows whose `e.enabled=0` or whose exchange has no running client, verify the executor for
that mode is actually running (an `off` or wrong-mode executor will not finalize them).

**Sweep fairness across modes.** The malformed-mutation sweep filters by execution mode INSIDE its
SQL query, before the LIMIT, ordered by request id. Rows belonging to the other mode never occupy
this executor's sweep window, so a backlog of dry-run junk cannot delay live cleanup (or vice
versa). If malformed rows of YOUR mode are not draining, verify the executor for that mode is
actually running — the other mode's executor will never take them.

**When manual reconciliation is required.** Any order/cycle left `NEEDS_RECONCILE` needs operator
resolution via §13. That happens in exactly these recovery cases: an ownership mismatch was
finalized; a stale mutation's order stayed unreadable past the recovery window (finalized on the
RETAINED actual cycle — never the claimed one); the order's exchange had no usable recovery client
or was disabled (no probe possible); or a probe/retry budget was exhausted legitimately (the venue
stayed throttled past `max_retries × real-deadline`). The DEAD request is terminal in every case.

## Emergency-stop behavior by cycle state

"Emergency stop" = **Stop the session** (scope-local) or **engage the kill switch** (global).
Neither recalls a request already sent to the venue; both block *new* buys and keep
risk-reducing paths working. The reconciler/executor continue their conservative handling.

| Cycle state at stop | New buys | What keeps working |
|---|---|---|
| Before buy send (QUEUED) | **blocked** (guard denies; request fails, nothing sent) | n/a |
| Buy `IN_FLIGHT` | **blocked** | the in-flight place is not recalled; its ack/timeout is recorded normally; ambiguous → `NEEDS_RECONCILE` (never blind-resent) |
| Buy filled, sell not placed | **blocked** | **sell placement proceeds** (risk-reducing) so inventory is exited |
| Sell resting | **blocked** | reprice/cancel of the resting sell allowed |
| Sell `IN_FLIGHT` | **blocked** | its status is read normally; cancel allowed |
| Cancel `IN_FLIGHT` | **blocked** | the cancel completes; status read normally |
| Cycle `NEEDS_RECONCILE` | **blocked** | unaffected by stop; resolve via the operator reconciliation tool (step 13) |

Sells, cancels, and status reads never pass through the canary buy gate, so they are
unaffected by stopping the session; the kill switch is asymmetric for the same reason.
