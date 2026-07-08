# v3TradeBot — First Real Canary Live Run (Operator Runbook)

> **Scope is intentionally tiny.** A live run is **one exchange, one symbol, one open cycle,
> a tiny notional**, and only proceeds when preflight passes, an acknowledgement is active
> and not expired, and a canary session is started. Nothing here broadens that scope.
>
> All endpoints below are JSON over the dashboard binary. The mutating operator endpoints
> used in this runbook (PR17+) need a bearer token (`Authorization: Bearer <token>`); the
> required role is noted per step. Secrets are never shown or logged anywhere in this flow.
>
> **Network exposure (important).** The PR16 **read-only** dashboard views/WebSocket have
> **no built-in user authentication** — read-only does NOT mean safe to expose. Keep the
> dashboard bound to `127.0.0.1` (the shipped default) and reach it only over a trusted VPN
> or an authenticated reverse proxy; never publish port `8080` to an untrusted network. Bearer
> tokens gate the PR17+ *mutating* endpoints, not the read-only views. See DEPLOY.md §3a.

## 0. Pre-requisites (one-time)

- Real **master key** set in the bootstrap config file (`[security] master_key`) — never an
  env var. With no/invalid master key, credential loading + live execution are safe-disabled.
- Migrations applied (`cmd/migrate`); every binary fails fast if a migration is pending.
- Dashboard tokens provisioned out-of-band (SHA-256 hashes in `dashboard_tokens`): at least
  one `admin`, one `credential_operator`, one `config_operator`.

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
`max_order_notional` / `max_base_qty`, `max_open_cycles=1`, the daily caps,
`require_canary_ack=1`, `canary_exchange_id` / `canary_market_id` (the single scope), the
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

## 13. Resolve `NEEDS_RECONCILE`  *(role: reconcile_operator/admin)*

```
GET  /api/reconcile                 # list
GET  /api/reconcile/{id}            # full context
POST /api/reconcile/{id}/preview    # exact proposed changes, mutates nothing
POST /api/reconcile/{id}/apply      # apply with a reason
```
Never blind-close: preview then apply. The lock is released only on proven zero exposure /
full exit. `mark_failed` with open/unknown exposure is refused unless explicitly forced with
`external_resolution_confirmed=true` + an `external_resolution_reason`.

---

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
