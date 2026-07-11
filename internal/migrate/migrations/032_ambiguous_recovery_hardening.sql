-- migrate:ddl
-- 032_ambiguous_recovery_hardening
--
-- PR19 round 3 — hardening of the ambiguous-execution recovery.
--
-- (a) simexec faithfulness (deterministic across restarts/instances): store the full set of
--     exchange-visible IMMUTABLE fields with the order (order_type, time_in_force — symbol,
--     side, quantity, limit_price, client_order_id, scenario already exist) so GetOrder/
--     CancelOrder and the idempotency/immutable-field comparison use the order's OWN persisted
--     values, never the current client instance's configured scenario. `hidden_probes` models
--     eventual-consistency DELAY: while > 0 a GetOrder returns "unknown" (and decrements it), so
--     an accepted order can be invisible on the first probe(s) and appear on a later one — the
--     recovery must treat a first "not found" as ambiguous, not proof-of-not-placed.
--
-- The requirement that a MUTATING request can never REACH the exchange without a cycle + order
-- (it could not otherwise be classified dry-run vs live, nor recovered) is enforced at RUNTIME —
-- the right layer, since `exchange_requests` is a generic priority queue that must not be coupled
-- to trading FKs. It is guarded twice: (1) the mode-scoped claim (queue.Claim) only claims a
-- mutating request whose cycle's dry_run matches the executor's mode — a cycle-less mutating row
-- is never claimable by a real dry-run/live executor; (2) a pre-send fail-closed guard in the
-- order-executor: a PLACE/CANCEL with a missing cycle_id or order_id is FAILED without any
-- exchange/simexec call. Read-only request types keep allowing a NULL cycle (a bare GET_BALANCE
-- has no cycle). DDL only, idempotent.

ALTER TABLE sim_exchange_orders
  ADD COLUMN IF NOT EXISTS order_type    VARCHAR(16) NOT NULL DEFAULT 'limit' AFTER limit_price,
  ADD COLUMN IF NOT EXISTS time_in_force VARCHAR(8)  NOT NULL DEFAULT ''      AFTER order_type,
  ADD COLUMN IF NOT EXISTS hidden_probes INT         NOT NULL DEFAULT 0       AFTER filled_quantity;
