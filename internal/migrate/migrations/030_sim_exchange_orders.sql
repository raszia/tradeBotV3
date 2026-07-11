-- migrate:ddl
-- 030_sim_exchange_orders
--
-- PR19 correction: PERSISTENT simulated-exchange order state for dry-run mode. The dry-run
-- simulator (internal/simexec) must not keep orders in a process-local map — otherwise a
-- follow-up GET_ORDER handled by a DIFFERENT order-executor instance (or after a restart, or
-- by the reconciler) would return "order unknown" and spuriously push a dry-run cycle to
-- NEEDS_RECONCILE. Every simulated PlaceOrder is recorded here (keyed by the deterministic
-- SIM-<client_order_id>), so any process/instance can look it up and compute the SAME
-- deterministic outcome from the stored state.
--
-- PR19 round 2 (ambiguous-execution): the row now carries an EXPLICIT, MUTABLE lifecycle
-- (status + filled_quantity) instead of only a fixed scenario, so simulated timeouts can be
-- modelled faithfully:
--   * a PlaceOrder can be "accepted but the ack timed out": the row is persisted FIRST (with
--     its real state/fill) and PlaceOrder then returns ErrAckTimeout WITHOUT the exchange id,
--     so a later read-only lookup by client_order_id discovers the true outcome;
--   * a CancelOrder can change the persisted state (canceled / partially-then-canceled /
--     filled-before-cancel / still-open) and THEN return ErrAckTimeout, so the read-only
--     recovery GetOrder returns the real post-cancel state.
-- The `scenario` still selects which of those outcomes a client produces.
--
-- client_order_id is now NOT NULL and UNIQUE per exchange so (a) an order can be looked up by
-- client_order_id when the exchange_order_id is unknown after a place timeout, and (b) a
-- re-placed identical client_order_id is idempotent (INSERT collides → the first accepted
-- order is immutable) instead of overwriting. exchange_order_id stays deterministic
-- (SIM-<client_order_id>) and independently unique.
--
-- This is SIMULATION state only — it references no real exchange and holds no secrets. DDL
-- only, idempotent.
CREATE TABLE IF NOT EXISTS sim_exchange_orders (
  id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_code     VARCHAR(64)  NOT NULL,
  exchange_order_id VARCHAR(128) NOT NULL,               -- SIM-<client_order_id> (deterministic)
  client_order_id   VARCHAR(128) NOT NULL,               -- the system's local client order id
  symbol            VARCHAR(64)  NOT NULL,
  side              VARCHAR(8)   NOT NULL,                -- buy | sell
  quantity          DECIMAL(36,18) NOT NULL,
  limit_price       DECIMAL(36,18) NOT NULL,
  scenario          VARCHAR(48)  NOT NULL,                -- the scenario captured at placement
  status            VARCHAR(16)  NOT NULL DEFAULT 'OPEN', -- OPEN | FILLED | CANCELED | UNKNOWN
  filled_quantity   DECIMAL(36,18) NOT NULL DEFAULT 0,
  created_at        TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at        TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  -- Both identifiers are unique per exchange: exchange_order_id for the normal GetOrder path,
  -- client_order_id for the post-place-timeout recovery lookup AND for immutable idempotency.
  UNIQUE KEY uq_sim_order  (exchange_code, exchange_order_id),
  UNIQUE KEY uq_sim_client (exchange_code, client_order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
