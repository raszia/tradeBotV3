-- PR21: operator resolution audit for NEEDS_RECONCILE cycles/orders.
--
-- Every operator resolution (preview is read-only and is NOT recorded here; only an
-- applied resolution writes a row) is captured immutably: who, when, which cycle/order,
-- the action, the old/new states, the reason, any supplied fill data, a before/after
-- snapshot, and whether the symbol lock was released. This is the audit trail required
-- before any NEEDS_RECONCILE case may be closed — there is no blind/automatic close.
--
-- DDL-only file (house rule: never mix DDL + DML in one migration).
CREATE TABLE IF NOT EXISTS reconcile_resolutions (
  id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  cycle_id         BIGINT UNSIGNED NOT NULL,
  order_id         BIGINT UNSIGNED NULL,                 -- the order acted on, when applicable
  operator         VARCHAR(128) NOT NULL,                -- authenticated dashboard operator name
  action           VARCHAR(48)  NOT NULL,                -- resolution action key
  old_cycle_state  VARCHAR(48)  NULL,
  new_cycle_state  VARCHAR(48)  NULL,
  old_order_state  VARCHAR(48)  NULL,
  new_order_state  VARCHAR(48)  NULL,
  reason           TEXT NULL,                            -- operator-supplied reason (never secrets)
  fill_json        JSON NULL,                            -- supplied fill data, if any (qty/price/fee/asset/side)
  before_json      JSON NULL,                            -- snapshot before applying
  after_json       JSON NULL,                            -- snapshot after applying
  lock_released    TINYINT(1) NOT NULL DEFAULT 0,        -- whether the symbol lock was released
  -- mark_failed safety (correction): when exposure was open/unknown, FAILED is only
  -- allowed if the operator EXPLICITLY confirms the exposure was handled outside the
  -- system. This flag records that confirmation; the external reason is folded into reason.
  external_resolution_confirmed TINYINT(1) NOT NULL DEFAULT 0,
  created_at       TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_reconcile_res_cycle (cycle_id),
  KEY idx_reconcile_res_order (order_id),
  KEY idx_reconcile_res_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
