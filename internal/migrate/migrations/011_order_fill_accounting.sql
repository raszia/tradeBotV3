-- migrate:ddl
-- 011_order_fill_accounting
--
-- PR10 processes executor results and records the final outcome of the simulated-IOC
-- buy: PLACE_ORDER → wait → CANCEL_ORDER → GET_ORDER → record fills. These columns
-- persist the fill accounting + evidence on the order (the per-fill rows go in the
-- existing `fills` table; aggregate result + evidence live here for easy reporting).
--
-- actual_execution_mode was already added in migration 010. DDL only, idempotent.
ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS remaining_quantity     DECIMAL(36,18) NULL AFTER filled_quantity,
  ADD COLUMN IF NOT EXISTS fill_result            VARCHAR(16)    NULL AFTER actual_execution_mode,
  ADD COLUMN IF NOT EXISTS last_normalized_status VARCHAR(32)    NULL AFTER fill_result;
