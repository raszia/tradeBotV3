-- migrate:ddl
-- 012_cycle_exit_accounting
--
-- PR11 manages the exit sell and closes the cycle. These columns hold the sell-side
-- accounting + realized result on the cycle (the per-fill rows go in the existing
-- `fills` table; the sell ORDER carries its own filled_quantity/avg_fill_price/fee
-- like the buy order). last_reprice_at gates how often a resting sell is repriced.
--
-- realized_quote is the realized PnL in the quote currency and is NULL when it
-- cannot be computed safely (e.g. fees in a non-quote asset). DDL only, idempotent.
ALTER TABLE cycles
  ADD COLUMN IF NOT EXISTS sold_quantity    DECIMAL(36,18) NULL AFTER buy_size,
  ADD COLUMN IF NOT EXISTS avg_sell_price   DECIMAL(36,12) NULL AFTER sold_quantity,
  ADD COLUMN IF NOT EXISTS sell_quote       DECIMAL(36,8)  NULL AFTER avg_sell_price,
  ADD COLUMN IF NOT EXISTS sell_fee         DECIMAL(36,18) NULL AFTER sell_quote,
  ADD COLUMN IF NOT EXISTS sell_fee_asset   VARCHAR(32)    NULL AFTER sell_fee,
  ADD COLUMN IF NOT EXISTS net_quantity     DECIMAL(36,18) NULL AFTER sell_fee_asset,
  ADD COLUMN IF NOT EXISTS realized_quote   DECIMAL(36,8)  NULL AFTER net_quantity,
  ADD COLUMN IF NOT EXISTS last_reprice_at  DATETIME(6)    NULL AFTER realized_quote,
  ADD COLUMN IF NOT EXISTS close_reason     VARCHAR(48)    NULL AFTER last_reprice_at;
