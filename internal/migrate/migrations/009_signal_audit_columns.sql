-- migrate:ddl
-- 009_signal_audit_columns
--
-- The trade-engine compares an Iranian market against Binance. For IRT/IRR
-- markets it converts the Binance USDT price into the Iranian quote using a
-- USDT/IRT reference rate. To make every comparison auditable (rule: do not mix
-- quote units silently), record the quote unit and the conversion rate used.
--
-- For USDT-quoted markets reference_rate is NULL (no conversion). binance_price in
-- comparison_events/signals is stored already CONVERTED to the Iranian quote unit,
-- so binance_price and iranian_price are directly comparable. DDL, idempotent.
ALTER TABLE comparison_events
  ADD COLUMN IF NOT EXISTS quote_unit     VARCHAR(16)    NULL AFTER canonical_symbol,
  ADD COLUMN IF NOT EXISTS reference_rate DECIMAL(36,12) NULL AFTER iranian_price;

ALTER TABLE signals
  ADD COLUMN IF NOT EXISTS quote_unit     VARCHAR(16)    NULL AFTER canonical_symbol,
  ADD COLUMN IF NOT EXISTS reference_rate DECIMAL(36,12) NULL AFTER iranian_price;
