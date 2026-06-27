-- migrate:ddl
-- 013_balance_last_seen
--
-- PR13 balance sync records WHEN an exchange/asset balance was last observed,
-- separate from when it last CHANGED. last_seen_at is bumped on every observation;
-- the balance columns + balance_hash change only when the balance actually changes
-- (history rows are the change log). This lets later reconciliation/dashboard detect
-- a STALE balance (an asset that stopped appearing in the venue response) WITHOUT
-- zeroing or deleting it — a missing asset is never treated as zero unless the venue
-- response explicitly proves it. DDL only, idempotent.
ALTER TABLE wallet_balances_current
  ADD COLUMN IF NOT EXISTS last_seen_at DATETIME(6) NULL AFTER balance_hash;
