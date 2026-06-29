-- migrate:ddl
-- PR2 correction: schema hardening.
--
-- Adds the foreign keys, operational indexes, and DB-level financial-value CHECK constraints
-- that the original PR2 tables were missing. Delivered as a FORWARD ALTER migration (not an
-- edit of the original 002/005/006 files) to honour the immutable-applied-migrations rule
-- enforced by migrate.EnsureCurrent (PR1 correction): an already-applied migration is never
-- edited — schema changes ship as a new NNN file.
--
-- DDL-only, idempotent (IF NOT EXISTS) so a re-run after a mid-file failure is safe. A CHECK
-- comparison against NULL is "unknown" → the CHECK passes, so nullable columns keep their
-- NULL semantics. MariaDB 10.2.1+ enforces CHECK constraints.

-- 1. Operational indexes: the system queries the latest rows by exchange + symbol, and these
-- two tables grow large. (Both already have (canonical_symbol, created_at) + (created_at).)
ALTER TABLE comparison_events
  ADD INDEX IF NOT EXISTS idx_cmp_exchange_symbol (exchange_id, canonical_symbol, created_at);
ALTER TABLE signals
  ADD INDEX IF NOT EXISTS idx_signals_exchange_symbol (exchange_id, canonical_symbol, created_at);

-- 2. Foreign keys on signals (cycle_id already had one). exchange_id / config_version are
-- NULLable; a NULL passes the FK (a signal may carry no exchange / predate any config). The
-- engine only writes a signal under an active config (version > 0), so config_version is
-- always a real config_versions.id or NULL. The exchange_id FK reuses the index added above.
ALTER TABLE signals
  ADD CONSTRAINT fk_signals_exchange FOREIGN KEY IF NOT EXISTS (exchange_id) REFERENCES exchanges (id),
  ADD CONSTRAINT fk_signals_config   FOREIGN KEY IF NOT EXISTS (config_version) REFERENCES config_versions (id);

-- 3. Financial-value CHECK constraints.
ALTER TABLE orders
  ADD CONSTRAINT IF NOT EXISTS chk_orders_qty_nonneg    CHECK (quantity >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_orders_filled_nonneg CHECK (filled_quantity >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_orders_filled_le_qty CHECK (filled_quantity <= quantity),
  ADD CONSTRAINT IF NOT EXISTS chk_orders_price_nonneg  CHECK (limit_price >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_orders_fee_nonneg    CHECK (fee_amount >= 0);

ALTER TABLE fills
  ADD CONSTRAINT IF NOT EXISTS chk_fills_qty_nonneg   CHECK (quantity >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_fills_price_nonneg CHECK (price >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_fills_fee_nonneg   CHECK (fee_amount >= 0);

-- tick_size / step_size use 0 (or NULL) as the "no snapping" sentinel — SnapDownToTick/
-- SnapQtyToStep return the value unchanged when the tick/step is not positive. So the safe
-- DB invariant is NON-NEGATIVE (a negative tick/step is nonsensical; 0 = unconstrained),
-- which prevents bad values while preserving the system's 0-means-no-snap semantics.
ALTER TABLE exchange_markets
  ADD CONSTRAINT IF NOT EXISTS chk_em_tick_nonneg   CHECK (tick_size >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_em_step_nonneg   CHECK (step_size >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_em_minqty_nonneg CHECK (min_order_quantity >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_em_minamt_nonneg CHECK (min_order_amount >= 0);

-- Spot balances are non-negative: this system handles only spot wallets, so a negative
-- balance is an anomaly to surface (a failed CHECK), not a normal state. No documented
-- exception allows negative spot balances; if margin/debt wallets are ever supported, a new
-- migration would relax these with an explicit rationale.
ALTER TABLE wallet_balances_current
  ADD CONSTRAINT IF NOT EXISTS chk_balcur_avail_nonneg  CHECK (available >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_balcur_locked_nonneg CHECK (locked >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_balcur_total_nonneg  CHECK (total >= 0);
ALTER TABLE wallet_balance_history
  ADD CONSTRAINT IF NOT EXISTS chk_balhist_avail_nonneg  CHECK (available >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_balhist_locked_nonneg CHECK (locked >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_balhist_total_nonneg  CHECK (total >= 0);
