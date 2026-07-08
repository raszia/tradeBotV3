-- migrate:ddl
-- 027_regime_history_and_config_checks
--
-- PR15 correction. Two things:
--
-- 1. market_regime_history must capture the FULL regime evolution, exactly like the hash
--    that gates it — including stale_reason and the state_hash itself. Without these,
--    "history" can show THAT the regime changed but not WHAT changed (e.g. an UNKNOWN
--    whose stale_reason changed inserts a new row but the dashboard can't see why). Add
--    both columns so a history row is self-describing.
--
-- 2. Regime basket config validation at the DB layer (defence in depth; LoadBaskets also
--    validates in code). Invalid config must never reach the calculator. A CHECK compared
--    against NULL is "unknown" → passes, so nullable columns keep NULL semantics. MariaDB
--    10.2.1+ enforces CHECK constraints. DDL-only, idempotent (IF NOT EXISTS) so a re-run
--    after a mid-file failure is safe.

-- 1. Full-field history columns (mirror market_regime_current).
ALTER TABLE market_regime_history
  ADD COLUMN IF NOT EXISTS stale_reason VARCHAR(255) NULL AFTER symbol_contributions,
  ADD COLUMN IF NOT EXISTS state_hash   CHAR(64)     NULL AFTER stale_reason;

-- 2a. Basket thresholds/interval: non-negative and correctly ordered.
ALTER TABLE market_regime_baskets
  ADD CONSTRAINT IF NOT EXISTS chk_regime_interval_nonneg CHECK (update_interval_seconds >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_regime_neutral_nonneg  CHECK (neutral_band_bps >= 0),
  ADD CONSTRAINT IF NOT EXISTS chk_regime_threshold_order CHECK (moderate_threshold_bps >= neutral_band_bps AND strong_threshold_bps >= moderate_threshold_bps);

-- 2b. Symbol weight must be strictly positive (a zero/negative weight is meaningless).
ALTER TABLE market_regime_basket_symbols
  ADD CONSTRAINT IF NOT EXISTS chk_regime_symbol_weight_pos CHECK (weight > 0);

-- 2c. Timeframe lookback + weight must be strictly positive.
ALTER TABLE market_regime_timeframes
  ADD CONSTRAINT IF NOT EXISTS chk_regime_tf_seconds_pos CHECK (seconds > 0),
  ADD CONSTRAINT IF NOT EXISTS chk_regime_tf_weight_pos  CHECK (weight > 0);
