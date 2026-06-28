-- migrate:ddl
-- 016_regime_state_hash
--
-- PR15 clarification: regime history must capture the FULL regime evolution, not only
-- direction/level label changes (e.g. BULLISH/STRONG @ confidence 0.35 is not the same
-- as @ 0.90, and UNKNOWN with a changed stale_reason is a meaningful change too).
-- state_hash is a content hash over ALL important output fields (direction, level,
-- confidence, score, per-timeframe scores, per-symbol contributions, stale_reason,
-- config_version); a history row is appended whenever it changes. DDL only, idempotent.
ALTER TABLE market_regime_current
  ADD COLUMN IF NOT EXISTS state_hash CHAR(64) NULL AFTER stale_reason;
