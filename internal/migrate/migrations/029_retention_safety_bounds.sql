-- migrate:ddl
-- 029_retention_safety_bounds
--
-- PR18 correction: DB-level safe bounds for retention_settings (defence in depth; the
-- retention worker also validates these at runtime and skips any out-of-range table without
-- deleting). Bounds match internal/retention's runtime constants:
--   retention_days      : NULL (not configured) OR 1..3650 (≤10 years)
--   batch_size          : 1..50000   (a bounded single DELETE ... LIMIT)
--   max_batches_per_run : 1..10000   (per-run work ceiling)
--   pause_ms            : 0..60000   (≤60s between batches)
-- A CHECK against NULL is "unknown" → passes, so a NULL retention_days keeps its
-- "not configured" meaning. DDL only, idempotent.
ALTER TABLE retention_settings
  ADD CONSTRAINT IF NOT EXISTS chk_ret_days     CHECK (retention_days IS NULL OR (retention_days >= 1 AND retention_days <= 3650)),
  ADD CONSTRAINT IF NOT EXISTS chk_ret_batch    CHECK (batch_size >= 1 AND batch_size <= 50000),
  ADD CONSTRAINT IF NOT EXISTS chk_ret_maxbatch CHECK (max_batches_per_run >= 1 AND max_batches_per_run <= 10000),
  ADD CONSTRAINT IF NOT EXISTS chk_ret_pause    CHECK (pause_ms >= 0 AND pause_ms <= 60000);
