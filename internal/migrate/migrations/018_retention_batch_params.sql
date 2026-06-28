-- migrate:ddl
-- 018_retention_batch_params
--
-- PR18 retention worker deletes old rows from high-volume operational tables in BOUNDED
-- BATCHES (never one huge delete) to avoid long locks / replication pain. These
-- per-table knobs live on retention_settings alongside enabled + retention_days.
-- retention_days is never defaulted by the worker — a missing/zero value means "not
-- configured; do nothing". DDL only, idempotent.
ALTER TABLE retention_settings
  ADD COLUMN IF NOT EXISTS batch_size          INT NOT NULL DEFAULT 1000 AFTER enabled,
  ADD COLUMN IF NOT EXISTS max_batches_per_run INT NOT NULL DEFAULT 100  AFTER batch_size,
  ADD COLUMN IF NOT EXISTS pause_ms            INT NOT NULL DEFAULT 0    AFTER max_batches_per_run;
