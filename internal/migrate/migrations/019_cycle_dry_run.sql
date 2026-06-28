-- migrate:ddl
-- 019_cycle_dry_run
--
-- PR19 dry-run trading mode runs the full lifecycle through the real queue/executor/
-- order-processing boundaries against a SIMULATED exchange client (no real orders). A
-- dry-run cycle is marked here so the dashboard can label it DRY_RUN and the reconciler
-- never confuses a simulated order with a real exchange order. Default 0 (live/real
-- semantics) so existing rows are unaffected. DDL only, idempotent.
ALTER TABLE cycles
  ADD COLUMN IF NOT EXISTS dry_run TINYINT(1) NOT NULL DEFAULT 0 AFTER state;
