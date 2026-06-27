-- migrate:ddl
-- 001_app_meta
--
-- Minimal infrastructure metadata table holding key/value application metadata
-- (e.g. a schema bootstrap marker or build info). This is NOT trading data.
--
-- It exists in PR1 so the migration runner has a real, end-to-end migration to
-- apply and so the runner can be integration-tested against a fresh database.
-- The full trading schema (cycles, orders, fills, exchange_requests, ...) lands
-- in PR2 as migrations 002+.
--
-- DDL migration: statements auto-commit; uses IF NOT EXISTS so a re-run after a
-- partial failure is safe.
CREATE TABLE IF NOT EXISTS app_meta (
  k          VARCHAR(191) NOT NULL PRIMARY KEY,
  v          TEXT NOT NULL,
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
