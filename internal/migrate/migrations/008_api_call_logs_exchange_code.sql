-- migrate:ddl
-- 008_api_call_logs_exchange_code
--
-- The raw API IO logger (internal/exchanges) attributes each log to an exchange
-- by its code (the numeric exchange_id is not always known at the HTTP layer).
-- Add a nullable exchange_code column + index for triage. DDL, idempotent.
ALTER TABLE api_call_logs
  ADD COLUMN IF NOT EXISTS exchange_code VARCHAR(64) NULL AFTER exchange_id;

CREATE INDEX IF NOT EXISTS idx_apilog_exchange_code ON api_call_logs (exchange_code, created_at);
