-- migrate:ddl
-- 014_health_status_vocab
--
-- PR14 health monitor records a NORMALIZED health status (a small fixed vocabulary,
-- not adapter-invented names) and enough failure context for later dashboard/engine
-- decisions. public/private REST health are tracked SEPARATELY (a healthy public API
-- does not prove the authenticated private API is healthy). The pre-existing
-- rest_status/ws_status/api_key_status enums + counters (migration 006) are kept and
-- still updated; these add the normalized layer. DDL only, idempotent.
--
-- Status vocabulary: HEALTHY | DEGRADED | UNAVAILABLE | AUTH_FAILED | RATE_LIMITED | UNKNOWN
ALTER TABLE exchange_health_current
  ADD COLUMN IF NOT EXISTS public_status        VARCHAR(16) NOT NULL DEFAULT 'UNKNOWN' AFTER api_key_status,
  ADD COLUMN IF NOT EXISTS private_status       VARCHAR(16) NOT NULL DEFAULT 'UNKNOWN' AFTER public_status,
  ADD COLUMN IF NOT EXISTS consecutive_failures INT         NOT NULL DEFAULT 0          AFTER auth_error_count,
  ADD COLUMN IF NOT EXISTS last_error_category  VARCHAR(32) NULL                        AFTER consecutive_failures,
  ADD COLUMN IF NOT EXISTS last_error_message   VARCHAR(255) NULL                       AFTER last_error_category;
