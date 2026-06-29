-- PR24: first real canary live-run instrumentation.
--
-- A live_run_session is an explicit, operator-started canary run: it makes the first real
-- live order observable (session + checklist), correlatable (live_audit carries the
-- session/ack/preflight identity), and reversible/stoppable (stopping the active session
-- blocks new buys immediately while risk-reducing sell/cancel/status keep working). It does
-- NOT broaden live scope — it is still one exchange / one symbol / one cycle / tiny notional,
-- gated by the PR23 acknowledgement.
--
-- DDL-only file (house rule: never mix DDL + DML in one migration).
CREATE TABLE IF NOT EXISTS live_run_sessions (
  id                         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  operator                   VARCHAR(128) NOT NULL,
  exchange_id                BIGINT UNSIGNED NOT NULL,
  exchange_market_id         BIGINT UNSIGNED NOT NULL,
  canonical_symbol           VARCHAR(64) NOT NULL,
  credential_id              BIGINT UNSIGNED NULL,
  preflight_hash             CHAR(64) NOT NULL,        -- the config hash the run was started under
  acknowledgement_id         BIGINT UNSIGNED NULL,     -- the live_acknowledgements row it binds to
  caps_json                  JSON NULL,                -- snapshot of caps at start (no secrets)
  status                     VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',  -- ACTIVE | STOPPED
  start_reason               TEXT NULL,
  stop_reason                TEXT NULL,
  first_order_checklist_json JSON NULL,                -- the final pre-send checklist for the first real buy
  started_at                 TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  stopped_at                 DATETIME(6) NULL,
  KEY idx_live_session_status (status),
  KEY idx_live_session_scope (exchange_id, exchange_market_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Correlate every live-guard decision with the run session, acknowledgement, and config
-- hash that were in force when it was made.
ALTER TABLE live_audit
  ADD COLUMN IF NOT EXISTS live_session_id    BIGINT UNSIGNED NULL,
  ADD COLUMN IF NOT EXISTS acknowledgement_id BIGINT UNSIGNED NULL,
  ADD COLUMN IF NOT EXISTS preflight_hash     CHAR(64) NULL;
