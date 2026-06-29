-- PR23: live preflight + canary rollout controls.
--
-- Extends live_controls with freshness windows + canary scope + an ack requirement, and
-- adds live_acknowledgements (the explicit operator sign-off that binds to a preflight
-- hash). Safe by default: require_canary_ack defaults to 1, so a live BUY needs a current
-- acknowledgement bound to the present config — even when credentials/caps/live flags exist.
--
-- DDL-only file (house rule: never mix DDL + DML in one migration).
ALTER TABLE live_controls
  -- when 1, a live buy requires a current acknowledgement for the canary exchange/symbol
  ADD COLUMN IF NOT EXISTS require_canary_ack TINYINT(1) NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS canary_exchange_id BIGINT UNSIGNED NULL,
  ADD COLUMN IF NOT EXISTS canary_market_id   BIGINT UNSIGNED NULL,
  -- freshness windows for preflight (NULL -> a safe built-in default is used)
  ADD COLUMN IF NOT EXISTS credential_validation_max_age_minutes INT NULL,
  ADD COLUMN IF NOT EXISTS market_data_max_age_seconds INT NULL,
  ADD COLUMN IF NOT EXISTS balance_max_age_minutes INT NULL,
  ADD COLUMN IF NOT EXISTS dry_run_success_max_age_minutes INT NULL,
  -- an acknowledgement older than this is EXPIRED: a config-only hash cannot catch
  -- conditions that rot with time, so the ack itself ages out and the guard re-checks the
  -- dynamic safety conditions before every live buy (NULL -> a safe built-in default)
  ADD COLUMN IF NOT EXISTS canary_ack_max_age_minutes INT NULL,
  -- when 1, private health must be OK to pass preflight; when 0, an unhealthy private
  -- probe is a WARNING the operator accepts (still recorded)
  ADD COLUMN IF NOT EXISTS health_required TINYINT(1) NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS live_acknowledgements (
  id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  operator           VARCHAR(128) NOT NULL,
  exchange_id        BIGINT UNSIGNED NOT NULL,
  exchange_market_id BIGINT UNSIGNED NOT NULL,
  canonical_symbol   VARCHAR(64) NOT NULL,
  credential_id      BIGINT UNSIGNED NULL,           -- the active credential at ack time
  preflight_hash     CHAR(64) NOT NULL,              -- binds the ack to the config-relevant preflight result
  caps_json          JSON NULL,                      -- snapshot of caps at ack time (no secrets)
  reason             TEXT NULL,
  active             TINYINT(1) NOT NULL DEFAULT 1,  -- a newer ack / config change supersedes it
  acknowledged_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  revoked_at         DATETIME(6) NULL,
  KEY idx_live_ack_active (exchange_id, exchange_market_id, active),
  KEY idx_live_ack_created (acknowledged_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
