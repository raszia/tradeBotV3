-- migrate:ddl
-- 020_live_controls
--
-- PR20 LIMITED LIVE execution: real orders only under strict, explicit caps + a global
-- kill switch + per-exchange/per-symbol live flags + an audit trail. Safe by default:
-- the kill switch defaults ENGAGED (1) and every cap defaults NULL ("not configured" =>
-- live trading does NOT start). Real credential decryption + real-adapter wiring is a
-- separate PR (PR20a); until then live mode has no real client and sends nothing.
-- DDL only, idempotent.

-- Global live controls (singleton row id=1). NULL caps => not configured => no live.
CREATE TABLE IF NOT EXISTS live_controls (
  id                        TINYINT UNSIGNED NOT NULL PRIMARY KEY DEFAULT 1,
  kill_switch               TINYINT(1) NOT NULL DEFAULT 1,   -- engaged by default (safe)
  max_open_cycles           INT NULL,
  max_daily_orders          INT NULL,
  max_daily_quote           DECIMAL(36,8) NULL,
  max_order_notional        DECIMAL(36,8) NULL,
  max_base_qty              DECIMAL(36,18) NULL,
  max_consecutive_failures  INT NULL,
  max_unresolved_reconcile  INT NULL,
  enabled_by                VARCHAR(128) NULL,
  config_version            BIGINT UNSIGNED NULL,
  updated_at                TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Per-exchange and per-symbol live-enable flags (default OFF — allowed scope is opt-in).
ALTER TABLE exchanges
  ADD COLUMN IF NOT EXISTS live_enabled TINYINT(1) NOT NULL DEFAULT 0 AFTER enabled;
ALTER TABLE exchange_markets
  ADD COLUMN IF NOT EXISTS live_enabled TINYINT(1) NOT NULL DEFAULT 0 AFTER enabled_for_sell_manage;

-- Audit of every live mutating decision (allow or deny), no secrets.
CREATE TABLE IF NOT EXISTS live_audit (
  id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_id        BIGINT UNSIGNED NULL,
  exchange_market_id BIGINT UNSIGNED NULL,
  cycle_id           BIGINT UNSIGNED NULL,
  order_id           BIGINT UNSIGNED NULL,
  request_id         BIGINT UNSIGNED NULL,
  action             VARCHAR(32) NOT NULL,         -- 'place_buy','place_sell','cancel','new_cycle'
  side               VARCHAR(8) NULL,
  notional           DECIMAL(36,8) NULL,
  decision           VARCHAR(8) NOT NULL,          -- 'allow' | 'deny'
  reason             VARCHAR(255) NULL,
  execution_mode     VARCHAR(16) NOT NULL,
  config_version     BIGINT UNSIGNED NULL,
  created_at         TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_live_audit_created (created_at),
  KEY idx_live_audit_cycle (cycle_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
