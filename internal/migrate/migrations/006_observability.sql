-- migrate:ddl
-- 006_observability
--
-- Balances, exchange health, and high-volume log/event tables.
--
-- DESIGN DECISION (item 9 — high-volume tables): api_call_logs, app_logs,
-- comparison_events, exchange_health_samples, and wallet_balance_history are
-- written at high rate (async/batched) and pruned by the retention-worker. They
-- are intentionally created WITHOUT foreign keys and start NON-PARTITIONED with a
-- strong created_at index (so retention is simple batched DELETEs and inserts stay
-- cheap). Partitioning may be added later if volume requires it; the created_at
-- index keeps that migration non-breaking. Current-state tables and `signals`
-- keep foreign keys.

-- Latest balance per (exchange, asset). balance_hash supports duplicate-prevention
-- of history rows (item 13): only insert a history row when the hash changes.
CREATE TABLE IF NOT EXISTS wallet_balances_current (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_id  BIGINT UNSIGNED NOT NULL,
  asset        VARCHAR(32) NOT NULL,
  available    DECIMAL(36,18) NOT NULL DEFAULT 0,
  locked       DECIMAL(36,18) NOT NULL DEFAULT 0,
  total        DECIMAL(36,18) NOT NULL DEFAULT 0,
  balance_hash CHAR(64) NULL,
  updated_at   TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_balance_current (exchange_id, asset),
  CONSTRAINT fk_balcur_exchange FOREIGN KEY (exchange_id) REFERENCES exchanges (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Balance history (append-only; high-volume; no FK; retention via created_at). A
-- row is inserted only when the balance hash differs from the current row, so the
-- same hash recurring later is allowed (no UNIQUE on hash).
CREATE TABLE IF NOT EXISTS wallet_balance_history (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_id  BIGINT UNSIGNED NOT NULL,
  asset        VARCHAR(32) NOT NULL,
  available    DECIMAL(36,18) NOT NULL,
  locked       DECIMAL(36,18) NOT NULL,
  total        DECIMAL(36,18) NOT NULL,
  balance_hash CHAR(64) NOT NULL,
  created_at   TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_balhist_ex_asset (exchange_id, asset, created_at),
  KEY idx_balhist_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Current per-exchange health snapshot (item 14). One row per exchange.
CREATE TABLE IF NOT EXISTS exchange_health_current (
  id                     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_id            BIGINT UNSIGNED NOT NULL,
  api_key_status         ENUM('unknown','ok','invalid','disabled') NOT NULL DEFAULT 'unknown',
  rest_status            ENUM('unknown','up','down') NOT NULL DEFAULT 'unknown',
  ws_status              ENUM('unknown','up','down') NOT NULL DEFAULT 'unknown',
  latency_ms             INT NULL,
  timeout_count          BIGINT UNSIGNED NOT NULL DEFAULT 0,
  error_count            BIGINT UNSIGNED NOT NULL DEFAULT 0,
  rate_limit_error_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  auth_error_count       BIGINT UNSIGNED NOT NULL DEFAULT 0,
  last_success_at        DATETIME(6) NULL,
  last_failure_at        DATETIME(6) NULL,
  updated_at             TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_health_current (exchange_id),
  CONSTRAINT fk_healthcur_exchange FOREIGN KEY (exchange_id) REFERENCES exchanges (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- High-volume health samples (no FK; retention via created_at).
CREATE TABLE IF NOT EXISTS exchange_health_samples (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_id BIGINT UNSIGNED NOT NULL,
  sample_type VARCHAR(32) NULL,                  -- 'rest','ws','auth',...
  ok          TINYINT(1) NULL,
  latency_ms  INT NULL,
  error       TEXT NULL,
  created_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_healthsample_ex (exchange_id, created_at),
  KEY idx_healthsample_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Raw API request/response logs (item 11). High-volume; no FK; retention via
-- created_at. SECRETS (api keys/secrets/signatures/auth headers/tokens) MUST be
-- masked or omitted BEFORE insert — headers/bodies stored here are already
-- sanitized by the writer (PR4/PR7). `timeout` flags a client-side timeout.
CREATE TABLE IF NOT EXISTS api_call_logs (
  id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_id      BIGINT UNSIGNED NULL,
  cycle_id         BIGINT UNSIGNED NULL,
  order_id         BIGINT UNSIGNED NULL,
  request_id       VARCHAR(64) NULL,
  method           VARCHAR(8) NULL,
  url              TEXT NULL,
  request_headers  JSON NULL,                    -- masked
  request_body     MEDIUMTEXT NULL,              -- masked/truncated
  response_status  INT NULL,
  response_headers JSON NULL,                    -- masked
  response_body    MEDIUMTEXT NULL,              -- masked/truncated
  latency_ms       INT NULL,
  error            TEXT NULL,
  timeout          TINYINT(1) NOT NULL DEFAULT 0,
  created_at       TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_apilog_ex (exchange_id, created_at),
  KEY idx_apilog_created (created_at),
  KEY idx_apilog_cycle (cycle_id),
  KEY idx_apilog_order (order_id),
  KEY idx_apilog_request (request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Application logs persisted to DB (operational logs go to DB, not files; item 11).
-- High-volume; no FK; retention via created_at.
-- Note: `binary` is a reserved word in MariaDB, so the producing binary's name is
-- stored as source_binary.
CREATE TABLE IF NOT EXISTS app_logs (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  level         VARCHAR(16) NOT NULL,
  source_binary VARCHAR(64) NULL,
  message       TEXT NOT NULL,
  fields        JSON NULL,
  cycle_id      BIGINT UNSIGNED NULL,
  order_id      BIGINT UNSIGNED NULL,
  created_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_applog_created (created_at),
  KEY idx_applog_level (level, created_at),
  KEY idx_applog_binary (source_binary, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Comparison events: one per price comparison (very high volume). No FK; retention
-- via created_at. `passed` marks whether it met the threshold (became a signal).
CREATE TABLE IF NOT EXISTS comparison_events (
  id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_id             BIGINT UNSIGNED NULL,
  canonical_symbol        VARCHAR(64) NOT NULL,
  binance_price           DECIMAL(36,12) NULL,
  iranian_price           DECIMAL(36,12) NULL,
  spread_bps              INT NULL,
  fee_adjusted_spread_bps INT NULL,
  passed                  TINYINT(1) NOT NULL DEFAULT 0,
  config_version          BIGINT UNSIGNED NULL,
  created_at              TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_cmp_symbol (canonical_symbol, created_at),
  KEY idx_cmp_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Signals: written only when a comparison passes the threshold (moderate volume;
-- kept). FK to cycles (nullable: a signal is recorded before/whether or not a
-- cycle is created).
CREATE TABLE IF NOT EXISTS signals (
  id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  cycle_id                BIGINT UNSIGNED NULL,
  exchange_id             BIGINT UNSIGNED NULL,
  canonical_symbol        VARCHAR(64) NOT NULL,
  binance_price           DECIMAL(36,12) NULL,
  iranian_price           DECIMAL(36,12) NULL,
  spread_bps              INT NULL,
  fee_adjusted_spread_bps INT NULL,
  buy_size                DECIMAL(36,18) NULL,
  regime_direction        VARCHAR(16) NULL,
  regime_level            VARCHAR(16) NULL,
  regime_confidence       DECIMAL(9,6) NULL,
  config_version          BIGINT UNSIGNED NULL,
  accepted                TINYINT(1) NOT NULL DEFAULT 0,
  reject_reason           VARCHAR(255) NULL,
  signal_time             DATETIME(6) NULL,
  created_at              TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_signals_symbol (canonical_symbol, created_at),
  KEY idx_signals_cycle (cycle_id),
  KEY idx_signals_created (created_at),
  CONSTRAINT fk_signals_cycle FOREIGN KEY (cycle_id) REFERENCES cycles (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
