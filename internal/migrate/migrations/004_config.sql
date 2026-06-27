-- migrate:ddl
-- 004_config
--
-- Versioned, dashboard-editable configuration and its audit trail. This is HOW we
-- trade (trading parameters), kept separate from exchange_markets (which is WHAT
-- the venue supports). DDL-only, idempotent.

-- Global config version registry. Each id IS a config version number; cycles
-- store the active version at signal time. The active version is the latest
-- 'active' row. Exact historical values are reconstructable from config_change_audit.
CREATE TABLE IF NOT EXISTS config_versions (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  status       ENUM('draft','active','superseded') NOT NULL DEFAULT 'active',
  created_by   VARCHAR(128) NULL,
  activated_at DATETIME(6) NULL,
  note         VARCHAR(255) NULL,
  created_at   TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_config_versions_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Auditable record of every NON-SECRET config change (who/old/new/version/time).
-- Credential changes do NOT go here — they use exchange_credential_audit (003).
-- Never store secrets in old_value/new_value.
CREATE TABLE IF NOT EXISTS config_change_audit (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  config_version BIGINT UNSIGNED NULL,
  entity_type    VARCHAR(64) NOT NULL,   -- 'symbol_config','exchange_config','exchange_market',...
  entity_id      BIGINT UNSIGNED NULL,
  field          VARCHAR(128) NULL,
  old_value      TEXT NULL,
  new_value      TEXT NULL,
  changed_by     VARCHAR(128) NULL,
  reason         VARCHAR(255) NULL,
  activated_at   DATETIME(6) NULL,
  created_at     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_cfgaudit_entity (entity_type, entity_id),
  KEY idx_cfgaudit_version (config_version),
  KEY idx_cfgaudit_created (created_at),
  CONSTRAINT fk_cfgaudit_version FOREIGN KEY (config_version) REFERENCES config_versions (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Per-exchange-market TRADING configuration (the owner's strategy parameters).
-- One row per exchange_market. buy_size_unit declares whether buy_size is in base
-- or quote units (set by the owner's strategy).
CREATE TABLE IF NOT EXISTS symbol_configs (
  id                       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_market_id       BIGINT UNSIGNED NOT NULL,
  min_spread_bps           INT NULL,
  buy_size                 DECIMAL(36,18) NULL,
  buy_size_unit            ENUM('base','quote') NOT NULL DEFAULT 'quote',
  sell_offset_bps          INT NULL,
  reprice_interval_seconds INT NULL,
  order_timeout_ms         INT NULL,
  max_retries              INT NULL,
  retry_backoff_ms         INT NULL,
  config_version           BIGINT UNSIGNED NULL,
  created_at               TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at               TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_symbol_config (exchange_market_id),
  KEY idx_symbol_config_version (config_version),
  CONSTRAINT fk_symbolcfg_market  FOREIGN KEY (exchange_market_id) REFERENCES exchange_markets (id),
  CONSTRAINT fk_symbolcfg_version FOREIGN KEY (config_version)     REFERENCES config_versions (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Per-exchange operational configuration. max_concurrent_requests is the
-- per-exchange concurrency limit enforced by the order-executor queue (PR7).
CREATE TABLE IF NOT EXISTS exchange_configs (
  id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_id             BIGINT UNSIGNED NOT NULL,
  max_concurrent_requests INT NOT NULL DEFAULT 1,
  request_timeout_ms      INT NULL,
  max_retries             INT NULL,
  retry_backoff_ms        INT NULL,
  rate_limit_per_sec      INT NULL,
  config_version          BIGINT UNSIGNED NULL,
  created_at              TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at              TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_exchange_config (exchange_id),
  KEY idx_exchange_config_version (config_version),
  CONSTRAINT fk_exchangecfg_exchange FOREIGN KEY (exchange_id)    REFERENCES exchanges (id),
  CONSTRAINT fk_exchangecfg_version  FOREIGN KEY (config_version) REFERENCES config_versions (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Fee schedule per exchange (and optionally per market). exchange_market_id NULL
-- means an exchange-wide default. Each cycle stores its own fee snapshot (005)
-- because fees may change later.
CREATE TABLE IF NOT EXISTS exchange_fees (
  id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_id        BIGINT UNSIGNED NOT NULL,
  exchange_market_id BIGINT UNSIGNED NULL,
  maker_fee          DECIMAL(18,8) NOT NULL,
  taker_fee          DECIMAL(18,8) NOT NULL,
  effective_from     DATETIME(6) NULL,
  config_version     BIGINT UNSIGNED NULL,
  created_at         TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at         TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  KEY idx_fees_exchange (exchange_id, exchange_market_id),
  KEY idx_fees_version (config_version),
  CONSTRAINT fk_fees_exchange FOREIGN KEY (exchange_id)        REFERENCES exchanges (id),
  CONSTRAINT fk_fees_market   FOREIGN KEY (exchange_market_id) REFERENCES exchange_markets (id),
  CONSTRAINT fk_fees_version  FOREIGN KEY (config_version)     REFERENCES config_versions (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Retention policy for high-volume DB tables (operational retention config). The
-- retention-worker (PR18) reads this. retention_days is required; max_rows /
-- max_total_bytes are optional size/volume caps. (File-log rotation is NOT here —
-- it belongs in bootstrap config because file logging precedes DB availability.)
CREATE TABLE IF NOT EXISTS retention_settings (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  table_name      VARCHAR(64) NOT NULL,
  retention_days  INT NOT NULL,
  max_rows        BIGINT UNSIGNED NULL,
  max_total_bytes BIGINT UNSIGNED NULL,
  enabled         TINYINT(1) NOT NULL DEFAULT 1,
  config_version  BIGINT UNSIGNED NULL,
  created_at      TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at      TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_retention_table (table_name),
  CONSTRAINT fk_retention_version FOREIGN KEY (config_version) REFERENCES config_versions (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
