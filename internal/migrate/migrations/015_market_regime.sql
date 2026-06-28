-- migrate:ddl
-- 015_market_regime
--
-- PR15 market-regime calculation. Baskets of Binance symbols (read from Redis, never
-- called directly) are scored across configurable timeframes/weights/thresholds into a
-- normalized regime (direction/level/confidence). All DB-configurable — nothing is
-- hardcoded. Current state is upserted; history is appended (for audit/analysis).
-- DDL only, idempotent.

-- A regime basket: a named, weighted set of symbols + timeframes + thresholds.
CREATE TABLE IF NOT EXISTS market_regime_baskets (
  id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name                    VARCHAR(64) NOT NULL,
  enabled                 TINYINT(1) NOT NULL DEFAULT 1,
  update_interval_seconds INT NOT NULL DEFAULT 60,
  neutral_band_bps        INT NOT NULL DEFAULT 5,    -- |score| <= this => NEUTRAL / FLAT
  moderate_threshold_bps  INT NOT NULL DEFAULT 30,   -- |score| >= this => MODERATE
  strong_threshold_bps    INT NOT NULL DEFAULT 100,  -- |score| >= this => STRONG
  config_version          BIGINT UNSIGNED NULL,
  created_at              TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at              TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_regime_basket (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Symbols (Binance canonical, e.g. BTC/USDT) + weights per basket.
CREATE TABLE IF NOT EXISTS market_regime_basket_symbols (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  basket_id      BIGINT UNSIGNED NOT NULL,
  binance_symbol VARCHAR(64) NOT NULL,
  weight         DECIMAL(18,8) NOT NULL DEFAULT 1,
  enabled        TINYINT(1) NOT NULL DEFAULT 1,
  UNIQUE KEY uq_regime_symbol (basket_id, binance_symbol),
  KEY idx_regime_symbol_basket (basket_id),
  CONSTRAINT fk_regime_symbol_basket FOREIGN KEY (basket_id) REFERENCES market_regime_baskets (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Timeframes (lookback windows) + weights per basket.
CREATE TABLE IF NOT EXISTS market_regime_timeframes (
  id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  basket_id BIGINT UNSIGNED NOT NULL,
  label     VARCHAR(16) NOT NULL,    -- e.g. '5m','1h','4h'
  seconds   INT NOT NULL,            -- lookback window in seconds
  weight    DECIMAL(18,8) NOT NULL DEFAULT 1,
  UNIQUE KEY uq_regime_tf (basket_id, label),
  KEY idx_regime_tf_basket (basket_id),
  CONSTRAINT fk_regime_tf_basket FOREIGN KEY (basket_id) REFERENCES market_regime_baskets (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Current regime per basket (upserted).
CREATE TABLE IF NOT EXISTS market_regime_current (
  id                   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  basket_id            BIGINT UNSIGNED NOT NULL,
  direction            VARCHAR(16) NOT NULL DEFAULT 'UNKNOWN',
  level                VARCHAR(16) NOT NULL DEFAULT 'UNKNOWN',
  confidence           DECIMAL(9,6) NOT NULL DEFAULT 0,
  score_bps            DECIMAL(18,6) NULL,
  timeframe_scores     JSON NULL,
  symbol_contributions JSON NULL,
  stale_reason         VARCHAR(255) NULL,
  config_version       BIGINT UNSIGNED NULL,
  computed_at          DATETIME(6) NULL,
  updated_at           TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_regime_current (basket_id),
  CONSTRAINT fk_regime_current_basket FOREIGN KEY (basket_id) REFERENCES market_regime_baskets (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Regime history (append-only, deduped on change). High-volume → timestamp-indexed,
-- no FK on the write path (retention friendly).
CREATE TABLE IF NOT EXISTS market_regime_history (
  id                   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  basket_id            BIGINT UNSIGNED NOT NULL,
  direction            VARCHAR(16) NOT NULL,
  level                VARCHAR(16) NOT NULL,
  confidence           DECIMAL(9,6) NOT NULL,
  score_bps            DECIMAL(18,6) NULL,
  timeframe_scores     JSON NULL,
  symbol_contributions JSON NULL,
  config_version       BIGINT UNSIGNED NULL,
  created_at           TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_regime_hist_basket (basket_id, created_at),
  KEY idx_regime_hist_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
