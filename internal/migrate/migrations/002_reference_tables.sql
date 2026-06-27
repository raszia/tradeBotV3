-- migrate:ddl
-- 002_reference_tables
--
-- Reference data for venues, assets, canonical markets, and per-exchange market
-- listings (the "market discovery" storage). These describe WHAT exists and what
-- the exchange supports — NOT how we trade it (that lives in symbol_configs, PR
-- migration 004). DDL-only, idempotent.

-- Exchanges (venues). Binance is the price reference; Iranian venues are where we
-- buy/sell. `enabled` is operational config (toggled from the dashboard).
CREATE TABLE IF NOT EXISTS exchanges (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  code       VARCHAR(64)  NOT NULL,                 -- 'binance','nobitex','wallex',...
  name       VARCHAR(128) NOT NULL,
  category   ENUM('binance','iranian','other') NOT NULL DEFAULT 'iranian',
  enabled    TINYINT(1)   NOT NULL DEFAULT 0,
  created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_exchanges_code (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Assets (currencies). Both crypto (BTC, USDT) and fiat (IRT, IRR).
CREATE TABLE IF NOT EXISTS assets (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  symbol     VARCHAR(32) NOT NULL,                  -- 'BTC','USDT','IRT','IRR'
  name       VARCHAR(64) NULL,
  kind       ENUM('crypto','fiat') NOT NULL DEFAULT 'crypto',
  created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_assets_symbol (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Canonical markets (exchange-agnostic). canonical_symbol is BASE/QUOTE, e.g.
-- 'BTC/USDT' or 'BTC/IRT'. quote_asset_type lets us treat USDT and rial markets
-- uniformly while knowing which is which.
CREATE TABLE IF NOT EXISTS markets (
  id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  canonical_symbol VARCHAR(64) NOT NULL,            -- 'BTC/USDT'
  base_asset_id    BIGINT UNSIGNED NOT NULL,
  quote_asset_id   BIGINT UNSIGNED NOT NULL,
  quote_asset_type ENUM('USDT','IRT','IRR','OTHER') NOT NULL,
  created_at       TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at       TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_markets_canonical (canonical_symbol),
  UNIQUE KEY uq_markets_base_quote (base_asset_id, quote_asset_id),
  KEY idx_markets_quote_type (quote_asset_type),
  CONSTRAINT fk_markets_base  FOREIGN KEY (base_asset_id)  REFERENCES assets (id),
  CONSTRAINT fk_markets_quote FOREIGN KEY (quote_asset_id) REFERENCES assets (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Exchange markets: per-exchange listing discovered from each venue. Holds the
-- exchange-native symbol, mapping to a canonical market, the venue's trading
-- RULES (precisions, mins, tick/step, fees), venue status, raw metadata, and the
-- four per-symbol ENABLE flags.
--
-- Mapping rule (item 7): market_id is the authoritative link to the canonical
-- market; canonical_symbol is denormalized for convenient queries and MUST equal
-- markets.canonical_symbol whenever market_id is set. market_id/base/quote may be
-- NULL right after discovery until the symbol is mapped.
--
-- Per-symbol enable flags (item 14 — behavior documented in PROJECT_ARCHITECTURE.md,
-- enforced in later PRs):
--   enabled_for_collection  - collect this market's book/price into Redis
--   enabled_for_signal      - evaluate signals on it
--   enabled_for_trading     - allowed to START NEW buy cycles (disabling stops
--                             new trades but must NOT abandon open cycles)
--   enabled_for_sell_manage - manage existing sell/reprice/cancel; defaults TRUE
--                             so an open cycle is always managed to resolution
CREATE TABLE IF NOT EXISTS exchange_markets (
  id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_id             BIGINT UNSIGNED NOT NULL,
  market_id               BIGINT UNSIGNED NULL,
  exchange_symbol         VARCHAR(64) NOT NULL,         -- venue-native symbol string
  canonical_symbol        VARCHAR(64) NOT NULL,         -- denormalized 'BTC/USDT'
  base_asset_id           BIGINT UNSIGNED NULL,
  quote_asset_id          BIGINT UNSIGNED NULL,
  quote_asset_type        ENUM('USDT','IRT','IRR','OTHER') NOT NULL DEFAULT 'OTHER',
  price_precision         SMALLINT NULL,
  quantity_precision      SMALLINT NULL,
  min_order_amount        DECIMAL(36,8)  NULL,          -- min notional in quote
  min_order_quantity      DECIMAL(36,18) NULL,          -- min base quantity
  tick_size               DECIMAL(36,18) NULL,
  step_size               DECIMAL(36,18) NULL,
  maker_fee               DECIMAL(18,8)  NULL,
  taker_fee               DECIMAL(18,8)  NULL,
  deposit_status          ENUM('unknown','enabled','disabled') NOT NULL DEFAULT 'unknown',
  withdraw_status         ENUM('unknown','enabled','disabled') NOT NULL DEFAULT 'unknown',
  tradable_status         ENUM('unknown','tradable','halted')  NOT NULL DEFAULT 'unknown',
  enabled_for_collection  TINYINT(1) NOT NULL DEFAULT 0,
  enabled_for_signal      TINYINT(1) NOT NULL DEFAULT 0,
  enabled_for_trading     TINYINT(1) NOT NULL DEFAULT 0,
  enabled_for_sell_manage TINYINT(1) NOT NULL DEFAULT 1,
  raw_metadata            JSON NULL,
  last_discovery_at       DATETIME(6) NULL,
  created_at              TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at              TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_exmarket (exchange_id, exchange_symbol),
  KEY idx_exmarket_canonical (exchange_id, canonical_symbol),
  KEY idx_exmarket_market (market_id),
  KEY idx_exmarket_symbol (canonical_symbol),
  KEY idx_exmarket_trading (exchange_id, enabled_for_trading),
  CONSTRAINT fk_exmarket_exchange FOREIGN KEY (exchange_id)   REFERENCES exchanges (id),
  CONSTRAINT fk_exmarket_market   FOREIGN KEY (market_id)     REFERENCES markets (id),
  CONSTRAINT fk_exmarket_base     FOREIGN KEY (base_asset_id) REFERENCES assets (id),
  CONSTRAINT fk_exmarket_quote    FOREIGN KEY (quote_asset_id) REFERENCES assets (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
