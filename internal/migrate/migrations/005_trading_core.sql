-- migrate:ddl
-- 005_trading_core
--
-- The trading source of truth: cycles, orders, fills, their state-event logs, the
-- DB-backed symbol lock, the exchange-request queue, and per-cycle fee snapshots.
-- DDL-only, idempotent. State columns are driven ONLY by the state machine (PR3);
-- `version` columns provide optimistic concurrency (CAS) on every transition.

-- Trading cycles. One cycle = one buy-then-sell arbitrage attempt. Carries the
-- config_version used at signal time and a full regime SNAPSHOT (item 17) so a
-- report can show what the market regime was and how it affected the decision.
-- The owning lock is found via symbol_locks.cycle_id (no cycles.lock_id, to avoid
-- a circular FK).
CREATE TABLE IF NOT EXISTS cycles (
  id                     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_market_id     BIGINT UNSIGNED NOT NULL,           -- the Iranian buy market
  buy_exchange_id        BIGINT UNSIGNED NOT NULL,
  canonical_symbol       VARCHAR(64) NOT NULL,
  state                  VARCHAR(48) NOT NULL DEFAULT 'NEW',
  version                BIGINT UNSIGNED NOT NULL DEFAULT 0,  -- optimistic-concurrency token
  config_version         BIGINT UNSIGNED NULL,
  -- signal context (item 16 reporting):
  signal_time            DATETIME(6) NULL,
  binance_price_at_signal  DECIMAL(36,12) NULL,
  iranian_price_at_signal  DECIMAL(36,12) NULL,
  spread_bps             INT NULL,
  fee_adjusted_spread_bps INT NULL,
  buy_size               DECIMAL(36,18) NULL,
  -- market regime snapshot at signal time (item 17):
  regime_basket_id       BIGINT UNSIGNED NULL,
  regime_direction       VARCHAR(16) NULL,
  regime_level           VARCHAR(16) NULL,
  regime_confidence      DECIMAL(9,6) NULL,
  regime_basket_score    DECIMAL(18,8) NULL,
  regime_config_version  BIGINT UNSIGNED NULL,
  regime_snapshot        JSON NULL,                          -- per-timeframe & per-symbol contributions
  regime_at              DATETIME(6) NULL,
  -- lifecycle:
  fail_reason            VARCHAR(255) NULL,
  opened_at              DATETIME(6) NULL,
  closed_at              DATETIME(6) NULL,
  created_at             TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at             TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  KEY idx_cycles_state (state),                              -- reconciler: open-cycle scan
  KEY idx_cycles_market (exchange_market_id),
  KEY idx_cycles_symbol (canonical_symbol),
  KEY idx_cycles_created (created_at),
  CONSTRAINT fk_cycles_market   FOREIGN KEY (exchange_market_id) REFERENCES exchange_markets (id),
  CONSTRAINT fk_cycles_exchange FOREIGN KEY (buy_exchange_id)    REFERENCES exchanges (id),
  CONSTRAINT fk_cycles_version  FOREIGN KEY (config_version)     REFERENCES config_versions (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Every cycle state transition (append-only). UNIQUE(cycle_id, version) makes a
-- replayed transition a no-op rather than a duplicate event.
CREATE TABLE IF NOT EXISTS cycle_state_events (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  cycle_id     BIGINT UNSIGNED NOT NULL,
  event_type   VARCHAR(64) NOT NULL,
  from_state   VARCHAR(48) NULL,
  to_state     VARCHAR(48) NOT NULL,
  version      BIGINT UNSIGNED NOT NULL,
  message      TEXT NULL,
  payload_json JSON NULL,
  created_at   TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_cycle_events_cycle (cycle_id, id),
  UNIQUE KEY uq_cycle_event_version (cycle_id, version),
  CONSTRAINT fk_cycle_events_cycle FOREIGN KEY (cycle_id) REFERENCES cycles (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Orders. Every order has an internal local_client_order_id (UNIQUE) and is
-- REGISTERED here before any exchange request to place it is enqueued. The id
-- actually sent to the exchange is client_order_id_sent and is set ONLY when the
-- exchange supports client order ids (some Iranian venues do not). order_type and
-- time_in_force are owned by the strategy (no IOC is imposed here).
CREATE TABLE IF NOT EXISTS orders (
  id                    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  cycle_id              BIGINT UNSIGNED NOT NULL,
  exchange_id           BIGINT UNSIGNED NOT NULL,
  exchange_market_id    BIGINT UNSIGNED NOT NULL,
  side                  ENUM('buy','sell') NOT NULL,
  role                  ENUM('entry_buy','exit_sell') NOT NULL,
  local_client_order_id VARCHAR(64) NOT NULL,
  client_order_id_sent  VARCHAR(64) NULL,
  exchange_order_id     VARCHAR(128) NULL,
  state                 VARCHAR(48) NOT NULL DEFAULT 'NEW',
  version               BIGINT UNSIGNED NOT NULL DEFAULT 0,
  order_type            VARCHAR(16) NOT NULL DEFAULT 'limit',
  time_in_force         VARCHAR(16) NULL,
  limit_price           DECIMAL(36,12) NULL,
  quantity              DECIMAL(36,18) NOT NULL,
  filled_quantity       DECIMAL(36,18) NOT NULL DEFAULT 0,
  avg_fill_price        DECIMAL(36,12) NULL,
  quote_spent           DECIMAL(36,8) NULL,
  fee_amount            DECIMAL(36,18) NULL,
  fee_asset             VARCHAR(32) NULL,
  reject_reason         VARCHAR(255) NULL,
  created_at            TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at            TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_orders_local_coid (local_client_order_id),
  KEY idx_orders_cycle (cycle_id),
  KEY idx_orders_exchange_oid (exchange_id, exchange_order_id),
  KEY idx_orders_state (state),
  KEY idx_orders_market (exchange_market_id),
  CONSTRAINT fk_orders_cycle    FOREIGN KEY (cycle_id)           REFERENCES cycles (id),
  CONSTRAINT fk_orders_exchange FOREIGN KEY (exchange_id)        REFERENCES exchanges (id),
  CONSTRAINT fk_orders_market   FOREIGN KEY (exchange_market_id) REFERENCES exchange_markets (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Every order state transition (append-only). UNIQUE(order_id, version) for
-- replay-safety, mirroring cycle_state_events.
CREATE TABLE IF NOT EXISTS order_events (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  order_id     BIGINT UNSIGNED NOT NULL,
  event_type   VARCHAR(64) NOT NULL,
  from_state   VARCHAR(48) NULL,
  to_state     VARCHAR(48) NOT NULL,
  version      BIGINT UNSIGNED NOT NULL,
  message      TEXT NULL,
  payload_json JSON NULL,
  created_at   TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_order_events_order (order_id, id),
  UNIQUE KEY uq_order_event_version (order_id, version),
  CONSTRAINT fk_order_events_order FOREIGN KEY (order_id) REFERENCES orders (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Individual fills. UNIQUE(order_id, exchange_fill_id) dedups venue-reported fills
-- (NULLs are allowed to repeat, which is acceptable for venues without fill ids).
CREATE TABLE IF NOT EXISTS fills (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  order_id        BIGINT UNSIGNED NOT NULL,
  cycle_id        BIGINT UNSIGNED NOT NULL,
  exchange_fill_id VARCHAR(128) NULL,
  quantity        DECIMAL(36,18) NOT NULL,
  price           DECIMAL(36,12) NOT NULL,
  quote_amount    DECIMAL(36,8) NULL,
  fee_amount      DECIMAL(36,18) NULL,
  fee_asset       VARCHAR(32) NULL,
  filled_at       DATETIME(6) NULL,
  created_at      TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_fill (order_id, exchange_fill_id),
  KEY idx_fills_cycle (cycle_id),
  KEY idx_fills_order (order_id),
  CONSTRAINT fk_fills_order FOREIGN KEY (order_id) REFERENCES orders (id),
  CONSTRAINT fk_fills_cycle FOREIGN KEY (cycle_id) REFERENCES cycles (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- DB-backed symbol lock with COMPOSITE scope (exchange|canonical_symbol), so e.g.
-- BTC/USDT on Nobitex does not block BTC/USDT on Wallex. The STORED generated
-- column active_key is NULL unless state='ACTIVE', and UNIQUE(active_key) thus
-- enforces exactly one ACTIVE lock per scope+symbol (MariaDB ignores NULLs in a
-- unique index). Released/stale locks free the scope.
CREATE TABLE IF NOT EXISTS symbol_locks (
  id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  scope            VARCHAR(96) NOT NULL,                 -- e.g. exchange code
  canonical_symbol VARCHAR(64) NOT NULL,
  cycle_id         BIGINT UNSIGNED NOT NULL,
  state            ENUM('ACTIVE','RELEASED','STALE') NOT NULL DEFAULT 'ACTIVE',
  active_key       VARCHAR(180) GENERATED ALWAYS AS
                     (CASE WHEN state = 'ACTIVE' THEN CONCAT(scope, '|', canonical_symbol) ELSE NULL END) STORED,
  locked_at        DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  expires_at       DATETIME(6) NOT NULL,
  released_at      DATETIME(6) NULL,
  created_at       TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at       TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_active_lock (active_key),
  KEY idx_locks_expiry (state, expires_at),
  KEY idx_locks_cycle (cycle_id),
  CONSTRAINT fk_locks_cycle FOREIGN KEY (cycle_id) REFERENCES cycles (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Database-backed priority queue for exchange requests. The trade-engine ENQUEUES
-- (status QUEUED) inside its cycle-creation transaction; the order-executor CLAIMS
-- and sends. The status enum is fixed system-wide. idempotency_key is UNIQUE.
-- Indexes support the claim query (idx_claim), retry promotion (idx_retry), and
-- stuck-in-flight detection (idx_inflight).
CREATE TABLE IF NOT EXISTS exchange_requests (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_id     BIGINT UNSIGNED NOT NULL,
  symbol          VARCHAR(64) NULL,
  cycle_id        BIGINT UNSIGNED NULL,
  order_id        BIGINT UNSIGNED NULL,
  request_type    ENUM('PLACE_ORDER','CANCEL_ORDER','GET_ORDER','GET_OPEN_ORDERS','GET_BALANCE') NOT NULL,
  priority        SMALLINT NOT NULL DEFAULT 100,         -- lower = more urgent
  status          ENUM('QUEUED','CLAIMED','IN_FLIGHT','SUCCEEDED','FAILED','RETRY_SCHEDULED','DEAD') NOT NULL DEFAULT 'QUEUED',
  payload         JSON NOT NULL,
  response        JSON NULL,
  timeout_ms      INT NOT NULL DEFAULT 10000,
  retry_count     INT NOT NULL DEFAULT 0,
  max_retries     INT NOT NULL DEFAULT 5,
  next_retry_at   DATETIME(6) NULL,
  idempotency_key VARCHAR(128) NOT NULL,
  claimed_by      VARCHAR(128) NULL,
  claimed_at      DATETIME(6) NULL,
  inflight_at     DATETIME(6) NULL,
  last_error      TEXT NULL,
  created_at      TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at      TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_exreq_idem (idempotency_key),
  KEY idx_exreq_claim (exchange_id, status, priority, id),
  KEY idx_exreq_retry (status, next_retry_at),
  KEY idx_exreq_inflight (exchange_id, status, inflight_at),
  KEY idx_exreq_order (order_id),
  KEY idx_exreq_cycle (cycle_id),
  KEY idx_exreq_created (created_at),
  CONSTRAINT fk_exreq_exchange FOREIGN KEY (exchange_id) REFERENCES exchanges (id),
  CONSTRAINT fk_exreq_cycle    FOREIGN KEY (cycle_id)    REFERENCES cycles (id),
  CONSTRAINT fk_exreq_order    FOREIGN KEY (order_id)    REFERENCES orders (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Per-cycle fee snapshot taken at decision time (fees may change later), so a
-- cycle's economics are reproducible.
CREATE TABLE IF NOT EXISTS cycle_fee_snapshots (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  cycle_id      BIGINT UNSIGNED NOT NULL,
  exchange_id   BIGINT UNSIGNED NOT NULL,
  maker_fee     DECIMAL(18,8) NOT NULL,
  taker_fee     DECIMAL(18,8) NOT NULL,
  source        VARCHAR(64) NULL,
  snapshot_json JSON NULL,
  created_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_feesnap_cycle (cycle_id),
  CONSTRAINT fk_feesnap_cycle    FOREIGN KEY (cycle_id)    REFERENCES cycles (id),
  CONSTRAINT fk_feesnap_exchange FOREIGN KEY (exchange_id) REFERENCES exchanges (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
