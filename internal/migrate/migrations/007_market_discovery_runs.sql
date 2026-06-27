-- migrate:ddl
-- 007_market_discovery_runs
--
-- Tracks each execution of the dashboard-triggered market-discovery action
-- (item 4 / item 13). Discovery reads available Binance and Iranian-exchange
-- markets, finds common tradable symbols (USDT- and rial-based), and upserts
-- exchange_markets. This table records WHEN discovery ran, WHAT it changed, and
-- whether it failed — so the dashboard can show discovery status/history.
--
-- The discovery EXECUTION logic (which needs the exchange clients) and the
-- dashboard trigger are later PRs; this is storage only. Discovery never starts
-- trading.
CREATE TABLE IF NOT EXISTS market_discovery_runs (
  id                     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  requested_by           VARCHAR(128) NULL,
  status                 ENUM('pending','running','succeeded','failed') NOT NULL DEFAULT 'pending',
  started_at             DATETIME(6) NULL,
  finished_at            DATETIME(6) NULL,
  binance_markets_count  INT NULL,
  exchange_markets_count INT NULL,
  common_markets_count   INT NULL,
  created_count          INT NULL,
  updated_count          INT NULL,
  disabled_count         INT NULL,
  error_message          TEXT NULL,
  raw_summary            JSON NULL,
  created_at             TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_discovery_status (status, created_at),
  KEY idx_discovery_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
