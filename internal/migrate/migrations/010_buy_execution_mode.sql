-- migrate:ddl
-- 010_buy_execution_mode
--
-- PR9 makes the trade-engine create buy cycles. The owner-defined buy policy is
-- maker-first with taker fallback (see PROJECT_ARCHITECTURE.md §2a): try a maker
-- limit slightly below the Iranian ask to save fees; after a configurable number of
-- maker attempts within a rolling window, escalate to a taker buy at/near the ask.
-- These knobs are DB-configurable per exchange-market/symbol (versioned), and the
-- chosen mode/attempt is persisted on the order and cycle for audit/dashboard.
--
-- DDL only, idempotent (ADD COLUMN IF NOT EXISTS).

-- Per-symbol maker/taker policy.
ALTER TABLE symbol_configs
  ADD COLUMN IF NOT EXISTS maker_first_enabled         TINYINT(1) NOT NULL DEFAULT 1 AFTER retry_backoff_ms,
  ADD COLUMN IF NOT EXISTS maker_attempts_before_taker INT        NOT NULL DEFAULT 1 AFTER maker_first_enabled,
  ADD COLUMN IF NOT EXISTS maker_signal_window_seconds INT        NOT NULL DEFAULT 60 AFTER maker_attempts_before_taker,
  ADD COLUMN IF NOT EXISTS maker_wait_before_cancel_ms INT        NOT NULL DEFAULT 2000 AFTER maker_signal_window_seconds,
  ADD COLUMN IF NOT EXISTS maker_price_offset_bps      INT        NOT NULL DEFAULT 5 AFTER maker_wait_before_cancel_ms,
  ADD COLUMN IF NOT EXISTS taker_price_mode            VARCHAR(16) NOT NULL DEFAULT 'ASK' AFTER maker_price_offset_bps,
  ADD COLUMN IF NOT EXISTS max_taker_slippage_bps      INT        NULL AFTER taker_price_mode;

-- The buy order records which mode it was prepared as (intended) and, later
-- (PR10/PR11), what actually happened (actual), plus the maker context used.
ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS intended_execution_mode VARCHAR(24)    NULL AFTER role,
  ADD COLUMN IF NOT EXISTS actual_execution_mode   VARCHAR(24)    NULL AFTER intended_execution_mode,
  ADD COLUMN IF NOT EXISTS maker_attempt_number    INT            NULL AFTER actual_execution_mode,
  ADD COLUMN IF NOT EXISTS maker_offset_bps        INT            NULL AFTER maker_attempt_number,
  ADD COLUMN IF NOT EXISTS ask_price_at_decision   DECIMAL(36,12) NULL AFTER maker_offset_bps;

-- The cycle mirrors the intended mode + attempt + the opportunity-window start so
-- per-cycle reporting (dashboard) does not need to join the order.
ALTER TABLE cycles
  ADD COLUMN IF NOT EXISTS intended_execution_mode       VARCHAR(24) NULL AFTER buy_size,
  ADD COLUMN IF NOT EXISTS maker_attempt_number          INT         NULL AFTER intended_execution_mode,
  ADD COLUMN IF NOT EXISTS opportunity_window_started_at DATETIME(6) NULL AFTER maker_attempt_number;
