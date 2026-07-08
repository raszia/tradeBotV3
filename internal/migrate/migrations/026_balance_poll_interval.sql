-- migrate:ddl
-- PR13: per-exchange balance-sync poll cadence.
--
-- A per-exchange balance poll interval (seconds). NULL or 0 means "use the syncer's default
-- Interval"; a positive value polls THAT exchange every N seconds (still floored by the
-- syncer's MinInterval), so a rate-limited venue can be polled LESS often than others without
-- forcing a single global cadence. Lives on exchange_configs alongside the other per-exchange
-- operational knobs (concurrency/timeouts/rate limit) — DB-config, not env. Idempotent.
ALTER TABLE exchange_configs
  ADD COLUMN IF NOT EXISTS balance_poll_interval_seconds INT NULL;

ALTER TABLE exchange_configs
  ADD CONSTRAINT IF NOT EXISTS chk_ec_balance_interval_nonneg CHECK (balance_poll_interval_seconds >= 0);
