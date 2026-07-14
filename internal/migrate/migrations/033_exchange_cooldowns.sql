-- migrate:ddl
-- PR20 correction #5: DURABLE per-exchange rate-limit cooldowns.
--
-- A throttled exchange must pause for the required period even across a process restart.
-- Before this table the park deadline lived only in executor memory, so a restart could send
-- a new request to a still-throttled venue before its cooldown expired. One row per exchange
-- (the deadline is exchange-wide by definition); extend-only updates are enforced by the
-- writer with GREATEST(...) so a shorter later cooldown can never shorten a longer active one.
--
-- Contains NO secrets: `reason` is a short category/code (e.g. "rate_limit:TooManyRequests")
-- and `source` is which signal identified it (status|header|body|code|fallback) — never a
-- response body, token, or credential.
CREATE TABLE IF NOT EXISTS exchange_cooldowns (
  exchange_id    BIGINT UNSIGNED NOT NULL,
  cooldown_until DATETIME(6)     NOT NULL,
  reason         VARCHAR(128)    NOT NULL DEFAULT '',
  source         VARCHAR(32)     NOT NULL DEFAULT '',
  updated_at     TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (exchange_id),
  KEY idx_exchange_cooldowns_until (cooldown_until),
  CONSTRAINT fk_exchange_cooldowns_exchange FOREIGN KEY (exchange_id)
    REFERENCES exchanges(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
