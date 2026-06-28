-- migrate:ddl
-- 017_dashboard_tokens
--
-- PR17 operator authentication for dashboard CONFIG EDITING. Every mutating dashboard
-- route requires a bearer token mapped to an operator role; read views stay open
-- (local, read-only). Tokens are stored as a SHA-256 hash only (never the plaintext)
-- and provisioned out-of-band (an admin inserts a hashed token); a token-management UI
-- is a later concern. Roles separate a read-only viewer from a config operator (and a
-- credential operator / admin for later). DDL only, idempotent.
CREATE TABLE IF NOT EXISTS dashboard_tokens (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name         VARCHAR(128) NOT NULL,
  token_hash   CHAR(64) NOT NULL,                       -- sha256(token) hex; never the plaintext
  role         VARCHAR(32) NOT NULL DEFAULT 'viewer',   -- viewer | config_operator | credential_operator | admin
  enabled      TINYINT(1) NOT NULL DEFAULT 1,
  created_at   TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  last_used_at DATETIME(6) NULL,
  UNIQUE KEY uq_dashboard_token (token_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
