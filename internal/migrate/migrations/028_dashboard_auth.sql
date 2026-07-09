-- migrate:ddl
-- 028_dashboard_auth
--
-- PR17 dashboard login/session model. A small set of trusted internal users (username +
-- salted PBKDF2-SHA256 password hash + role + active flag) and server-side sessions (only
-- the SHA-256 hash of the opaque session token is stored — never the token itself, never a
-- plaintext password). This replaces the PR16 "no auth" state: the read-only views, the
-- read/mutation APIs, and the WebSocket all require a valid logged-in session; only
-- /healthz and /login are open.
--
-- DDL only, idempotent (IF NOT EXISTS) so a re-run after a mid-file failure is safe.

CREATE TABLE IF NOT EXISTS dashboard_users (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  username       VARCHAR(64)  NOT NULL,
  password_hash  VARCHAR(255) NOT NULL,                       -- PBKDF2-SHA256 ($pbkdf2-sha256$...); NEVER plaintext
  role           VARCHAR(32)  NOT NULL DEFAULT 'viewer',      -- viewer | config_operator | admin
  active         TINYINT(1)   NOT NULL DEFAULT 1,             -- 0 = disabled (cannot log in)
  last_login_at  DATETIME(6)  NULL,
  created_at     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_dashboard_user (username),
  CONSTRAINT chk_dashboard_user_role CHECK (role IN ('viewer','config_operator','admin'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dashboard_sessions (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  token_hash  CHAR(64)     NOT NULL,                          -- sha256(hex) of the session token; token itself NEVER stored
  user_id     BIGINT UNSIGNED NOT NULL,
  username    VARCHAR(64)  NOT NULL,                          -- snapshot at login (for audit convenience)
  role        VARCHAR(32)  NOT NULL,                          -- snapshot at login
  created_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  expires_at  DATETIME(6)  NOT NULL,
  revoked_at  DATETIME(6)  NULL,                              -- set on logout / invalidation
  UNIQUE KEY uq_dashboard_session_token (token_hash),
  KEY idx_dashboard_session_user (user_id),
  KEY idx_dashboard_session_expiry (expires_at),
  CONSTRAINT fk_dashboard_session_user FOREIGN KEY (user_id) REFERENCES dashboard_users (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
