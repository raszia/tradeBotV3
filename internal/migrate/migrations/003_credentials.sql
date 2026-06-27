-- migrate:ddl
-- 003_credentials
--
-- Exchange API credentials and their audit trail. SECURITY-CRITICAL.
--
-- There are intentionally NO plaintext columns (no api_key/api_secret/passphrase).
-- Secrets are stored only as encrypted payloads. Each encrypted_* column holds a
-- self-contained AES-256-GCM payload structured as nonce(12B) || ciphertext ||
-- tag(16B), so no separate per-row nonce column is needed; encryption_algorithm
-- and key_version support algorithm/key rotation. All dashboard/API/log outputs
-- must show MASKED values only. The encryption implementation itself is a later
-- PR; this migration only provides the at-rest schema.
CREATE TABLE IF NOT EXISTS exchange_credentials (
  id                   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_id          BIGINT UNSIGNED NOT NULL,
  label                VARCHAR(64) NOT NULL DEFAULT 'default',  -- multiple creds per exchange
  encrypted_api_key    VARBINARY(2048) NULL,
  encrypted_api_secret VARBINARY(4096) NULL,
  encrypted_passphrase VARBINARY(2048) NULL,
  encryption_algorithm VARCHAR(32) NOT NULL DEFAULT 'AES-256-GCM',
  key_version          INT NOT NULL DEFAULT 1,
  enabled              TINYINT(1) NOT NULL DEFAULT 0,
  status               ENUM('unknown','active','invalid','disabled','error') NOT NULL DEFAULT 'unknown',
  last_checked_at      DATETIME(6) NULL,
  last_auth_error      TEXT NULL,                  -- must never contain secrets
  created_at           TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at           TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_credential (exchange_id, label),
  KEY idx_credential_enabled (exchange_id, enabled),
  CONSTRAINT fk_credential_exchange FOREIGN KEY (exchange_id) REFERENCES exchanges (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Dedicated audit trail for credential changes. Rows MUST NEVER store plaintext
-- secrets. At most a short masked key prefix (e.g. first 4 chars) is recorded for
-- identification. No FK to exchange_credentials.id so audit history survives a
-- credential deletion.
CREATE TABLE IF NOT EXISTS exchange_credential_audit (
  id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_id      BIGINT UNSIGNED NOT NULL,
  credential_id    BIGINT UNSIGNED NULL,
  action           ENUM('create','update','enable','disable','rotate','delete','auth_check') NOT NULL,
  changed_by       VARCHAR(128) NULL,
  masked_key_prefix VARCHAR(16) NULL,              -- e.g. 'abcd…' — NEVER the full secret
  reason           VARCHAR(255) NULL,
  created_at       TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_credaudit_exchange (exchange_id, created_at),
  KEY idx_credaudit_credential (credential_id),
  CONSTRAINT fk_credaudit_exchange FOREIGN KEY (exchange_id) REFERENCES exchanges (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
