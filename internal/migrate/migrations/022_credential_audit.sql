-- PR22: credential provisioning/rotation/disable audit.
--
-- Every credential operation (create / rotate / disable / validate) writes one immutable
-- row here. It records WHO did WHAT to WHICH credential, the status + key_version before
-- and after, and a reason — but NEVER any secret material (no api key/secret/passphrase,
-- no encrypted blob, no plaintext). The encrypted secrets live only in exchange_credentials
-- (VARBINARY); this table is safe to read on the dashboard.
--
-- DDL-only file (house rule: never mix DDL + DML in one migration).
CREATE TABLE IF NOT EXISTS credential_audit (
  id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  exchange_id      BIGINT UNSIGNED NOT NULL,
  credential_id    BIGINT UNSIGNED NULL,            -- the affected credential row (NULL if it no longer applies)
  operator         VARCHAR(128) NOT NULL,           -- authenticated dashboard operator name
  action           VARCHAR(32)  NOT NULL,           -- create | rotate_new | rotate_disable_old | disable | validate
  old_status       VARCHAR(16)  NULL,
  new_status       VARCHAR(16)  NULL,
  old_key_version  INT NULL,
  new_key_version  INT NULL,
  reason           TEXT NULL,                        -- operator reason (never secrets)
  created_at       TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY idx_cred_audit_exchange (exchange_id),
  KEY idx_cred_audit_credential (credential_id),
  KEY idx_cred_audit_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
