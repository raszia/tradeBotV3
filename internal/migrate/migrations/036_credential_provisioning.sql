-- migrate:ddl
-- 036_credential_provisioning
--
-- PR22 credential provisioning/rotation safety. Three forward, idempotent DDL changes:
--
--   1. Add a 'valid' credential status: a credential that has been VALIDATED (a read-only
--      auth check succeeded) but is NOT yet the active one. The safe workflow is
--      create(unknown) -> validate(valid) -> activate(active). Only 'active' + enabled=1 is
--      loaded by the provider, so a 'valid' credential is proven-good but not yet in use.
--
--   2. A DATABASE-LEVEL guard that at most ONE active+enabled credential exists per exchange
--      (rotation must never leave two). A STORED generated column is exchange_id exactly when
--      the row is the live credential (enabled=1 AND status='active'), else NULL; a UNIQUE key
--      over it lets many disabled/valid/invalid rows coexist but permits only one live row per
--      exchange (MariaDB unique indexes ignore NULLs). This complements the per-exchange
--      FOR UPDATE serialization in the provisioner. NOTE: if an exchange already has two active
--      credentials from before this migration, the ALTER fails loudly (it does not silently
--      rewrite data) — resolve the duplicates, then re-run.
--
--   3. Add 'credential_operator' to the dashboard user role vocabulary (an orthogonal
--      capability, like reconcile_operator — enforced by an exact-set check, never the config
--      ladder).

ALTER TABLE exchange_credentials
  MODIFY status ENUM('unknown','valid','active','invalid','disabled','error') NOT NULL DEFAULT 'unknown';

ALTER TABLE exchange_credentials
  ADD COLUMN IF NOT EXISTS active_guard BIGINT UNSIGNED
    GENERATED ALWAYS AS (CASE WHEN enabled = 1 AND status = 'active' THEN exchange_id ELSE NULL END) STORED;

ALTER TABLE exchange_credentials
  ADD UNIQUE KEY IF NOT EXISTS uq_active_credential_per_exchange (active_guard);

ALTER TABLE dashboard_users DROP CONSTRAINT IF EXISTS chk_dashboard_user_role;

ALTER TABLE dashboard_users
  ADD CONSTRAINT chk_dashboard_user_role
  CHECK (role IN ('viewer','config_operator','admin','reconcile_operator','credential_operator'));
