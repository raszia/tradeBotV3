-- migrate:ddl
-- 034_reconcile_operator_role
--
-- PR21 operator reconciliation. Two forward, idempotent DDL changes:
--   1. Add the `reconcile_operator` value to the dashboard user role vocabulary. Reconciliation
--      access is an ORTHOGONAL capability, NOT a rung on the config ladder: a reconcile_operator
--      must be able to resolve NEEDS_RECONCILE cases but must NOT gain config-edit permission. The
--      authorization code enforces that with an exact-role capability check (never the linear
--      roleRank ladder); this migration only widens the CHECK constraint so the role can be stored.
--   2. Record the exposure classification (PROVEN_ZERO / OPEN / UNKNOWN / INCONSISTENT) that a
--      resolution acted on, alongside the existing before/after snapshots in reconcile_resolutions.
--
-- The role vocabulary lives in a named CHECK constraint on dashboard_users.role (a VARCHAR(32));
-- MariaDB lets us swap it idempotently. dashboard_sessions.role has no CHECK, so only the users
-- table changes.

ALTER TABLE dashboard_users DROP CONSTRAINT IF EXISTS chk_dashboard_user_role;

ALTER TABLE dashboard_users
  ADD CONSTRAINT chk_dashboard_user_role
  CHECK (role IN ('viewer','config_operator','admin','reconcile_operator'));

ALTER TABLE reconcile_resolutions
  ADD COLUMN IF NOT EXISTS exposure_classification VARCHAR(16) NULL AFTER external_resolution_confirmed;
