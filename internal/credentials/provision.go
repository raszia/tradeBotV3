package credentials

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/go-sql-driver/mysql"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/secrets"
)

// Provisioner creates, rotates, and disables exchange credentials. It encrypts plaintext
// in memory with the bootstrap master key and writes ONLY ciphertext to
// exchange_credentials — plaintext is never stored, never logged, never returned, and
// never written to the audit. Every operation writes a credential_audit row (no secrets).
type Provisioner struct {
	db     *sql.DB
	cipher *secrets.Cipher
	clk    clock.Clock
	log    *slog.Logger
	// faultBeforeCommit is a TEST-ONLY hook. When non-nil it runs just before an ActivateOrRotate
	// transaction commits; a non-nil return forces a full rollback, proving the credential state
	// change and its audit row roll back together (PR22 requirement 8). Never set in production.
	faultBeforeCommit func() error
}

// NewProvisioner builds a Provisioner. An empty master key returns secrets.ErrNoMasterKey
// so provisioning is disabled safely (it never panics). The master key comes from the
// bootstrap config file — never a runtime env var.
func NewProvisioner(db *sql.DB, masterKey string, clk clock.Clock, log *slog.Logger) (*Provisioner, error) {
	cipher, err := secrets.NewCipher(masterKey)
	if err != nil {
		return nil, err
	}
	if clk == nil {
		clk = clock.NewSystem()
	}
	if log == nil {
		log = slog.Default()
	}
	return &Provisioner{db: db, cipher: cipher, clk: clk, log: log}, nil
}

// CredentialValidationError marks an operator-input problem (-> HTTP 400). It carries NO
// secret material.
type CredentialValidationError struct{ Msg string }

func (e CredentialValidationError) Error() string { return e.Msg }

func cvf(format string, a ...any) error {
	return CredentialValidationError{Msg: fmt.Sprintf(format, a...)}
}

// IsCredentialValidation reports whether err is an operator-input validation error.
func IsCredentialValidation(err error) bool {
	var v CredentialValidationError
	return errors.As(err, &v)
}

var validCredStatus = map[string]bool{"unknown": true, "active": true, "invalid": true, "disabled": true, "error": true}

// CreateInput is the plaintext (in-memory-only) input to Create. The secret fields are
// encrypted immediately and never stored/logged/returned in the clear. NOTE: enabled, status and
// key_version are NOT accepted from the caller — the service always assigns them (PR22 requirement
// 1), so a new credential can never be created already-active.
type CreateInput struct {
	ExchangeCode string `json:"exchange_code"`
	Label        string `json:"label"`
	Algorithm    string `json:"algorithm"`
	APIKey       string `json:"api_key"`
	APISecret    string `json:"api_secret"`
	Passphrase   string `json:"passphrase"`
	Reason       string `json:"reason"`
	Operator     string `json:"-"` // from the authenticated session, never the body
}

// Create encrypts the supplied secrets and inserts ONE credential row that is ALWAYS INACTIVE and
// UNVALIDATED — enabled=0, status='unknown', key_version = service-assigned (max for the exchange
// + 1). A new credential can never be used until it is validated and then activated (PR22
// requirement 1). Returns only the new credential id — never the plaintext or the encrypted blob.
func (p *Provisioner) Create(ctx context.Context, in CreateInput) (int64, error) {
	exID, err := p.exchangeID(ctx, in.ExchangeCode)
	if err != nil {
		return 0, err
	}
	if err := p.validateCreate(&in); err != nil {
		return 0, err
	}
	encKey, encSec, encPass, err := p.encryptFields(in.APIKey, in.APISecret, in.Passphrase)
	if err != nil {
		return 0, err
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	// Service-assigned key_version — never from the body.
	var maxKV sql.NullInt64
	_ = tx.QueryRowContext(ctx, "SELECT MAX(key_version) FROM exchange_credentials WHERE exchange_id=?", exID).Scan(&maxKV)
	kv := 1
	if maxKV.Valid {
		kv = int(maxKV.Int64) + 1
	}
	res, err := tx.ExecContext(ctx, `
INSERT INTO exchange_credentials
  (exchange_id, label, encrypted_api_key, encrypted_api_secret, encrypted_passphrase, encryption_algorithm, key_version, enabled, status)
VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'unknown')`,
		exID, in.Label, encKey, encSec, encPass, in.Algorithm, kv)
	if err != nil {
		if isDup(err) {
			return 0, cvf("a credential labelled %q already exists for %s", in.Label, in.ExchangeCode)
		}
		return 0, err
	}
	id, _ := res.LastInsertId()
	if err := p.audit(ctx, tx, exID, &id, in.Operator, "create", "", "unknown", 0, kv, in.Reason); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

// RotateInput rotates an exchange's credentials: a NEW credential supersedes the current
// active one. The old plaintext is not needed (only the new secrets are supplied).
// ValidateCredential performs the ONLY allowed credential check — a read-only balance read via the
// narrow BalanceReader (never places/cancels) against the EXACT credential the operator selected
// (PR22 requirement 2; the caller builds the reader with Builder.BuildForCredential(id)). The
// status update AND the audit row are written in ONE transaction (requirement 5). Classification
// (requirement 2):
//   - success                        → status 'valid' (validated, ready to activate).
//   - DEFINITE auth/permission error → status 'invalid'.
//   - transient (timeout/network/rate limit/5xx) → status UNCHANGED (a brief incident must never
//     permanently invalidate a good credential); the attempt is noted + audited.
func (p *Provisioner) ValidateCredential(ctx context.Context, credentialID int64, r BalanceReader, operator string) error {
	if strings.TrimSpace(operator) == "" {
		return cvf("an authenticated operator is required")
	}
	// Validation is fully SERIALIZED against activation/rotation (PR22 round-3 blocker 1): the
	// credential row and then its exchange row are locked FOR UPDATE — the SAME lock order as
	// ActivateOrRotate — and held for the DURATION of the (read-only) network check. So an
	// activate/rotate cannot commit in the middle of a validation, and a failed validation can never
	// overwrite a credential that was concurrently activated. This is a low-frequency operator
	// action, so holding the lock across the network call is acceptable.
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var exID, kv int64
	var curStatus string
	err = tx.QueryRowContext(ctx, "SELECT exchange_id, status, key_version FROM exchange_credentials WHERE id=? FOR UPDATE", credentialID).Scan(&exID, &curStatus, &kv)
	if errors.Is(err, sql.ErrNoRows) {
		return cvf("credential %d not found", credentialID)
	}
	if err != nil {
		return err
	}
	var lockedEx int64
	if err := tx.QueryRowContext(ctx, "SELECT id FROM exchanges WHERE id=? FOR UPDATE", exID).Scan(&lockedEx); err != nil {
		return err
	}
	// Read-only validation WHILE holding the locks — activation/rotation is blocked until we commit.
	_, balErr := r.GetBalances(ctx)

	// Decide the new status from the CURRENT (locked) status, never the pre-network snapshot.
	newStatus := curStatus
	note := "read-only balance check: ok"
	switch {
	case curStatus == "disabled":
		// It was disabled (e.g. rotated away) during validation — never resurrect its status.
		note = "read-only check completed but the credential was disabled during validation — status unchanged"
	case balErr == nil:
		// Success. Keep ACTIVE (never downgrade the live credential to 'valid'); otherwise mark valid.
		if curStatus == "active" {
			newStatus = "active"
			note = "read-only check: ok (already active)"
		} else {
			newStatus = "valid"
		}
	case IsDefiniteAuthError(balErr):
		newStatus = "invalid"
		note = "read-only check: authentication/permission rejected" // safe, no secret
	default:
		// Transient (timeout/network/rate-limit/5xx) — preserve the current DB state EXACTLY.
		newStatus = curStatus
		note = "read-only check could not complete (transient error) — status unchanged"
	}

	if _, err := tx.ExecContext(ctx,
		"UPDATE exchange_credentials SET status=?, last_auth_error=?, last_checked_at=? WHERE id=?",
		newStatus, note, p.clk.Now().UTC(), credentialID); err != nil {
		return err
	}
	if err := p.audit(ctx, tx, exID, &credentialID, operator, "validate", curStatus, newStatus, int(kv), int(kv), note); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return balErr // surface the (masked) error to the caller
}

// ActivateOrRotate makes the ALREADY-VALIDATED credential `credentialID` the single active
// credential for its exchange, disabling any previously-active one — ALL in one transaction,
// serialized per exchange with a FOR UPDATE lock on the exchange row (PR22 requirement 3). It
// refuses a credential that has not been validated (status must be 'valid' or already 'active').
// Because the old credential is disabled and the new one activated together, the exchange is never
// left without a working credential, and (with the DB active-guard) two concurrent rotations can
// never leave two active credentials. If any step fails, the previous credential stays active.
func (p *Provisioner) ActivateOrRotate(ctx context.Context, credentialID int64, reason, operator string) error {
	if strings.TrimSpace(reason) == "" {
		return cvf("a reason is required to activate/rotate a credential")
	}
	if strings.TrimSpace(operator) == "" {
		return cvf("an authenticated operator is required")
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Lock the credential row, then serialize on its exchange row (per-exchange rotation lock).
	var exID, kv int64
	var status string
	err = tx.QueryRowContext(ctx, "SELECT exchange_id, status, key_version FROM exchange_credentials WHERE id=? FOR UPDATE", credentialID).Scan(&exID, &status, &kv)
	if errors.Is(err, sql.ErrNoRows) {
		return cvf("credential %d not found", credentialID)
	}
	if err != nil {
		return err
	}
	var lockedEx int64
	if err := tx.QueryRowContext(ctx, "SELECT id FROM exchanges WHERE id=? FOR UPDATE", exID).Scan(&lockedEx); err != nil {
		return err
	}
	// Confirm the credential has been VALIDATED (never activate an unvalidated one — requirement 1).
	if status != "valid" && status != "active" {
		return cvf("credential %d has not been validated (status %q) — validate it before activating", credentialID, status)
	}

	// Disable every OTHER active credential FIRST so activating the new one cannot trip the
	// one-active-per-exchange DB guard, and so the exchange never has two active credentials.
	rows, err := tx.QueryContext(ctx, "SELECT id, key_version FROM exchange_credentials WHERE exchange_id=? AND enabled=1 AND status='active' AND id<>? FOR UPDATE", exID, credentialID)
	if err != nil {
		return err
	}
	type oldRow struct {
		id int64
		kv int
	}
	var olds []oldRow
	for rows.Next() {
		var o oldRow
		if err := rows.Scan(&o.id, &o.kv); err != nil {
			rows.Close()
			return err
		}
		olds = append(olds, o)
	}
	rows.Close()
	for _, o := range olds {
		if _, err := tx.ExecContext(ctx, "UPDATE exchange_credentials SET enabled=0, status='disabled' WHERE id=?", o.id); err != nil {
			return err
		}
		oid := o.id
		if err := p.audit(ctx, tx, exID, &oid, operator, "rotate_disable_old", "active", "disabled", o.kv, o.kv, reason); err != nil {
			return err
		}
	}

	// Activate the selected credential.
	if _, err := tx.ExecContext(ctx, "UPDATE exchange_credentials SET enabled=1, status='active' WHERE id=?", credentialID); err != nil {
		if isDup(err) {
			return cvf("another credential is already active for this exchange — retry")
		}
		return err
	}
	if err := p.audit(ctx, tx, exID, &credentialID, operator, "activate", status, "active", int(kv), int(kv), reason); err != nil {
		return err
	}
	if p.faultBeforeCommit != nil {
		if ferr := p.faultBeforeCommit(); ferr != nil {
			return ferr // deferred tx.Rollback() undoes the activate + the disables + all audit rows
		}
	}
	return tx.Commit()
}

// Disable turns a credential off (enabled=0, status='disabled'), keeping the row + audit history.
// It REFUSES to disable the LAST usable credential for an exchange while that exchange has open
// operational risk (a non-terminal live cycle, a non-terminal/NEEDS_RECONCILE live order, an active
// live exchange request, or an active live session) — an operator must not remove the ability to
// sell inventory, cancel an order, or recover an ambiguous result (PR22 requirement 4). Terminal
// history, dry-run data, and inactive locks never block. The state change + audit are one tx.
func (p *Provisioner) Disable(ctx context.Context, credentialID int64, reason, operator string) error {
	if strings.TrimSpace(reason) == "" {
		return cvf("a reason is required to disable a credential")
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var exID, kv int64
	var oldStatus string
	var enabled int
	err = tx.QueryRowContext(ctx, "SELECT exchange_id, status, key_version, enabled FROM exchange_credentials WHERE id=? FOR UPDATE", credentialID).Scan(&exID, &oldStatus, &kv, &enabled)
	if errors.Is(err, sql.ErrNoRows) {
		return cvf("credential %d not found", credentialID)
	}
	if err != nil {
		return err
	}
	// Open-risk guard: only when disabling this credential would leave the exchange with NO
	// OPERATIONAL credential. Only enabled + status='active' is operational — the live provider loads
	// exactly that; a 'valid' (validated-but-not-active) credential is NOT a live replacement (PR22
	// round-2 blocker 2a). Since the DB active-guard permits only one active credential per exchange,
	// this effectively refuses disabling the live credential under open risk — use rotation instead.
	if enabled == 1 && oldStatus == "active" {
		var otherOperational int
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM exchange_credentials WHERE exchange_id=? AND id<>? AND enabled=1 AND status='active'",
			exID, credentialID).Scan(&otherOperational); err != nil {
			return err
		}
		if otherOperational == 0 {
			if detail, err := p.exchangeOpenRisk(ctx, tx, exID); err != nil {
				return err
			} else if detail != "" {
				return cvf("refusing to disable the last operational (active) credential for this exchange while it has open risk (%s) — rotate to a validated credential instead, or resolve the risk first", detail)
			}
		}
	}

	if _, err := tx.ExecContext(ctx, "UPDATE exchange_credentials SET enabled=0, status='disabled' WHERE id=?", credentialID); err != nil {
		return err
	}
	if err := p.audit(ctx, tx, exID, &credentialID, operator, "disable", oldStatus, "disabled", int(kv), int(kv), reason); err != nil {
		return err
	}
	return tx.Commit()
}

// exchangeOpenRisk returns a short, non-empty description when the exchange has LIVE operational
// risk that a working credential is needed to resolve, else "". Dry-run data and terminal history
// never count (requirement 4: "do not make this unnecessarily broad").
func (p *Provisioner) exchangeOpenRisk(ctx context.Context, tx *sql.Tx, exID int64) (string, error) {
	var liveSession, liveCycle, liveOrder, activeReq bool
	// Active-request ownership is ORDER-AUTHORITATIVE (PR22 round-2 blocker 2b): when a request has
	// an order_id, its real exchange + mode come from the persisted ORDER's cycle, NOT the request
	// row's own (possibly wrong/stale/missing) exchange_id/cycle_id. Only a request with NO order_id
	// falls back to its own fields. This way a queue row with bad metadata cannot hide an active
	// request whose real order is a live order on the exchange being disabled.
	err := tx.QueryRowContext(ctx, `
SELECT
  EXISTS(SELECT 1 FROM live_run_sessions WHERE exchange_id=? AND status='ACTIVE'),
  EXISTS(SELECT 1 FROM cycles WHERE buy_exchange_id=? AND dry_run=0 AND state NOT IN ('CLOSED','CANCELLED','FAILED')),
  EXISTS(SELECT 1 FROM orders o JOIN cycles c ON c.id=o.cycle_id
         WHERE o.exchange_id=? AND c.dry_run=0 AND o.state NOT IN ('FILLED','CANCELLED','REJECTED','EXPIRED','FAILED')),
  EXISTS(SELECT 1 FROM exchange_requests er
         LEFT JOIN orders o  ON o.id  = er.order_id
         LEFT JOIN cycles oc ON oc.id = o.cycle_id
         LEFT JOIN cycles rc ON rc.id = er.cycle_id
         WHERE er.status IN ('QUEUED','RETRY_SCHEDULED','CLAIMED','IN_FLIGHT')
           AND (
             (er.order_id IS NOT NULL AND o.exchange_id = ? AND oc.dry_run = 0)
             OR (er.order_id IS NULL AND er.exchange_id = ? AND (rc.dry_run = 0 OR rc.id IS NULL))
           ))`,
		exID, exID, exID, exID, exID).Scan(&liveSession, &liveCycle, &liveOrder, &activeReq)
	if err != nil {
		return "", err
	}
	var risks []string
	if liveSession {
		risks = append(risks, "an active live session")
	}
	if liveCycle {
		risks = append(risks, "a non-terminal live cycle")
	}
	if liveOrder {
		risks = append(risks, "an open or NEEDS_RECONCILE live order")
	}
	if activeReq {
		risks = append(risks, "an active live exchange request")
	}
	return strings.Join(risks, ", "), nil
}

// ---- helpers ----

func (p *Provisioner) validateCreate(in *CreateInput) error {
	in.Label = strings.TrimSpace(in.Label)
	if in.Label == "" {
		return cvf("label is required")
	}
	if strings.TrimSpace(in.APIKey) == "" || strings.TrimSpace(in.APISecret) == "" {
		return cvf("api_key and api_secret are required")
	}
	if in.Algorithm == "" {
		in.Algorithm = secrets.AlgorithmAESGCM
	}
	if !secrets.SupportedAlgorithm(in.Algorithm) {
		return cvf("unsupported encryption algorithm %q", in.Algorithm)
	}
	// enabled/status/key_version are NEVER taken from the caller — Create assigns enabled=0,
	// status='unknown', service key_version (PR22 requirement 1).
	if strings.TrimSpace(in.Reason) == "" {
		return cvf("a reason is required to create a credential")
	}
	return nil
}

// encryptFields encrypts the three secret fields; an empty field becomes a NULL blob.
func (p *Provisioner) encryptFields(apiKey, apiSecret, passphrase string) (encKey, encSec, encPass any, err error) {
	enc := func(s string) (any, error) {
		if strings.TrimSpace(s) == "" {
			return nil, nil
		}
		b, e := p.cipher.Encrypt([]byte(s))
		if e != nil {
			return nil, e
		}
		return b, nil
	}
	if encKey, err = enc(apiKey); err != nil {
		return
	}
	if encSec, err = enc(apiSecret); err != nil {
		return
	}
	encPass, err = enc(passphrase)
	return
}

func (p *Provisioner) exchangeID(ctx context.Context, code string) (int64, error) {
	var id int64
	err := p.db.QueryRowContext(ctx, "SELECT id FROM exchanges WHERE code=?", strings.TrimSpace(code)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, cvf("exchange %q not found", code)
	}
	return id, err
}

func (p *Provisioner) audit(ctx context.Context, tx *sql.Tx, exID int64, credID *int64, operator, action, oldStatus, newStatus string, oldKV, newKV int, reason string) error {
	if strings.TrimSpace(operator) == "" {
		return cvf("an authenticated operator is required")
	}
	var cid any
	if credID != nil {
		cid = *credID
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO credential_audit (exchange_id, credential_id, operator, action, old_status, new_status, old_key_version, new_key_version, reason)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		exID, cid, operator, action, nullStrCred(oldStatus), nullStrCred(newStatus), nullIntCred(oldKV), nullIntCred(newKV), reason)
	return err
}

func nullStrCred(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func nullIntCred(i int) any {
	if i == 0 {
		return nil
	}
	return i
}
func b2iCred(b bool) int {
	if b {
		return 1
	}
	return 0
}

// stripVersionSuffix removes a trailing "#vN" so a derived rotation label doesn't grow
// "#v2#v3#v4" across rotations.
func stripVersionSuffix(label string) string {
	if i := strings.LastIndex(label, "#v"); i >= 0 {
		return label[:i]
	}
	return label
}

func isDup(err error) bool {
	var myErr *mysql.MySQLError
	return errors.As(err, &myErr) && myErr.Number == 1062
}
