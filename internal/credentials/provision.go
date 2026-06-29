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
// encrypted immediately and never stored/logged/returned in the clear.
type CreateInput struct {
	ExchangeCode string `json:"exchange_code"`
	Label        string `json:"label"`
	KeyVersion   int    `json:"key_version"`
	Algorithm    string `json:"algorithm"`
	Enabled      bool   `json:"enabled"`
	Status       string `json:"status"`
	APIKey       string `json:"api_key"`
	APISecret    string `json:"api_secret"`
	Passphrase   string `json:"passphrase"`
	Reason       string `json:"reason"`
	Operator     string `json:"-"` // from the authenticated session, never the body
}

// Create encrypts the supplied secrets and inserts ONE credential row, then audits it. It
// returns only the new credential id — never the plaintext or the encrypted blob.
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

	res, err := tx.ExecContext(ctx, `
INSERT INTO exchange_credentials
  (exchange_id, label, encrypted_api_key, encrypted_api_secret, encrypted_passphrase, encryption_algorithm, key_version, enabled, status)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		exID, in.Label, encKey, encSec, encPass, in.Algorithm, in.KeyVersion, b2iCred(in.Enabled), in.Status)
	if err != nil {
		if isDup(err) {
			return 0, cvf("a credential labelled %q already exists for %s", in.Label, in.ExchangeCode)
		}
		return 0, err
	}
	id, _ := res.LastInsertId()
	if err := p.audit(ctx, tx, exID, &id, in.Operator, "create", "", in.Status, 0, in.KeyVersion, in.Reason); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

// RotateInput rotates an exchange's credentials: a NEW credential supersedes the current
// active one. The old plaintext is not needed (only the new secrets are supplied).
type RotateInput struct {
	ExchangeCode string `json:"exchange_code"`
	Label        string `json:"label"` // optional base label for the new row (derived if empty)
	Algorithm    string `json:"algorithm"`
	APIKey       string `json:"api_key"`
	APISecret    string `json:"api_secret"`
	Passphrase   string `json:"passphrase"`
	Reason       string `json:"reason"`
	Operator     string `json:"-"`
}

// Rotate creates a new active credential at key_version = (current max active)+1 and
// DISABLES every previously-active credential for the exchange, all in one transaction, so
// exactly one active credential ever exists (no ambiguity). Both sides are audited. The
// new secrets are encrypted in memory; nothing plaintext is stored/returned.
func (p *Provisioner) Rotate(ctx context.Context, in RotateInput) (int64, error) {
	exID, err := p.exchangeID(ctx, in.ExchangeCode)
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(in.APIKey) == "" || strings.TrimSpace(in.APISecret) == "" {
		return 0, cvf("api_key and api_secret are required for rotation")
	}
	algo := in.Algorithm
	if algo == "" {
		algo = secrets.AlgorithmAESGCM
	}
	if !secrets.SupportedAlgorithm(algo) {
		return 0, cvf("unsupported encryption algorithm %q", in.Algorithm)
	}
	if strings.TrimSpace(in.Reason) == "" {
		return 0, cvf("a reason is required to rotate credentials")
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	// Current active credential (highest key_version), if any.
	var curID sql.NullInt64
	var curLabel sql.NullString
	var curKV sql.NullInt64
	_ = tx.QueryRowContext(ctx,
		"SELECT id, label, key_version FROM exchange_credentials WHERE exchange_id=? AND enabled=1 AND status='active' ORDER BY key_version DESC, id DESC LIMIT 1",
		exID).Scan(&curID, &curLabel, &curKV)

	newKV := 1
	if curKV.Valid {
		newKV = int(curKV.Int64) + 1
	}
	label := strings.TrimSpace(in.Label)
	if label == "" {
		base := "default"
		if curLabel.Valid {
			base = stripVersionSuffix(curLabel.String)
		}
		label = fmt.Sprintf("%s#v%d", base, newKV)
	}

	encKey, encSec, encPass, err := p.encryptFields(in.APIKey, in.APISecret, in.Passphrase)
	if err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, `
INSERT INTO exchange_credentials
  (exchange_id, label, encrypted_api_key, encrypted_api_secret, encrypted_passphrase, encryption_algorithm, key_version, enabled, status)
VALUES (?, ?, ?, ?, ?, ?, ?, 1, 'active')`,
		exID, label, encKey, encSec, encPass, algo, newKV)
	if err != nil {
		if isDup(err) {
			return 0, cvf("a credential labelled %q already exists for %s", label, in.ExchangeCode)
		}
		return 0, err
	}
	newID, _ := res.LastInsertId()
	if err := p.audit(ctx, tx, exID, &newID, in.Operator, "rotate_new", "", "active", int(curKV.Int64), newKV, in.Reason); err != nil {
		return 0, err
	}

	// Disable EVERY previously-active credential (not just the highest) so no two active
	// credentials are ever ambiguous.
	rows, err := tx.QueryContext(ctx, "SELECT id, key_version FROM exchange_credentials WHERE exchange_id=? AND enabled=1 AND status='active' AND id<>?", exID, newID)
	if err != nil {
		return 0, err
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
			return 0, err
		}
		olds = append(olds, o)
	}
	rows.Close()
	for _, o := range olds {
		if _, err := tx.ExecContext(ctx, "UPDATE exchange_credentials SET enabled=0, status='disabled' WHERE id=?", o.id); err != nil {
			return 0, err
		}
		oid := o.id
		if err := p.audit(ctx, tx, exID, &oid, in.Operator, "rotate_disable_old", "active", "disabled", o.kv, o.kv, in.Reason); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return newID, nil
}

// Disable turns a credential off (enabled=0, status='disabled') so PR20a's provider stops
// using it, KEEPING the row (and its audit history) — secrets are not deleted by default.
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
	err = tx.QueryRowContext(ctx, "SELECT exchange_id, status, key_version FROM exchange_credentials WHERE id=?", credentialID).Scan(&exID, &oldStatus, &kv)
	if errors.Is(err, sql.ErrNoRows) {
		return cvf("credential %d not found", credentialID)
	}
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE exchange_credentials SET enabled=0, status='disabled' WHERE id=?", credentialID); err != nil {
		return err
	}
	if err := p.audit(ctx, tx, exID, &credentialID, operator, "disable", oldStatus, "disabled", int(kv), int(kv), reason); err != nil {
		return err
	}
	return tx.Commit()
}

// Validate performs the ONLY allowed credential check — a read-only balance read via the
// narrow BalanceReader (never places/cancels) — then records the result + audits it. The
// caller supplies the read-only client (built by the Builder), so Validate itself does no
// exchange I/O directly.
func (p *Provisioner) Validate(ctx context.Context, credentialID int64, r BalanceReader, operator string) error {
	var exID, kv int64
	var oldStatus string
	err := p.db.QueryRowContext(ctx, "SELECT exchange_id, status, key_version FROM exchange_credentials WHERE id=?", credentialID).Scan(&exID, &oldStatus, &kv)
	if errors.Is(err, sql.ErrNoRows) {
		return cvf("credential %d not found", credentialID)
	}
	if err != nil {
		return err
	}
	newStatus := "active"
	note := any(nil)
	_, balErr := r.GetBalances(ctx)
	if balErr != nil {
		newStatus = "invalid"
		note = "balance validation failed"
	}
	if _, err := p.db.ExecContext(ctx,
		"UPDATE exchange_credentials SET status=?, last_auth_error=?, last_checked_at=? WHERE id=?",
		newStatus, note, p.clk.Now().UTC(), credentialID); err != nil {
		return err
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := p.audit(ctx, tx, exID, &credentialID, operator, "validate", oldStatus, newStatus, int(kv), int(kv), "read-only balance check"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return balErr // surface the auth error (masked) to the caller
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
	if in.KeyVersion <= 0 {
		in.KeyVersion = 1
	}
	if in.Algorithm == "" {
		in.Algorithm = secrets.AlgorithmAESGCM
	}
	if !secrets.SupportedAlgorithm(in.Algorithm) {
		return cvf("unsupported encryption algorithm %q", in.Algorithm)
	}
	if in.Status == "" {
		in.Status = "active"
	}
	if !validCredStatus[in.Status] {
		return cvf("invalid status %q", in.Status)
	}
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
