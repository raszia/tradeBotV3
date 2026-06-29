package credentials

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/migrate"
	"v3TradeBot/internal/secrets"
)

type pfix struct {
	t    *testing.T
	ctx  context.Context
	db   *sql.DB
	log  *bytes.Buffer
	prov *Provisioner
	code string
	exID int64
}

func setupP(t *testing.T) *pfix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the provisioning integration test")
	}
	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := migrate.Run(ctx, db, migrate.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	prov, err := NewProvisioner(db, testMasterKey, clock.NewSystem(), logger)
	if err != nil {
		t.Fatal(err)
	}
	code := fmt.Sprintf("pex%d", time.Now().UnixNano())
	r, err := db.Exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'P', 1)", code)
	if err != nil {
		t.Fatalf("seed exchange: %v", err)
	}
	id, _ := r.LastInsertId()
	return &pfix{t: t, ctx: ctx, db: db, log: buf, prov: prov, code: code, exID: id}
}

func (f *pfix) blob(credID int64, col string) []byte {
	var b []byte
	f.db.QueryRow("SELECT "+col+" FROM exchange_credentials WHERE id=?", credID).Scan(&b)
	return b
}
func (f *pfix) cred(credID int64) (enabled int, status string, kv int) {
	f.db.QueryRow("SELECT enabled, status, key_version FROM exchange_credentials WHERE id=?", credID).Scan(&enabled, &status, &kv)
	return
}
func (f *pfix) auditCount(action string) int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM credential_audit WHERE exchange_id=? AND action=?", f.exID, action).Scan(&n)
	return n
}
func (f *pfix) provider() *Provider {
	p, _ := NewProvider(f.db, testMasterKey, clock.NewSystem(), nil)
	return p
}

func (f *pfix) create(label, apiKey, apiSecret string) (int64, error) {
	return f.prov.Create(f.ctx, CreateInput{
		ExchangeCode: f.code, Label: label, Enabled: true, Status: "active",
		APIKey: apiKey, APISecret: apiSecret, Reason: "provision", Operator: "credop",
	})
}

// ---- tests ----

func TestCreateEncryptsAndInsertsNoPlaintext(t *testing.T) {
	f := setupP(t)
	const plainKey, plainSec = "PLAINKEY-abc123", "PLAINSECRET-xyz789"
	id, err := f.create("default", plainKey, plainSec)
	if err != nil {
		t.Fatal(err)
	}
	// The stored blobs are ciphertext: never equal to / containing the plaintext.
	keyBlob, secBlob := f.blob(id, "encrypted_api_key"), f.blob(id, "encrypted_api_secret")
	if len(keyBlob) == 0 || bytes.Contains(keyBlob, []byte(plainKey)) {
		t.Error("api key stored as plaintext / empty")
	}
	if bytes.Contains(secBlob, []byte(plainSec)) {
		t.Error("api secret stored as plaintext")
	}
	// It round-trips through the PR20a provider with the same master key.
	creds, err := f.provider().Credentials(f.ctx, f.code)
	if err != nil {
		t.Fatal(err)
	}
	if creds.APIKey != plainKey || creds.APISecret != plainSec {
		t.Errorf("decrypted = %q/%q, want %q/%q", creds.APIKey, creds.APISecret, plainKey, plainSec)
	}
	// Plaintext must never appear in logs.
	if strings.Contains(f.log.String(), plainKey) || strings.Contains(f.log.String(), plainSec) {
		t.Error("plaintext secret leaked into logs")
	}
}

func TestCreateWrongMasterKeyCannotDecrypt(t *testing.T) {
	f := setupP(t)
	if _, err := f.create("default", "k", "s"); err != nil {
		t.Fatal(err)
	}
	bad, _ := NewProvider(f.db, "a-totally-different-master-key", clock.NewSystem(), nil)
	if _, err := bad.Credentials(f.ctx, f.code); !errors.Is(err, secrets.ErrDecrypt) {
		t.Errorf("wrong master key err = %v, want ErrDecrypt", err)
	}
}

func TestCreateDuplicateLabelRejected(t *testing.T) {
	f := setupP(t)
	if _, err := f.create("default", "k", "s"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.create("default", "k2", "s2"); !IsCredentialValidation(err) {
		t.Errorf("duplicate label err = %v, want CredentialValidationError", err)
	}
}

func TestCreateValidatesInput(t *testing.T) {
	f := setupP(t)
	// missing api key
	if _, err := f.prov.Create(f.ctx, CreateInput{ExchangeCode: f.code, Label: "l", APISecret: "s", Reason: "r", Operator: "op"}); !IsCredentialValidation(err) {
		t.Errorf("missing api key err = %v, want validation", err)
	}
	// missing reason
	if _, err := f.prov.Create(f.ctx, CreateInput{ExchangeCode: f.code, Label: "l", APIKey: "k", APISecret: "s", Operator: "op"}); !IsCredentialValidation(err) {
		t.Errorf("missing reason err = %v, want validation", err)
	}
	// unknown exchange
	if _, err := f.prov.Create(f.ctx, CreateInput{ExchangeCode: "nope", Label: "l", APIKey: "k", APISecret: "s", Reason: "r", Operator: "op"}); !IsCredentialValidation(err) {
		t.Errorf("unknown exchange err = %v, want validation", err)
	}
}

func TestCreateAuditWritten(t *testing.T) {
	f := setupP(t)
	id, err := f.create("default", "k", "s")
	if err != nil {
		t.Fatal(err)
	}
	var op, action, newStatus string
	var newKV int
	var credID int64
	f.db.QueryRow("SELECT operator, action, new_status, new_key_version, credential_id FROM credential_audit WHERE exchange_id=? ORDER BY id DESC LIMIT 1", f.exID).
		Scan(&op, &action, &newStatus, &newKV, &credID)
	if op != "credop" || action != "create" || newStatus != "active" || newKV != 1 || credID != id {
		t.Errorf("audit = op:%s action:%s status:%s kv:%d cred:%d", op, action, newStatus, newKV, credID)
	}
}

func TestRotationActivatesNewDisablesOld(t *testing.T) {
	f := setupP(t)
	oldID, err := f.create("default", "old-key", "old-sec")
	if err != nil {
		t.Fatal(err)
	}
	newID, err := f.prov.Rotate(f.ctx, RotateInput{ExchangeCode: f.code, APIKey: "new-key", APISecret: "new-sec", Reason: "rotate", Operator: "credop"})
	if err != nil {
		t.Fatal(err)
	}
	// Old disabled, new active with higher key_version.
	oe, os, _ := f.cred(oldID)
	ne, ns, nkv := f.cred(newID)
	if oe != 0 || os != "disabled" {
		t.Errorf("old cred = enabled:%d status:%s, want 0/disabled", oe, os)
	}
	if ne != 1 || ns != "active" || nkv != 2 {
		t.Errorf("new cred = enabled:%d status:%s kv:%d, want 1/active/2", ne, ns, nkv)
	}
	// The provider now resolves to the NEW secret (no ambiguity).
	creds, err := f.provider().Credentials(f.ctx, f.code)
	if err != nil {
		t.Fatal(err)
	}
	if creds.APIKey != "new-key" {
		t.Errorf("provider resolved %q, want new-key", creds.APIKey)
	}
	// Exactly one active credential remains.
	var active int
	f.db.QueryRow("SELECT COUNT(*) FROM exchange_credentials WHERE exchange_id=? AND enabled=1 AND status='active'", f.exID).Scan(&active)
	if active != 1 {
		t.Errorf("active credentials = %d, want exactly 1", active)
	}
	if f.auditCount("rotate_new") != 1 || f.auditCount("rotate_disable_old") != 1 {
		t.Error("rotation must audit both the new and the disabled-old credential")
	}
}

func TestDisableIgnoredByProviderKeepsRow(t *testing.T) {
	f := setupP(t)
	id, err := f.create("default", "k", "s")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.prov.Disable(f.ctx, id, "compromised", "credop"); err != nil {
		t.Fatal(err)
	}
	// The PR20a provider must ignore a disabled credential.
	if _, err := f.provider().Credentials(f.ctx, f.code); !errors.Is(err, ErrNoActiveCredential) {
		t.Errorf("disabled credential should be ignored, err = %v", err)
	}
	// The row is KEPT (secrets not deleted).
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM exchange_credentials WHERE id=?", id).Scan(&n)
	if n != 1 {
		t.Error("disable must keep the credential row")
	}
	if f.auditCount("disable") != 1 {
		t.Error("disable must be audited")
	}
}

func TestNoMasterKeyDisablesProvisioner(t *testing.T) {
	if _, err := NewProvisioner(nil, "", clock.NewSystem(), nil); !errors.Is(err, secrets.ErrNoMasterKey) {
		t.Errorf("empty master key err = %v, want ErrNoMasterKey", err)
	}
}

// recordingReader is a read-only client that counts ALL calls — it cannot place/cancel
// (the BalanceReader interface lacks those methods).
type recordingReader struct {
	balCalls int32
	err      error
}

func (r *recordingReader) GetBalances(context.Context) ([]domain.Balance, error) {
	atomic.AddInt32(&r.balCalls, 1)
	return nil, r.err
}

func TestValidateIsReadOnlyAndAudits(t *testing.T) {
	f := setupP(t)
	id, err := f.create("default", "k", "s")
	if err != nil {
		t.Fatal(err)
	}
	rr := &recordingReader{}
	if err := f.prov.Validate(f.ctx, id, rr, "credop"); err != nil {
		t.Fatal(err)
	}
	if rr.balCalls != 1 {
		t.Errorf("validate should do exactly one balance read, got %d", rr.balCalls)
	}
	// recordingReader exposes no place/cancel — proven at compile time by the interface.
	if f.auditCount("validate") != 1 {
		t.Error("validate must be audited")
	}
	var status string
	f.db.QueryRow("SELECT status FROM exchange_credentials WHERE id=?", id).Scan(&status)
	if status != "active" {
		t.Errorf("successful validate status = %q, want active", status)
	}
}
