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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
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
		ExchangeCode: f.code, Label: label,
		APIKey: apiKey, APISecret: apiSecret, Reason: "provision", Operator: "credop",
	})
}

// validate marks a credential validated ('valid') by driving a successful read-only check.
func (f *pfix) markValidated(id int64) {
	f.t.Helper()
	if err := f.prov.ValidateCredential(f.ctx, id, &recordingReader{}, "credop"); err != nil {
		f.t.Fatalf("validate: %v", err)
	}
}

// provision creates + validates + activates a credential (the full safe workflow) and returns its id.
func (f *pfix) provision(label, apiKey, apiSecret string) int64 {
	f.t.Helper()
	id, err := f.create(label, apiKey, apiSecret)
	if err != nil {
		f.t.Fatal(err)
	}
	f.markValidated(id)
	if err := f.prov.ActivateOrRotate(f.ctx, id, "activate", "credop"); err != nil {
		f.t.Fatalf("activate: %v", err)
	}
	return id
}

func (f *pfix) activeCount() int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM exchange_credentials WHERE exchange_id=? AND enabled=1 AND status='active'", f.exID).Scan(&n)
	return n
}

// seedLiveCycle inserts a non-terminal LIVE cycle for the exchange (open operational risk).
func (f *pfix) seedLiveCycle(state string) int64 {
	f.t.Helper()
	u := fmt.Sprintf("%d", time.Now().UnixNano())
	b := f.lastID("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", "B"+u)
	q := f.lastID("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", "Q"+u)
	m := f.lastID("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", "M"+u+"/IRT", b, q)
	em := f.lastID("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, ?)", f.exID, m, "ES"+u, "M"+u+"/IRT")
	return f.lastID("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run) VALUES (?, ?, ?, ?, 0)", em, f.exID, "M"+u+"/IRT", state)
}

// seedOrder inserts an entry_buy order on the target exchange for the cycle in the given state.
func (f *pfix) seedOrder(cycleID int64, state string) int64 {
	f.t.Helper()
	u := fmt.Sprintf("%d", time.Now().UnixNano())
	var em int64
	f.db.QueryRow("SELECT exchange_market_id FROM cycles WHERE id=?", cycleID).Scan(&em)
	return f.lastID("INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, quantity) VALUES (?, ?, ?, 'buy', 'entry_buy', ?, ?, '1')", cycleID, f.exID, em, "lo"+u, state)
}

// seedOtherExchange inserts a DIFFERENT exchange (the wrong one a bad queue row might claim).
func (f *pfix) seedOtherExchange() int64 {
	f.t.Helper()
	return f.lastID("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'O', 1)", fmt.Sprintf("ox%d", time.Now().UnixNano()))
}

// seedRequestWrongMeta inserts an active exchange_request whose OWN exchange_id is wrong and whose
// cycle_id is NULL, but whose order_id points at a real live order on the target exchange.
func (f *pfix) seedRequestWrongMeta(orderID, wrongExchangeID int64, typ, status string) int64 {
	f.t.Helper()
	return f.lastID(
		"INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, symbol, request_type, priority, status, payload, timeout_ms, max_retries, idempotency_key) VALUES (?, NULL, ?, 'X/IRT', ?, 50, ?, '{}', 10000, 5, ?)",
		wrongExchangeID, orderID, typ, status, fmt.Sprintf("wm%d", time.Now().UnixNano()))
}

// racingReader launches a concurrent ActivateOrRotate of `credB` from INSIDE the validation network
// call (while validation holds the credential+exchange locks), then returns a definite auth error.
// The activation MUST block until validation commits, proving serialization (round-3 blocker 1).
type racingReader struct {
	f           *pfix
	credB       int64
	authErr     error
	activateErr *error
	done        chan struct{}
}

func (r *racingReader) GetBalances(context.Context) ([]domain.Balance, error) {
	go func() {
		*r.activateErr = r.f.prov.ActivateOrRotate(context.Background(), r.credB, "activate B", "op")
		close(r.done)
	}()
	time.Sleep(300 * time.Millisecond) // let the goroutine reach + block on the FOR UPDATE lock
	return nil, r.authErr
}

func (f *pfix) lastID(q string, a ...any) int64 {
	r, err := f.db.Exec(q, a...)
	if err != nil {
		f.t.Fatalf("exec %q: %v", q, err)
	}
	id, _ := r.LastInsertId()
	return id
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
	// It round-trips via the credential-specific path (the created row is inactive, so the
	// active-selection path would not see it).
	_, creds, err := f.provider().CredentialsByID(f.ctx, id)
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
	id, err := f.create("default", "k", "s")
	if err != nil {
		t.Fatal(err)
	}
	bad, _ := NewProvider(f.db, "a-totally-different-master-key", clock.NewSystem(), nil)
	if _, _, err := bad.CredentialsByID(f.ctx, id); !errors.Is(err, secrets.ErrDecrypt) {
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
	if op != "credop" || action != "create" || newStatus != "unknown" || newKV != 1 || credID != id {
		t.Errorf("audit = op:%s action:%s status:%s kv:%d cred:%d, want credop/create/unknown/1", op, action, newStatus, newKV, credID)
	}
}

// Req 1: a newly created credential is INACTIVE + UNVALIDATED and cannot be used; enabled/status/
// key_version are service-assigned regardless of anything the caller might want.
func TestCreatedCredentialIsInactiveAndUnusable(t *testing.T) {
	f := setupP(t)
	id, err := f.create("default", "k", "s")
	if err != nil {
		t.Fatal(err)
	}
	en, st, kv := f.cred(id)
	if en != 0 || st != "unknown" || kv != 1 {
		t.Errorf("created cred = enabled:%d status:%s kv:%d, want 0/unknown/1", en, st, kv)
	}
	// The provider must NOT hand out an unvalidated/inactive credential.
	if _, err := f.provider().Credentials(f.ctx, f.code); !errors.Is(err, ErrNoActiveCredential) {
		t.Errorf("provider gave out an inactive credential, err = %v want ErrNoActiveCredential", err)
	}
	// A second create gets key_version 2 (service-assigned, monotonic).
	id2, err := f.create("second", "k2", "s2")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, kv2 := f.cred(id2); kv2 != 2 {
		t.Errorf("second cred key_version = %d, want 2 (service-assigned)", kv2)
	}
}

// Req 2 (exact credential): CredentialsByID decrypts the SELECTED row, never the active one.
func TestCredentialsByIDUsesExactRow(t *testing.T) {
	f := setupP(t)
	old := f.provision("default", "OLD-KEY", "OLD-SEC") // created, validated, activated
	newID, err := f.create("rotated", "NEW-KEY", "NEW-SEC")
	if err != nil {
		t.Fatal(err)
	}
	// The auto-selected active credential is the OLD one, but CredentialsByID(newID) must decrypt NEW.
	_, creds, err := f.provider().CredentialsByID(f.ctx, newID)
	if err != nil {
		t.Fatal(err)
	}
	if creds.APIKey != "NEW-KEY" {
		t.Errorf("CredentialsByID(new) = %q, want NEW-KEY (the exact row, not the active one)", creds.APIKey)
	}
	if _, activeCreds, _ := f.provider().CredentialsByID(f.ctx, old); activeCreds.APIKey != "OLD-KEY" {
		t.Error("CredentialsByID(old) should still decrypt OLD-KEY")
	}
}

// Req 3: ActivateOrRotate activates the selected validated credential and disables the old one; a
// credential that has NOT been validated cannot be activated.
func TestActivateOrRotateAtomic(t *testing.T) {
	f := setupP(t)
	oldID := f.provision("default", "old-key", "old-sec")
	newID, err := f.create("rotated", "new-key", "new-sec")
	if err != nil {
		t.Fatal(err)
	}
	// Cannot activate the new one before it is validated.
	if err := f.prov.ActivateOrRotate(f.ctx, newID, "rotate", "credop"); !IsCredentialValidation(err) {
		t.Fatalf("activating an unvalidated credential err = %v, want validation refusal", err)
	}
	if f.activeCount() != 1 {
		t.Fatal("a refused activation must not disable the old credential")
	}
	// Validate, then activate → old disabled, new active, exactly one active.
	f.markValidated(newID)
	if err := f.prov.ActivateOrRotate(f.ctx, newID, "rotate", "credop"); err != nil {
		t.Fatal(err)
	}
	if oe, os, _ := f.cred(oldID); oe != 0 || os != "disabled" {
		t.Errorf("old cred = %d/%s, want 0/disabled", oe, os)
	}
	if ne, ns, _ := f.cred(newID); ne != 1 || ns != "active" {
		t.Errorf("new cred = %d/%s, want 1/active", ne, ns)
	}
	if f.activeCount() != 1 {
		t.Errorf("active credentials = %d, want exactly 1", f.activeCount())
	}
	if creds, _ := f.provider().Credentials(f.ctx, f.code); creds.APIKey != "new-key" {
		t.Errorf("provider resolved %q, want new-key", creds.APIKey)
	}
}

// Req 5 + 8: if the transaction faults just before commit, NEITHER the credential state change NOR
// its audit survives — the old credential stays active.
func TestRotationRollsBackStateAndAuditTogether(t *testing.T) {
	f := setupP(t)
	oldID := f.provision("default", "old-key", "old-sec")
	newID, err := f.create("rotated", "new-key", "new-sec")
	if err != nil {
		t.Fatal(err)
	}
	f.markValidated(newID)
	beforeActivate := f.auditCount("activate")
	f.prov.faultBeforeCommit = func() error { return errors.New("injected: crash before credential commit") }
	if err := f.prov.ActivateOrRotate(f.ctx, newID, "rotate", "credop"); err == nil {
		t.Fatal("activate must fail when the pre-commit fault fires")
	}
	f.prov.faultBeforeCommit = nil
	// Old still active, new not activated, and NO activate/disable audit rows were written.
	if oe, os, _ := f.cred(oldID); oe != 1 || os != "active" {
		t.Errorf("old cred = %d/%s after rollback, want 1/active", oe, os)
	}
	if ne, ns, _ := f.cred(newID); ne != 0 || ns != "valid" {
		t.Errorf("new cred = %d/%s after rollback, want 0/valid (unchanged)", ne, ns)
	}
	if f.auditCount("activate") != beforeActivate {
		t.Error("a rolled-back rotation must not leave an audit row (state + audit atomic)")
	}
}

// Req 6: two concurrent rotations leave exactly ONE active credential.
func TestConcurrentRotationsOneActive(t *testing.T) {
	f := setupP(t)
	a := f.provision("aaa", "key-a", "sec-a") // starts active
	b, err := f.create("bbb", "key-b", "sec-b")
	if err != nil {
		t.Fatal(err)
	}
	f.markValidated(b)
	// A concurrently re-activates A while B activates B. The per-exchange FOR UPDATE lock serializes
	// them and the DB active-guard is the backstop — the end state must be exactly one active.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = f.prov.ActivateOrRotate(f.ctx, a, "keep a", "op") }()
	go func() { defer wg.Done(); _ = f.prov.ActivateOrRotate(f.ctx, b, "switch to b", "op") }()
	wg.Wait()
	if f.activeCount() != 1 {
		t.Errorf("active credentials after concurrent rotations = %d, want exactly 1", f.activeCount())
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

// Req 2 + 3: a successful validation is read-only, audited, and marks the credential 'valid'
// (validated, NOT yet active).
func TestValidateIsReadOnlyAndMarksValid(t *testing.T) {
	f := setupP(t)
	id, err := f.create("default", "k", "s")
	if err != nil {
		t.Fatal(err)
	}
	rr := &recordingReader{}
	if err := f.prov.ValidateCredential(f.ctx, id, rr, "credop"); err != nil {
		t.Fatal(err)
	}
	if rr.balCalls != 1 {
		t.Errorf("validate should do exactly one balance read, got %d", rr.balCalls)
	}
	// recordingReader exposes no place/cancel — proven at compile time by the BalanceReader interface.
	if f.auditCount("validate") != 1 {
		t.Error("validate must be audited")
	}
	if en, st, _ := f.cred(id); st != "valid" || en != 0 {
		t.Errorf("validated cred = enabled:%d status:%s, want 0/valid (validated but not active)", en, st)
	}
}

// Req 2: a DEFINITE auth error marks the credential 'invalid'.
func TestValidateDefiniteAuthMarksInvalid(t *testing.T) {
	f := setupP(t)
	id, err := f.create("default", "k", "s")
	if err != nil {
		t.Fatal(err)
	}
	authErr := &exchanges.NormalizedAPIError{Exchange: f.code, Op: "GetBalances", Category: exchanges.CatAuth, Err: execution.ErrAuthFailed}
	_ = f.prov.ValidateCredential(f.ctx, id, &recordingReader{err: authErr}, "credop")
	if _, st, _ := f.cred(id); st != "invalid" {
		t.Errorf("status after a definite auth rejection = %q, want invalid", st)
	}
}

// Req 4 (validation): a TRANSIENT error must NOT permanently invalidate a good credential.
func TestValidateTransientErrorDoesNotInvalidate(t *testing.T) {
	f := setupP(t)
	// First validate OK so status is 'valid', then hit a transient error.
	id := f.provision("default", "k", "s") // valid → active
	// A network/timeout style error is not a definite auth rejection.
	transient := errors.New("dial tcp: i/o timeout")
	_ = f.prov.ValidateCredential(f.ctx, id, &recordingReader{err: transient}, "credop")
	if _, st, _ := f.cred(id); st != "active" {
		t.Errorf("status after a transient error = %q, want unchanged (active) — a blip must not invalidate", st)
	}
}

// Req 4 (disable guard): the LAST usable credential cannot be disabled while the exchange has open
// live risk; disabling is allowed once the risk is gone or another usable credential exists.
func TestDisableLastCredentialBlockedByOpenRisk(t *testing.T) {
	f := setupP(t)
	id := f.provision("default", "k", "s") // the only usable credential, active
	cyc := f.seedLiveCycle("BUY_FILLED")   // a non-terminal live cycle = open risk
	if err := f.prov.Disable(f.ctx, id, "retire", "op"); !IsCredentialValidation(err) {
		t.Fatalf("disabling the last credential under open risk err = %v, want refusal", err)
	}
	if en, _, _ := f.cred(id); en != 1 {
		t.Error("a refused disable must leave the credential enabled")
	}
	// Once the cycle is terminal, disabling is allowed.
	f.db.Exec("UPDATE cycles SET state='CLOSED' WHERE id=?", cyc)
	if err := f.prov.Disable(f.ctx, id, "retire", "op"); err != nil {
		t.Fatalf("disable after risk cleared: %v", err)
	}
	if en, st, _ := f.cred(id); en != 0 || st != "disabled" {
		t.Errorf("cred after disable = %d/%s, want 0/disabled", en, st)
	}
}

// Round-2 blocker 2a: a manually-enabled VALID credential is NOT operational (the live provider
// loads only enabled+ACTIVE), so it does NOT permit disabling the real active credential under risk.
func TestDisableActiveBlockedEvenWithValidReplacement(t *testing.T) {
	f := setupP(t)
	active := f.provision("default", "k", "s") // the operational (enabled+active) credential
	other, err := f.create("backup", "k2", "s2")
	if err != nil {
		t.Fatal(err)
	}
	f.markValidated(other)                                                   // status 'valid', enabled=0
	f.db.Exec("UPDATE exchange_credentials SET enabled=1 WHERE id=?", other) // enabled + 'valid' (NOT active)
	f.seedLiveCycle("BUY_FILLED")                                            // open live risk
	if err := f.prov.Disable(f.ctx, active, "retire", "op"); !IsCredentialValidation(err) {
		t.Fatalf("disabling the active credential with only a VALID (not ACTIVE) replacement err = %v, want refusal", err)
	}
	if en, _, _ := f.cred(active); en != 1 {
		t.Error("a refused disable must leave the active credential enabled")
	}
}

// Disabling a NON-active credential is always allowed (it is not the operational one).
func TestDisableNonActiveAllowedUnderRisk(t *testing.T) {
	f := setupP(t)
	f.provision("default", "k", "s") // the active credential (untouched)
	other, err := f.create("backup", "k2", "s2")
	if err != nil {
		t.Fatal(err)
	}
	f.markValidated(other)                                                   // 'valid', enabled=0 (not operational)
	f.db.Exec("UPDATE exchange_credentials SET enabled=1 WHERE id=?", other) // enabled 'valid'
	f.seedLiveCycle("BUY_FILLED")
	if err := f.prov.Disable(f.ctx, other, "retire the spare", "op"); err != nil {
		t.Fatalf("disabling a non-active credential should be allowed even under risk: %v", err)
	}
}

// A successful validation of an already-ACTIVE credential must keep it ACTIVE (never downgrade the
// live credential to 'valid', which would orphan the exchange). (With PR22 round-3 serialization a
// concurrent activation can no longer interleave a validation at all — see
// TestValidateSerializedAgainstConcurrentActivation — so this checks the status logic directly.)
func TestValidateKeepsActiveCredentialActive(t *testing.T) {
	f := setupP(t)
	id := f.provision("default", "k", "s") // enabled + active
	if err := f.prov.ValidateCredential(f.ctx, id, &recordingReader{}, "op"); err != nil {
		t.Fatal(err)
	}
	if en, st, _ := f.cred(id); en != 1 || st != "active" {
		t.Errorf("re-validated active credential = enabled:%d status:%s, want 1/active (not downgraded to valid)", en, st)
	}
	if f.activeCount() != 1 {
		t.Errorf("exactly one enabled+active credential must remain, got %d", f.activeCount())
	}
}

// Round-3 blocker 1: a definite-auth-failure validation running CONCURRENTLY with an activation of
// the same credential must not be able to leave the exchange with zero active credentials.
// Validation holds the credential+exchange locks across its network call, so the activation blocks
// until validation commits (marking B invalid); the activation then safely refuses the now-invalid
// credential. A stays active throughout.
func TestValidateSerializedAgainstConcurrentActivation(t *testing.T) {
	f := setupP(t)
	a := f.provision("cred-a", "ka", "sa") // A: enabled + active
	b, err := f.create("cred-b", "kb", "sb")
	if err != nil {
		t.Fatal(err)
	}
	f.markValidated(b) // B: 'valid', inactive
	authErr := &exchanges.NormalizedAPIError{Exchange: f.code, Op: "GetBalances", Category: exchanges.CatAuth, Err: execution.ErrAuthFailed}
	var activateErr error
	done := make(chan struct{})
	reader := &racingReader{f: f, credB: b, authErr: authErr, activateErr: &activateErr, done: done}

	// Validate B: holds the locks during GetBalances; a definite auth error marks B invalid, commit.
	_ = f.prov.ValidateCredential(f.ctx, b, reader, "op")
	<-done // the concurrent activation finishes only AFTER validation released the locks

	if en, st, _ := f.cred(b); en == 1 || st != "invalid" {
		t.Errorf("B = enabled:%d status:%s, want inactive + invalid", en, st)
	}
	if en, st, _ := f.cred(a); en != 1 || st != "active" {
		t.Errorf("A = enabled:%d status:%s, want 1/active (never deactivated by the racing validation)", en, st)
	}
	if f.activeCount() != 1 {
		t.Errorf("active credentials = %d, want exactly 1 (never zero)", f.activeCount())
	}
	if !IsCredentialValidation(activateErr) {
		t.Errorf("the concurrent activation err = %v, want a refusal (B was invalidated first)", activateErr)
	}
}

// Round-2 blocker 2b: an active request is detected through its actual persisted ORDER even when the
// queue row's own exchange_id/cycle_id are wrong or missing.
func TestDisableBlockedByActiveRequestViaOrderOwnership(t *testing.T) {
	for _, status := range []string{"QUEUED", "RETRY_SCHEDULED", "CLAIMED", "IN_FLIGHT"} {
		for _, typ := range []string{"PLACE_ORDER", "GET_ORDER"} {
			t.Run(status+"_"+typ, func(t *testing.T) {
				f := setupP(t)
				active := f.provision("default", "k", "s")
				// Isolate the request as the SOLE risk: the cycle and order are TERMINAL (so neither
				// the live-cycle nor the live-order check fires), leaving only the active request —
				// which must be found via its ORDER's exchange, not the request row's wrong metadata.
				cyc := f.seedLiveCycle("CLOSED") // dry_run=0 but terminal
				ord := f.seedOrder(cyc, "FILLED")
				other := f.seedOtherExchange() // a DIFFERENT exchange
				// The request row claims the WRONG exchange and NO cycle, but its order_id is the real
				// order on the target exchange.
				f.seedRequestWrongMeta(ord, other, typ, status)
				if err := f.prov.Disable(f.ctx, active, "retire", "op"); !IsCredentialValidation(err) {
					t.Fatalf("%s/%s: disable err = %v, want refusal (active request owned via its order on the target exchange)", status, typ, err)
				}
			})
		}
	}
}

// Req 9: neither audit rows nor any credential column exposes plaintext secrets.
func TestNoPlaintextSecretStoredOrAudited(t *testing.T) {
	f := setupP(t)
	id := f.provision("default", "SUPER-SECRET-KEY", "SUPER-SECRET-VALUE")
	// The credentials table stores only ciphertext (never the plaintext).
	var encKey []byte
	f.db.QueryRow("SELECT encrypted_api_key FROM exchange_credentials WHERE id=?", id).Scan(&encKey)
	if strings.Contains(string(encKey), "SUPER-SECRET") {
		t.Error("encrypted_api_key must not contain the plaintext")
	}
	// No audit row (any column) contains the secret.
	rows, _ := f.db.Query("SELECT COALESCE(reason,''), COALESCE(old_status,''), COALESCE(new_status,'') FROM credential_audit WHERE exchange_id=?", f.exID)
	defer rows.Close()
	for rows.Next() {
		var a, b, c string
		rows.Scan(&a, &b, &c)
		if strings.Contains(a+b+c, "SUPER-SECRET") {
			t.Error("a credential_audit row leaked a secret")
		}
	}
}
