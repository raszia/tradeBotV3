package credentials

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
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

const testMasterKey = "credentials-test-master-key"

// credProbe is a fake private adapter registered with the factory. On a balance read it
// fetches credentials via the injected provider (cfg.Creds), proving factory injection +
// in-memory decryption without any network. It counts mutating calls so a test can prove
// validation never places/cancels.
type credProbe struct {
	cfg                exchanges.ClientConfig
	place, cancel, bal int32
	gotKey, gotSecret  string
}

func (c *credProbe) Name() string { return "credprobe" }
func (c *credProbe) Capabilities() exchanges.Capabilities {
	return exchanges.Capabilities{BalanceFetch: true, PlaceOrder: true, CancelByOrderID: true}
}
func (c *credProbe) GetBalances(ctx context.Context) ([]domain.Balance, error) {
	atomic.AddInt32(&c.bal, 1)
	creds, err := c.cfg.Creds.Credentials(ctx, "credprobe")
	if err != nil {
		return nil, err
	}
	c.gotKey, c.gotSecret = creds.APIKey, creds.APISecret
	return nil, nil
}
func (c *credProbe) PlaceOrder(context.Context, execution.OrderRequest) (execution.OrderAck, error) {
	atomic.AddInt32(&c.place, 1)
	return execution.OrderAck{}, nil
}
func (c *credProbe) CancelOrder(context.Context, string) error {
	atomic.AddInt32(&c.cancel, 1)
	return nil
}
func (c *credProbe) GetOrder(context.Context, string) (execution.OrderStatus, error) {
	return execution.OrderStatus{}, nil
}
func (c *credProbe) GetOpenOrders(context.Context, string) ([]execution.OrderStatus, error) {
	return nil, nil
}
func (c *credProbe) SubscribeOrderUpdates(context.Context) (<-chan execution.NormalizedOrderEvent, error) {
	return nil, exchanges.Unsupported("credprobe", "order updates")
}

func init() {
	exchanges.Register(exchanges.Registration{
		Code:         "credprobe",
		Capabilities: exchanges.Capabilities{BalanceFetch: true, PlaceOrder: true, CancelByOrderID: true},
		NewPrivate: func(cfg exchanges.ClientConfig, _ *exchanges.IOLogger) (exchanges.PrivateClient, error) {
			return &credProbe{cfg: cfg}, nil
		},
	})
}

// ---- fixture ----

type cfix struct {
	t      *testing.T
	ctx    context.Context
	db     *sql.DB
	cipher *secrets.Cipher
	exID   int64
}

func setupC(t *testing.T) *cfix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the credentials integration test")
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
	cipher, _ := secrets.NewCipher(testMasterKey)
	f := &cfix{t: t, ctx: ctx, db: db, cipher: cipher}
	f.exID = f.seedExchange()
	return f
}

func (f *cfix) seedExchange() int64 {
	code := fmt.Sprintf("credprobe%d", time.Now().UnixNano())
	// The factory adapter is registered under "credprobe"; the DB row's code must match
	// for Credentials(ctx,"credprobe") to resolve. Use the literal code + clean any prior.
	f.db.Exec("DELETE FROM exchange_credentials WHERE exchange_id IN (SELECT id FROM exchanges WHERE code='credprobe')")
	f.db.Exec("DELETE FROM exchanges WHERE code='credprobe'")
	_ = code
	r, err := f.db.Exec("INSERT INTO exchanges (code, name, enabled, live_enabled) VALUES ('credprobe', 'CredProbe', 1, 1)")
	if err != nil {
		f.t.Fatalf("seed exchange: %v", err)
	}
	id, _ := r.LastInsertId()
	return id
}

func (f *cfix) enc(s string) []byte {
	b, err := f.cipher.Encrypt([]byte(s))
	if err != nil {
		f.t.Fatal(err)
	}
	return b
}

// seedCred inserts one credential row (api_key/secret encrypted with the test cipher).
func (f *cfix) seedCred(label string, enabled int, status string, keyVersion int, apiKey, apiSecret, algo string) {
	if algo == "" {
		algo = secrets.AlgorithmAESGCM
	}
	_, err := f.db.Exec(`INSERT INTO exchange_credentials
		(exchange_id, label, encrypted_api_key, encrypted_api_secret, encryption_algorithm, key_version, enabled, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		f.exID, label, f.enc(apiKey), f.enc(apiSecret), algo, keyVersion, enabled, status)
	if err != nil {
		f.t.Fatalf("seed cred: %v", err)
	}
}

func (f *cfix) provider() *Provider {
	p, err := NewProvider(f.db, testMasterKey, clock.NewSystem(), nil)
	if err != nil {
		f.t.Fatalf("provider: %v", err)
	}
	return p
}

func (f *cfix) status(label string) string {
	var s string
	f.db.QueryRow("SELECT status FROM exchange_credentials WHERE exchange_id=? AND label=?", f.exID, label).Scan(&s)
	return s
}

// ---- tests ----

func TestDecryptValidCredential(t *testing.T) {
	f := setupC(t)
	f.seedCred("default", 1, "active", 1, "the-api-key", "the-secret", "")
	creds, err := f.provider().Credentials(f.ctx, "credprobe")
	if err != nil {
		t.Fatal(err)
	}
	if creds.APIKey != "the-api-key" || creds.APISecret != "the-secret" {
		t.Errorf("decrypted = %q/%q, want the-api-key/the-secret", creds.APIKey, creds.APISecret)
	}
}

func TestWrongMasterKeyFailsSafelyAndMarksUnusable(t *testing.T) {
	f := setupC(t)
	f.seedCred("default", 1, "active", 1, "the-api-key", "the-secret", "")
	// A provider with a DIFFERENT master key cannot decrypt.
	bad, _ := NewProvider(f.db, "the-WRONG-master-key", clock.NewSystem(), nil)
	_, err := bad.Credentials(f.ctx, "credprobe")
	if !errors.Is(err, secrets.ErrDecrypt) {
		t.Fatalf("err = %v, want ErrDecrypt", err)
	}
	// The error carries no plaintext, and the row is marked unusable.
	if got := f.status("default"); got != "error" {
		t.Errorf("status after decrypt failure = %q, want error", got)
	}
}

func TestMissingMasterKeyDisablesProvider(t *testing.T) {
	f := setupC(t)
	if _, err := NewProvider(f.db, "", clock.NewSystem(), nil); !errors.Is(err, secrets.ErrNoMasterKey) {
		t.Errorf("empty master key err = %v, want ErrNoMasterKey", err)
	}
}

func TestUnsupportedAlgorithmFailsSafely(t *testing.T) {
	f := setupC(t)
	f.seedCred("default", 1, "active", 1, "k", "s", "AES-128-CBC")
	_, err := f.provider().Credentials(f.ctx, "credprobe")
	if !errors.Is(err, secrets.ErrUnsupportedAlgorithm) {
		t.Fatalf("err = %v, want ErrUnsupportedAlgorithm", err)
	}
	if got := f.status("default"); got != "error" {
		t.Errorf("status = %q, want error (marked unusable)", got)
	}
}

func TestDisabledCredentialIgnored(t *testing.T) {
	f := setupC(t)
	f.seedCred("default", 0, "active", 1, "k", "s", "") // enabled=0
	if _, err := f.provider().Credentials(f.ctx, "credprobe"); !errors.Is(err, ErrNoActiveCredential) {
		t.Errorf("disabled credential should be ignored, err = %v", err)
	}
	if f.provider().HasActiveCredential(f.ctx, "credprobe") {
		t.Error("HasActiveCredential must be false for a disabled credential")
	}
}

func TestActiveCredentialSelectedOverNonActive(t *testing.T) {
	f := setupC(t)
	f.seedCred("bad", 1, "invalid", 5, "invalid-key", "x", "") // higher version but not active
	f.seedCred("good", 1, "active", 2, "active-key", "y", "")
	creds, err := f.provider().Credentials(f.ctx, "credprobe")
	if err != nil {
		t.Fatal(err)
	}
	if creds.APIKey != "active-key" {
		t.Errorf("selected %q, want active-key (non-active higher version must be ignored)", creds.APIKey)
	}
}

func TestKeyVersionRespectedHighestActiveSelected(t *testing.T) {
	f := setupC(t)
	// PR22 enforces ONE active credential per exchange (DB active-guard). The superseded credential
	// is disabled; the provider selects the single active one.
	f.seedCred("v1", 0, "disabled", 1, "old-key", "x", "")
	f.seedCred("v2", 1, "active", 3, "new-key", "y", "")
	creds, err := f.provider().Credentials(f.ctx, "credprobe")
	if err != nil {
		t.Fatal(err)
	}
	if creds.APIKey != "new-key" {
		t.Errorf("selected %q, want new-key (the single active credential)", creds.APIKey)
	}
}

func TestBuildPrivateInjectsDecryptedCredsViaFactory(t *testing.T) {
	f := setupC(t)
	f.seedCred("default", 1, "active", 1, "factory-key", "factory-secret", "")
	builder := NewBuilder(f.db, f.provider(), nil)
	client, err := builder.BuildPrivate(f.ctx, "credprobe")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	// Calling a read-only method drives the adapter to fetch creds via the provider.
	if _, err := client.GetBalances(f.ctx); err != nil {
		t.Fatal(err)
	}
	probe := client.(*credProbe)
	if probe.gotKey != "factory-key" || probe.gotSecret != "factory-secret" {
		t.Errorf("client received %q/%q, want factory-key/factory-secret", probe.gotKey, probe.gotSecret)
	}
}

func TestBuildPrivateRefusesWithoutActiveCredential(t *testing.T) {
	f := setupC(t)
	// No credential seeded.
	builder := NewBuilder(f.db, f.provider(), nil)
	if _, err := builder.BuildPrivate(f.ctx, "credprobe"); !errors.Is(err, ErrNoActiveCredential) {
		t.Errorf("build without credential err = %v, want ErrNoActiveCredential (no client built)", err)
	}
}

func TestValidateIsReadOnlyNeverPlacesOrCancels(t *testing.T) {
	f := setupC(t)
	f.seedCred("default", 1, "active", 1, "k", "s", "")
	probe := &credProbe{cfg: exchanges.ClientConfig{Creds: f.provider()}}
	if err := f.provider().Validate(f.ctx, "credprobe", probe); err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&probe.bal) != 1 {
		t.Errorf("validate should call GetBalances once, got %d", probe.bal)
	}
	if probe.place != 0 || probe.cancel != 0 {
		t.Errorf("validate must NEVER place/cancel: place=%d cancel=%d", probe.place, probe.cancel)
	}
	// A successful validation stamps the credential active.
	if got := f.status("default"); got != "active" {
		t.Errorf("status after successful validate = %q, want active", got)
	}
}

// balErr is a minimal read-only BalanceReader that returns a fixed error (or nil). It is
// used to prove ProbePrivateHealth's credential-status policy without any network.
type balErr struct {
	err   error
	calls int32
}

func (b *balErr) GetBalances(ctx context.Context) ([]domain.Balance, error) {
	atomic.AddInt32(&b.calls, 1)
	return nil, b.err
}

// apiErr builds a NormalizedAPIError with the given adapter category (as a real adapter
// would emit), for exercising the auth-vs-temporary classification.
func apiErr(cat exchanges.ErrorCategory) error {
	return &exchanges.NormalizedAPIError{Exchange: "credprobe", Op: "GetBalances", Category: cat, Message: "boom"}
}

// TestProbePrivateHealthIsReadOnly proves the continuous probe never places/cancels.
func TestProbePrivateHealthIsReadOnly(t *testing.T) {
	f := setupC(t)
	f.seedCred("default", 1, "active", 1, "k", "s", "")
	probe := &credProbe{cfg: exchanges.ClientConfig{Creds: f.provider()}}
	if err := f.provider().ProbePrivateHealth(f.ctx, "credprobe", probe); err != nil {
		t.Fatal(err)
	}
	if probe.place != 0 || probe.cancel != 0 {
		t.Errorf("probe must NEVER place/cancel: place=%d cancel=%d", probe.place, probe.cancel)
	}
	if got := f.status("default"); got != "active" {
		t.Errorf("status after successful probe = %q, want active", got)
	}
}

// TestProbePrivateHealthCredentialPolicy is the reviewer's blocking-issue matrix: ONLY a
// definite auth error may invalidate a credential; every temporary error must leave it
// 'active' so a brief incident cannot permanently disable live trading.
func TestProbePrivateHealthCredentialPolicy(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus string // credential status AFTER the probe
	}{
		// 1. auth error invalidates (sentinel + adapter-classified auth).
		{"auth-sentinel-invalidates", execution.ErrAuthFailed, "invalid"},
		{"auth-apierr-invalidates", apiErr(exchanges.CatAuth), "invalid"},
		// 2-5. temporary errors must NOT invalidate.
		{"timeout-keeps-active", context.DeadlineExceeded, "active"},
		{"ack-timeout-keeps-active", execution.ErrAckTimeout, "active"},
		{"network-keeps-active", apiErr(exchanges.CatNetwork), "active"},
		{"rate-limit-sentinel-keeps-active", execution.ErrRateLimited, "active"},
		{"rate-limit-apierr-keeps-active", apiErr(exchanges.CatRateLimit), "active"},
		{"exchange-5xx-keeps-active", apiErr(exchanges.CatServer), "active"},
		{"unknown-keeps-active", errors.New("some transient blip"), "active"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := setupC(t)
			f.seedCred("default", 1, "active", 1, "k", "s", "")
			r := &balErr{err: c.err}
			gotErr := f.provider().ProbePrivateHealth(f.ctx, "credprobe", r)
			if !errors.Is(gotErr, c.err) && gotErr != c.err {
				t.Errorf("ProbePrivateHealth returned %v, want the underlying %v", gotErr, c.err)
			}
			if atomic.LoadInt32(&r.calls) != 1 {
				t.Errorf("GetBalances called %d times, want 1", r.calls)
			}
			if got := f.status("default"); got != c.wantStatus {
				t.Errorf("credential status after %q error = %q, want %q", c.name, got, c.wantStatus)
			}
		})
	}
}

// TestProbePrivateHealthSuccessAfterTemporaryFailure covers reviewer case 6: a temporary
// failure leaves the credential active, and a later success keeps it active (never having
// been invalidated in between), so BuildPrivate keeps working throughout an incident.
func TestProbePrivateHealthSuccessAfterTemporaryFailure(t *testing.T) {
	f := setupC(t)
	f.seedCred("default", 1, "active", 1, "k", "s", "")
	p := f.provider()

	// Temporary failure: credential must stay active and buildable.
	if err := p.ProbePrivateHealth(f.ctx, "credprobe", &balErr{err: context.DeadlineExceeded}); err == nil {
		t.Fatal("expected the timeout error to propagate")
	}
	if got := f.status("default"); got != "active" {
		t.Fatalf("credential status after timeout = %q, want active (transient must not invalidate)", got)
	}
	if !p.HasActiveCredential(f.ctx, "credprobe") {
		t.Fatal("credential must remain active/buildable through a transient failure")
	}

	// Recovery: a subsequent success keeps it active.
	if err := p.ProbePrivateHealth(f.ctx, "credprobe", &balErr{err: nil}); err != nil {
		t.Fatalf("recovery probe = %v, want nil", err)
	}
	if got := f.status("default"); got != "active" {
		t.Errorf("credential status after recovery = %q, want active", got)
	}
}

// TestValidateStillStrictForManualCheck pins that the one-shot operator Validate keeps its
// strict "any error → invalid" behaviour (only ProbePrivateHealth is lenient).
func TestValidateStillStrictForManualCheck(t *testing.T) {
	f := setupC(t)
	f.seedCred("default", 1, "active", 1, "k", "s", "")
	// A transient error via the strict manual path DOES invalidate — intentional.
	if err := f.provider().Validate(f.ctx, "credprobe", &balErr{err: context.DeadlineExceeded}); err == nil {
		t.Fatal("expected error")
	}
	if got := f.status("default"); got != "invalid" {
		t.Errorf("manual Validate should stay strict: status = %q, want invalid", got)
	}
}

// TestIsDefiniteAuthError is an offline guard on the classifier that gates invalidation.
func TestIsDefiniteAuthError(t *testing.T) {
	auth := []error{execution.ErrAuthFailed, apiErr(exchanges.CatAuth), fmt.Errorf("wrap: %w", execution.ErrAuthFailed)}
	notAuth := []error{
		nil, context.DeadlineExceeded, execution.ErrRateLimited, execution.ErrAckTimeout,
		apiErr(exchanges.CatNetwork), apiErr(exchanges.CatRateLimit), apiErr(exchanges.CatServer),
		apiErr(exchanges.CatTimeout), errors.New("plain"),
	}
	for _, e := range auth {
		if !isDefiniteAuthError(e) {
			t.Errorf("isDefiniteAuthError(%v) = false, want true", e)
		}
	}
	for _, e := range notAuth {
		if isDefiniteAuthError(e) {
			t.Errorf("isDefiniteAuthError(%v) = true, want false (must not invalidate)", e)
		}
	}
}
