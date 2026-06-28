package health

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/migrate"
)

// ---- offline ----

func jsonSyntaxErr() error {
	var x int
	return json.Unmarshal([]byte("{not json"), &x) // a *json.SyntaxError
}

func TestClassifyMatrix(t *testing.T) {
	apiErr := func(c exchanges.ErrorCategory) error {
		return &exchanges.NormalizedAPIError{Exchange: "x", Op: "GetMarkets", Category: c, Message: "boom"}
	}
	cases := []struct {
		name    string
		err     error
		status  Status
		categry Category
	}{
		{"nil-healthy", nil, StatusHealthy, CatNone},
		{"auth-sentinel", execution.ErrAuthFailed, StatusAuthFailed, CatAuth},
		{"rate-sentinel", execution.ErrRateLimited, StatusRateLimited, CatRateLimit},
		{"timeout-deadline", context.DeadlineExceeded, StatusUnavailable, CatTimeout},
		{"unsupported", exchanges.Unsupported("x", "ws"), StatusDegraded, CatUnsupported},
		{"invalid-response", jsonSyntaxErr(), StatusDegraded, CatInvalidResponse},
		{"api-auth", apiErr(exchanges.CatAuth), StatusAuthFailed, CatAuth},
		{"api-rate", apiErr(exchanges.CatRateLimit), StatusRateLimited, CatRateLimit},
		{"api-timeout", apiErr(exchanges.CatTimeout), StatusUnavailable, CatTimeout},
		{"api-network", apiErr(exchanges.CatNetwork), StatusUnavailable, CatNetwork},
		{"api-5xx", apiErr(exchanges.CatServer), StatusUnavailable, Cat5xx},
		{"api-4xx", apiErr(exchanges.CatBadRequest), StatusDegraded, Cat4xx},
		{"api-notfound-4xx", apiErr(exchanges.CatNotFound), StatusDegraded, Cat4xx},
		{"unknown-plain", errors.New("???"), StatusDegraded, CatUnknown},
		{"canceled-shutdown", context.Canceled, StatusUnknown, CatUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, cat := Classify(c.err)
			if s != c.status || cat != c.categry {
				t.Errorf("Classify(%s) = %s/%s, want %s/%s", c.name, s, cat, c.status, c.categry)
			}
		})
	}
}

// TestMonitorHoldsNoOrderClient is a structural guard: nothing the Monitor (or its
// Targets) holds can place/cancel orders — it only invokes read-only ProbeFuncs.
func TestMonitorHoldsNoOrderClient(t *testing.T) {
	type placer interface{ PlaceOrder(any) any }
	type canceller interface{ CancelOrder(any) any }
	pT := reflect.TypeOf((*placer)(nil)).Elem()
	cT := reflect.TypeOf((*canceller)(nil)).Elem()
	for _, typ := range []reflect.Type{reflect.TypeOf(Monitor{}), reflect.TypeOf(Target{})} {
		for i := 0; i < typ.NumField(); i++ {
			ft := typ.Field(i).Type
			if ft.Implements(pT) || ft.Implements(cT) {
				t.Errorf("%s.%s can place/cancel orders — health must be read-only", typ.Name(), typ.Field(i).Name)
			}
		}
	}
}

func TestNoTargetsStartupSafe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m := NewMonitor(nil, nil, clock.NewSystem(), nil, Config{Interval: time.Hour})
	if err := m.Run(ctx); err != nil {
		t.Errorf("Run with no targets = %v, want nil", err)
	}
}

// ---- gated ----

var hseq int

type hfix struct {
	t   *testing.T
	ctx context.Context
	db  *sql.DB
	rec *Recorder
}

func setupH(t *testing.T) *hfix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the health integration test")
	}
	ctx := context.Background()
	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := migrate.Run(ctx, sqlDB, migrate.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_ = db.NewFromDB(sqlDB)
	return &hfix{t: t, ctx: ctx, db: sqlDB, rec: NewRecorder(sqlDB)}
}

func (f *hfix) exchange() (string, int64) {
	f.t.Helper()
	hseq++
	code := fmt.Sprintf("hm_%d_%d", time.Now().UnixNano(), hseq)
	res, err := f.db.Exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'HM', 1)", code)
	if err != nil {
		f.t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return code, id
}

type curHealth struct {
	public, private, rest, apiKey string
	consec, errCount              int
	timeoutC, rateC, authC        int
	cat                           string
	hasSuccess, hasFailure        bool
}

func (f *hfix) current(exID int64) curHealth {
	var h curHealth
	err := f.db.QueryRow(`SELECT public_status, private_status, rest_status, api_key_status,
		consecutive_failures, error_count, timeout_count, rate_limit_error_count, auth_error_count,
		COALESCE(last_error_category,''), last_success_at IS NOT NULL, last_failure_at IS NOT NULL
		FROM exchange_health_current WHERE exchange_id=?`, exID).
		Scan(&h.public, &h.private, &h.rest, &h.apiKey, &h.consec, &h.errCount, &h.timeoutC, &h.rateC, &h.authC, &h.cat, &h.hasSuccess, &h.hasFailure)
	if err != nil {
		f.t.Fatalf("current: %v", err)
	}
	return h
}

func (f *hfix) samples(exID int64, kind Kind) int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM exchange_health_samples WHERE exchange_id=? AND sample_type=?", exID, string(kind)).Scan(&n)
	return n
}

func probe(err error) ProbeFunc { return func(context.Context) error { return err } }

func (f *hfix) monitor(targets ...Target) *Monitor {
	return NewMonitor(f.rec, targets, clock.NewSystem(), nil, Config{Timeout: time.Second})
}

func TestHealthyPublicProbe(t *testing.T) {
	f := setupH(t)
	code, exID := f.exchange()
	f.monitor(Target{ExchangeCode: code, Public: probe(nil)}).Pass(f.ctx)

	h := f.current(exID)
	if h.public != "HEALTHY" || h.rest != "up" || !h.hasSuccess {
		t.Errorf("healthy current = %+v, want public HEALTHY/rest up/last_success", h)
	}
	if h.private != "UNKNOWN" {
		t.Errorf("private_status = %s, want UNKNOWN (no private probe)", h.private)
	}
	if f.samples(exID, KindPublic) != 1 {
		t.Errorf("public samples = %d, want 1", f.samples(exID, KindPublic))
	}
}

func TestProbeErrorsClassifiedAndCounted(t *testing.T) {
	apiErr := func(c exchanges.ErrorCategory) error {
		return &exchanges.NormalizedAPIError{Exchange: "x", Op: "GetMarkets", Category: c, Message: "boom"}
	}
	cases := []struct {
		name           string
		err            error
		wantStatus     string
		wantCat        string
		timeoutC, rate int
		authC          int
		apiKeyInvalid  bool
	}{
		{"timeout", context.DeadlineExceeded, "UNAVAILABLE", "timeout", 1, 0, 0, false},
		{"auth", execution.ErrAuthFailed, "AUTH_FAILED", "auth", 0, 0, 1, false},
		{"rate", execution.ErrRateLimited, "RATE_LIMITED", "rate_limit", 0, 1, 0, false},
		{"network", apiErr(exchanges.CatNetwork), "UNAVAILABLE", "network", 0, 0, 0, false},
		{"5xx", apiErr(exchanges.CatServer), "UNAVAILABLE", "exchange_5xx", 0, 0, 0, false},
		{"invalid", jsonSyntaxErr(), "DEGRADED", "invalid_response", 0, 0, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := setupH(t)
			code, exID := f.exchange()
			f.monitor(Target{ExchangeCode: code, Public: probe(c.err)}).Pass(f.ctx)
			h := f.current(exID)
			if h.public != c.wantStatus || h.cat != c.wantCat {
				t.Errorf("%s: public=%s cat=%s, want %s/%s", c.name, h.public, h.cat, c.wantStatus, c.wantCat)
			}
			if h.consec != 1 || h.errCount != 1 || !h.hasFailure {
				t.Errorf("%s: consec=%d errs=%d failure=%v, want 1/1/true", c.name, h.consec, h.errCount, h.hasFailure)
			}
			if h.timeoutC != c.timeoutC || h.rateC != c.rate || h.authC != c.authC {
				t.Errorf("%s: counters t=%d r=%d a=%d, want %d/%d/%d", c.name, h.timeoutC, h.rateC, h.authC, c.timeoutC, c.rate, c.authC)
			}
		})
	}
}

func TestPrivateAuthFailMarksKeyInvalid(t *testing.T) {
	f := setupH(t)
	code, exID := f.exchange()
	f.monitor(Target{ExchangeCode: code, Public: probe(nil), Private: probe(execution.ErrAuthFailed)}).Pass(f.ctx)
	h := f.current(exID)
	if h.private != "AUTH_FAILED" || h.apiKey != "invalid" {
		t.Errorf("private=%s apiKey=%s, want AUTH_FAILED / invalid", h.private, h.apiKey)
	}
	// Public stayed healthy — public health does not prove private and vice-versa.
	if h.public != "HEALTHY" {
		t.Errorf("public=%s, want HEALTHY (independent of private)", h.public)
	}
}

func TestFailureIsolation(t *testing.T) {
	f := setupH(t)
	codeA, exA := f.exchange()
	codeB, exB := f.exchange()
	f.monitor(
		Target{ExchangeCode: codeA, Public: probe(errors.New("down"))},
		Target{ExchangeCode: codeB, Public: probe(nil)},
	).Pass(f.ctx)
	if f.current(exA).public == "HEALTHY" {
		t.Error("A should be unhealthy")
	}
	if f.current(exB).public != "HEALTHY" {
		t.Error("B must still be probed/healthy despite A failing")
	}
}

func TestConsecutiveFailuresThenReset(t *testing.T) {
	f := setupH(t)
	code, exID := f.exchange()
	fp := &mutProbe{err: errors.New("down")}
	m := f.monitor(Target{ExchangeCode: code, Public: fp.run})
	m.Pass(f.ctx)
	m.Pass(f.ctx)
	if h := f.current(exID); h.consec != 2 {
		t.Errorf("consecutive_failures = %d, want 2", h.consec)
	}
	// Recover.
	fp.err = nil
	m.Pass(f.ctx)
	h := f.current(exID)
	if h.consec != 0 || !h.hasSuccess {
		t.Errorf("after recovery: consec=%d hasSuccess=%v, want 0/true", h.consec, h.hasSuccess)
	}
	if !h.hasFailure {
		t.Error("last_failure_at must be preserved across a later success (history kept)")
	}
}

func TestLastSuccessPreservedAcrossFailure(t *testing.T) {
	f := setupH(t)
	code, exID := f.exchange()
	fp := &mutProbe{err: nil}
	m := f.monitor(Target{ExchangeCode: code, Public: fp.run})
	m.Pass(f.ctx) // success -> last_success_at set
	fp.err = errors.New("blip")
	m.Pass(f.ctx) // failure
	h := f.current(exID)
	if !h.hasSuccess {
		t.Error("last_success_at must survive a subsequent failure (not wiped)")
	}
	if !h.hasFailure || h.consec != 1 {
		t.Errorf("failure not recorded: hasFailure=%v consec=%d", h.hasFailure, h.consec)
	}
}

func TestContextCancelStopsLoop(t *testing.T) {
	f := setupH(t)
	code, _ := f.exchange()
	m := NewMonitor(f.rec, []Target{{ExchangeCode: code, Public: probe(nil)}}, clock.NewSystem(), nil, Config{Interval: 5 * time.Millisecond, Timeout: time.Second})
	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

type mutProbe struct{ err error }

func (m *mutProbe) run(context.Context) error { return m.err }
