package balance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/migrate"
)

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

// fakeBal is a READ-ONLY balance client (it implements only the BalanceClient
// surface — no PlaceOrder/CancelOrder exists on it).
type fakeBal struct {
	code     string
	balances []domain.Balance
	err      error
	block    bool
	calls    int32
}

func (f *fakeBal) Name() string { return f.code }
func (f *fakeBal) GetBalances(ctx context.Context) ([]domain.Balance, error) {
	atomic.AddInt32(&f.calls, 1)
	if f.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.balances, nil
}

func bal(asset, avail, locked, total string) domain.Balance {
	return domain.Balance{Asset: asset, Available: dec(avail), Locked: dec(locked), Total: dec(total)}
}

// ---- offline ----

func TestBalanceHash(t *testing.T) {
	h := balanceHash("BTC", dec("1.5"), dec("0.5"), dec("2"))
	// Canonical decimals: trailing zeros don't change the hash.
	if balanceHash("BTC", dec("1.50"), dec("0.50"), dec("2.0")) != h {
		t.Error("equal balances must hash equally")
	}
	if balanceHash("BTC", dec("1.6"), dec("0.5"), dec("2")) == h {
		t.Error("changed available must change the hash")
	}
	if balanceHash("BTC", dec("1.5"), dec("0.6"), dec("2")) == h {
		t.Error("changed locked must change the hash")
	}
	if balanceHash("BTC", dec("1.5"), dec("0.5"), dec("2.1")) == h {
		t.Error("changed total must change the hash")
	}
}

// TestBalanceClientIsReadOnly proves the syncer's client surface cannot place/cancel.
func TestBalanceClientIsReadOnly(t *testing.T) {
	typ := reflect.TypeOf((*BalanceClient)(nil)).Elem()
	if typ.NumMethod() != 2 {
		t.Fatalf("BalanceClient exposes %d methods, want exactly 2 (Name, GetBalances)", typ.NumMethod())
	}
	if _, ok := typ.MethodByName("GetBalances"); !ok {
		t.Error("BalanceClient missing GetBalances")
	}
	for _, bad := range []string{"PlaceOrder", "CancelOrder"} {
		if _, ok := typ.MethodByName(bad); ok {
			t.Errorf("BalanceClient must NOT expose %s", bad)
		}
	}
}

func TestStartupNoClientsIsSafe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	s := New(nil, nil, clock.NewSystem(), nil, Config{Interval: time.Hour})
	if err := s.Run(ctx); err != nil { // must not panic / require a store
		t.Errorf("Run with no clients = %v, want nil", err)
	}
}

// TestEffectiveIntervalFloor (PR13 #3 B) — a per-exchange interval below MinInterval is floored
// to MinInterval, so a misconfiguration can never over-poll a venue.
func TestEffectiveIntervalFloor(t *testing.T) {
	s := New(nil, map[string]BalanceClient{"ex": &fakeBal{code: "ex"}}, clock.NewSystem(), nil, Config{
		Interval: 60 * time.Second, MinInterval: 20 * time.Second,
		IntervalFor: func(string) time.Duration { return 5 * time.Second }, // below the floor
	})
	if got := s.effectiveInterval("ex"); got != 20*time.Second {
		t.Errorf("effectiveInterval = %v, want 20s (5s override floored by MinInterval 20s)", got)
	}
	// No override → the global default.
	s2 := New(nil, map[string]BalanceClient{"ex": &fakeBal{code: "ex"}}, clock.NewSystem(), nil, Config{Interval: 60 * time.Second})
	if got := s2.effectiveInterval("ex"); got != 60*time.Second {
		t.Errorf("effectiveInterval (no override) = %v, want the global default 60s", got)
	}
}

// TestPerExchangeCadence verifies rate-limit-aware, per-exchange balance polling: each
// exchange is polled on its own interval (floored at MinInterval so a misconfig can never
// over-poll), and an exchange not yet due is skipped. Pure scheduling — no DB needed.
func TestPerExchangeCadence(t *testing.T) {
	t0 := time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC)
	ivals := map[string]time.Duration{
		"fast": 2 * time.Second,
		"slow": 60 * time.Second,
		"tiny": 200 * time.Millisecond, // below MinInterval → floored
	}
	s := New(nil, map[string]BalanceClient{
		"fast": &fakeBal{code: "fast"}, "slow": &fakeBal{code: "slow"},
		"tiny": &fakeBal{code: "tiny"}, "def": &fakeBal{code: "def"},
	}, clock.NewManual(t0), nil, Config{
		Interval: 10 * time.Second, MinInterval: time.Second,
		IntervalFor: func(code string) time.Duration { return ivals[code] },
	})

	// Per-exchange interval, default fallback, and MinInterval floor.
	for code, want := range map[string]time.Duration{
		"fast": 2 * time.Second, "slow": 60 * time.Second,
		"tiny": time.Second /* floored */, "def": 10 * time.Second, /* default */
	} {
		if got := s.effectiveInterval(code); got != want {
			t.Errorf("effectiveInterval(%s)=%v, want %v", code, got, want)
		}
	}
	// baseTick = smallest effective interval (tiny floored to 1s).
	if got := s.baseTick(); got != time.Second {
		t.Errorf("baseTick=%v, want 1s", got)
	}

	has := func(now time.Time, want ...string) {
		t.Helper()
		got := map[string]bool{}
		for _, c := range s.dueCodes(now) {
			got[c] = true
		}
		set := map[string]bool{}
		for _, w := range want {
			set[w] = true
			if !got[w] {
				t.Errorf("at %v: %s should be due", now.Sub(t0), w)
			}
		}
		for c := range got {
			if !set[c] {
				t.Errorf("at %v: %s should NOT be due", now.Sub(t0), c)
			}
		}
	}

	// t0: everything is due (never polled).
	has(t0, "fast", "slow", "tiny", "def")
	// Simulate polling all at t0.
	for c := range s.clients {
		s.lastPolled[c] = t0
	}
	// +1s: only tiny (floored to 1s) is due.
	has(t0.Add(time.Second), "tiny")
	// +2s: tiny + fast.
	has(t0.Add(2*time.Second), "tiny", "fast")
	// +10s: tiny + fast + def (not slow).
	has(t0.Add(10*time.Second), "tiny", "fast", "def")
	// +60s: everything, including slow.
	has(t0.Add(60*time.Second), "fast", "slow", "tiny", "def")
}

// ---- gated ----

var bseq int

type bfix struct {
	t     *testing.T
	ctx   context.Context
	db    *sql.DB
	store *db.Store
}

func setupBal(t *testing.T) *bfix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the balance integration test")
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
	return &bfix{t: t, ctx: ctx, db: sqlDB, store: db.NewFromDB(sqlDB)}
}

func (f *bfix) exchange() (string, int64) {
	f.t.Helper()
	bseq++
	code := fmt.Sprintf("bal_%d_%d", time.Now().UnixNano(), bseq)
	res, err := f.db.Exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'Bal', 1)", code)
	if err != nil {
		f.t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return code, id
}

func (f *bfix) syncer(clients map[string]BalanceClient, cfg Config) *Syncer {
	s := New(f.store, clients, clock.NewSystem(), nil, cfg)
	if err := s.resolveExchangeIDs(f.ctx); err != nil {
		f.t.Fatal(err)
	}
	return s
}

func (f *bfix) current(exID int64, asset string) (avail, locked, total, hash string, ok bool) {
	var ls sql.NullString
	err := f.db.QueryRow("SELECT available, locked, total, COALESCE(balance_hash,''), COALESCE(last_seen_at,'') FROM wallet_balances_current WHERE exchange_id=? AND asset=?", exID, asset).
		Scan(&avail, &locked, &total, &hash, &ls)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", "", "", false
	}
	if err != nil {
		f.t.Fatal(err)
	}
	return avail, locked, total, hash, true
}

func (f *bfix) historyCount(exID int64, asset string) int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM wallet_balance_history WHERE exchange_id=? AND asset=?", exID, asset).Scan(&n)
	return n
}

func (f *bfix) lastSeen(exID int64, asset string) time.Time {
	var ls time.Time
	f.db.QueryRow("SELECT last_seen_at FROM wallet_balances_current WHERE exchange_id=? AND asset=?", exID, asset).Scan(&ls)
	return ls
}

func TestFirstObservationWritesCurrentAndHistory(t *testing.T) {
	f := setupBal(t)
	code, exID := f.exchange()
	fake := &fakeBal{code: code, balances: []domain.Balance{bal("BTC", "1.5", "0.5", "2")}}
	s := f.syncer(map[string]BalanceClient{code: fake}, Config{})
	s.SyncAll(f.ctx)

	avail, locked, total, hash, ok := f.current(exID, "BTC")
	if !ok || !dec(avail).Equal(dec("1.5")) || !dec(locked).Equal(dec("0.5")) || !dec(total).Equal(dec("2")) || hash == "" {
		t.Fatalf("current = %s/%s/%s hash=%q ok=%v", avail, locked, total, hash, ok)
	}
	if f.historyCount(exID, "BTC") != 1 {
		t.Errorf("history = %d, want 1", f.historyCount(exID, "BTC"))
	}
}

func TestUnchangedBalanceNoDuplicateHistory(t *testing.T) {
	f := setupBal(t)
	code, exID := f.exchange()
	fake := &fakeBal{code: code, balances: []domain.Balance{bal("BTC", "1.5", "0.5", "2")}}
	s := f.syncer(map[string]BalanceClient{code: fake}, Config{})
	s.SyncAll(f.ctx)
	t0 := f.lastSeen(exID, "BTC")
	time.Sleep(5 * time.Millisecond)
	s.SyncAll(f.ctx) // identical balance

	if f.historyCount(exID, "BTC") != 1 {
		t.Errorf("history after unchanged = %d, want 1 (no duplicate)", f.historyCount(exID, "BTC"))
	}
	if !f.lastSeen(exID, "BTC").After(t0) {
		t.Error("last_seen_at should advance even when the balance is unchanged")
	}
}

func TestChangedBalanceAddsHistory(t *testing.T) {
	f := setupBal(t)
	code, exID := f.exchange()
	fake := &fakeBal{code: code, balances: []domain.Balance{bal("BTC", "1.5", "0.5", "2")}}
	s := f.syncer(map[string]BalanceClient{code: fake}, Config{})
	s.SyncAll(f.ctx)
	// Change available.
	fake.balances = []domain.Balance{bal("BTC", "1.7", "0.5", "2.2")}
	s.SyncAll(f.ctx)
	if f.historyCount(exID, "BTC") != 2 {
		t.Errorf("history after available change = %d, want 2", f.historyCount(exID, "BTC"))
	}
	// Change locked only.
	fake.balances = []domain.Balance{bal("BTC", "1.7", "0.6", "2.3")}
	s.SyncAll(f.ctx)
	if f.historyCount(exID, "BTC") != 3 {
		t.Errorf("history after locked change = %d, want 3", f.historyCount(exID, "BTC"))
	}
	if a, _, _, _, _ := f.current(exID, "BTC"); !dec(a).Equal(dec("1.7")) {
		t.Errorf("current available = %s, want 1.7", a)
	}
}

func TestMissingAssetNotZeroed(t *testing.T) {
	f := setupBal(t)
	code, exID := f.exchange()
	fake := &fakeBal{code: code, balances: []domain.Balance{bal("BTC", "1.5", "0", "1.5"), bal("ETH", "10", "0", "10")}}
	s := f.syncer(map[string]BalanceClient{code: fake}, Config{})
	s.SyncAll(f.ctx)
	// ETH disappears from the response -> it must NOT be zeroed or deleted.
	fake.balances = []domain.Balance{bal("BTC", "1.5", "0", "1.5")}
	s.SyncAll(f.ctx)

	avail, _, _, _, ok := f.current(exID, "ETH")
	if !ok {
		t.Fatal("ETH current row must survive a missing-from-response observation")
	}
	if !dec(avail).Equal(dec("10")) {
		t.Errorf("ETH available = %s, want 10 (never zeroed on absence)", avail)
	}
	if f.historyCount(exID, "ETH") != 1 {
		t.Errorf("ETH history = %d, want 1 (no spurious zero row)", f.historyCount(exID, "ETH"))
	}
}

func TestFailureIsolationKeepsOthersAndPrevious(t *testing.T) {
	f := setupBal(t)
	codeA, exA := f.exchange()
	codeB, exB := f.exchange()
	// A previously recorded a balance; now A errors and B succeeds.
	fakeA := &fakeBal{code: codeA, balances: []domain.Balance{bal("BTC", "3", "0", "3")}}
	fakeB := &fakeBal{code: codeB, balances: []domain.Balance{bal("ETH", "9", "0", "9")}}
	s := f.syncer(map[string]BalanceClient{codeA: fakeA, codeB: fakeB}, Config{})
	s.SyncAll(f.ctx) // both succeed first

	fakeA.err = errors.New("api down")
	fakeA.balances = nil
	s.SyncAll(f.ctx)

	// A's previous balance survives (not wiped/zeroed); B continues to sync.
	if a, _, _, _, ok := f.current(exA, "BTC"); !ok || !dec(a).Equal(dec("3")) {
		t.Errorf("A BTC = %s ok=%v, want 3 (survives failure)", a, ok)
	}
	if e, _, _, _, ok := f.current(exB, "ETH"); !ok || !dec(e).Equal(dec("9")) {
		t.Errorf("B ETH = %s ok=%v, want 9 (other exchange unaffected)", e, ok)
	}
}

func TestPrecisionPreserved(t *testing.T) {
	f := setupBal(t)
	code, exID := f.exchange()
	fake := &fakeBal{code: code, balances: []domain.Balance{bal("BTC", "0.123456789012345678", "0", "0.123456789012345678")}}
	s := f.syncer(map[string]BalanceClient{code: fake}, Config{})
	s.SyncAll(f.ctx)
	avail, _, _, _, _ := f.current(exID, "BTC")
	if !dec(avail).Equal(dec("0.123456789012345678")) {
		t.Errorf("available = %s, want full 18-dp precision preserved", avail)
	}
}

func TestTimeoutKeepsPreviousBalance(t *testing.T) {
	f := setupBal(t)
	code, exID := f.exchange()
	fake := &fakeBal{code: code, balances: []domain.Balance{bal("BTC", "5", "0", "5")}}
	s := f.syncer(map[string]BalanceClient{code: fake}, Config{Timeout: 30 * time.Millisecond})
	s.SyncAll(f.ctx) // record 5
	// Now the client blocks past the timeout -> the call errors; previous survives.
	fake.block = true
	s.SyncAll(f.ctx)
	if a, _, _, _, ok := f.current(exID, "BTC"); !ok || !dec(a).Equal(dec("5")) {
		t.Errorf("BTC = %s ok=%v, want 5 (timeout must not wipe)", a, ok)
	}
}

func TestContextCancelStopsLoop(t *testing.T) {
	f := setupBal(t)
	code, _ := f.exchange()
	fake := &fakeBal{code: code, balances: []domain.Balance{bal("BTC", "1", "0", "1")}}
	s := f.syncer(map[string]BalanceClient{code: fake}, Config{Interval: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
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
	if atomic.LoadInt32(&fake.calls) == 0 {
		t.Error("expected at least one GetBalances call before cancel")
	}
}
