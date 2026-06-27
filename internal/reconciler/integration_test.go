package reconciler

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	"v3TradeBot/internal/db"
	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/migrate"
	"v3TradeBot/internal/state"
)

// fakeRO is a configurable READ-ONLY client. It also implements PlaceOrder /
// CancelOrder that FAIL the test if ever called — proving the reconciler never
// auto-sends (rule: no PlaceOrder/CancelOrder). It makes no network calls.
type fakeRO struct {
	t       *testing.T
	caps    exchanges.Capabilities
	results map[string]struct {
		st  execution.OrderStatus
		err error
	}
}

func (f *fakeRO) Name() string                         { return "fake" }
func (f *fakeRO) Capabilities() exchanges.Capabilities { return f.caps }
func (f *fakeRO) GetOrder(_ context.Context, id string) (execution.OrderStatus, error) {
	if r, ok := f.results[id]; ok {
		return r.st, r.err
	}
	return execution.OrderStatus{}, execution.ErrOrderUnknown
}
func (f *fakeRO) GetOpenOrders(context.Context, string) ([]execution.OrderStatus, error) {
	return nil, nil
}
func (f *fakeRO) GetBalances(context.Context) ([]domain.Balance, error) { return nil, nil }
func (f *fakeRO) PlaceOrder(context.Context, execution.OrderRequest) (execution.OrderAck, error) {
	f.t.Fatal("reconciler must NEVER call PlaceOrder")
	return execution.OrderAck{}, nil
}
func (f *fakeRO) CancelOrder(context.Context, string) error {
	f.t.Fatal("reconciler must NEVER call CancelOrder")
	return nil
}

var rseq int

type rfix struct {
	db   *sql.DB
	ctx  context.Context
	code string
	exID int64
	fake *fakeRO
	rec  *Reconciler
}

func setupR(t *testing.T) *rfix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the reconciler integration test")
	}
	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	ctx := context.Background()
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := migrate.Run(ctx, sqlDB, migrate.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	rseq++
	code := fmt.Sprintf("rec%d_%d", time.Now().UnixNano()%1_000_000, rseq)
	res, _ := sqlDB.Exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'r', 1)", code)
	exID, _ := res.LastInsertId()
	fake := &fakeRO{t: t, caps: exchanges.Capabilities{FetchByOrderID: true}, results: map[string]struct {
		st  execution.OrderStatus
		err error
	}{}}
	rec := New(db.NewFromDB(sqlDB), map[string]ReadOnlyClient{code: fake}, nil)
	return &rfix{db: sqlDB, ctx: ctx, code: code, exID: exID, fake: fake, rec: rec}
}

// seedCycleOrder creates a cycle + order in the given states with the given
// exchange_order_id ("" => NULL) and returns (cycleID, orderID).
func (f *rfix) seedCycleOrder(t *testing.T, cycleSt state.CycleState, orderSt state.OrderState, exchangeOrderID string) (int64, int64) {
	t.Helper()
	rseq++
	// Globally-unique across packages (go test ./... shares one DB): include nanos.
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), rseq) }
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	ex := func(q string, a ...any) sql.Result {
		r, err := f.db.Exec(q, a...)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	b := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B")))
	qa := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("Q")))
	m := last(ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", u("M"), b, qa))
	em := last(ex("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, 'X/Y')", f.exID, m, u("ES")))
	cyc := last(ex("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state) VALUES (?, ?, 'X/Y', ?)", em, f.exID, string(cycleSt)))
	var exoid any
	if exchangeOrderID != "" {
		exoid = exchangeOrderID
	}
	ord := last(ex(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, exchange_order_id, state, quantity)
		VALUES (?, ?, ?, 'buy', 'entry_buy', ?, ?, ?, '1')`, cyc, f.exID, em, u("loc"), exoid, string(orderSt)))
	return cyc, ord
}

func (f *rfix) seedLock(t *testing.T, cycleID int64) int64 {
	t.Helper()
	rseq++
	res, err := f.db.Exec("INSERT INTO symbol_locks (scope, canonical_symbol, cycle_id, expires_at) VALUES (?, ?, ?, NOW(6)+INTERVAL 1 HOUR)",
		f.code, fmt.Sprintf("S%d", rseq), cycleID)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id
}

func (f *rfix) result(id string, status execution.NormalizedOrderState, filled, exchangeOrderID string, err error) {
	f.fake.results[id] = struct {
		st  execution.OrderStatus
		err error
	}{st: execution.OrderStatus{Status: status, FilledQty: decimal.RequireFromString(filled), ExchangeOrderID: exchangeOrderID}, err: err}
}

func cycState(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()
	var s string
	if err := db.QueryRow("SELECT state FROM cycles WHERE id=?", id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}
func ordSt(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()
	var s string
	if err := db.QueryRow("SELECT state FROM orders WHERE id=?", id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}
func lockState(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()
	var s string
	if err := db.QueryRow("SELECT state FROM symbol_locks WHERE id=?", id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestStartupFilledOrderMarksCycleNeedsReconcile(t *testing.T) {
	f := setupR(t)
	cyc, ord := f.seedCycleOrder(t, state.CycleBuySubmitted, state.OrderSubmitted, "EX1")
	f.result("EX1", execution.StateFilled, "1", "EX1", nil)

	rep, err := f.rec.ReconcileStartup(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.CyclesChecked < 1 {
		t.Fatalf("CyclesChecked = %d", rep.CyclesChecked)
	}
	if ordSt(t, f.db, ord) != string(state.OrderFilled) {
		t.Errorf("order state = %s, want FILLED", ordSt(t, f.db, ord))
	}
	// Filled => exposure => cycle NEEDS_RECONCILE (accounting deferred to PR10).
	if cycState(t, f.db, cyc) != string(state.CycleNeedsReconcile) {
		t.Errorf("cycle state = %s, want NEEDS_RECONCILE", cycState(t, f.db, cyc))
	}
	// A state-machine event was written (not ad-hoc SQL).
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM order_events WHERE order_id=?", ord).Scan(&n)
	if n < 1 {
		t.Error("expected an order_event from the reconcile transition")
	}
}

func TestSafeCloseZeroExposureReleasesLock(t *testing.T) {
	f := setupR(t)
	// Order in CANCEL_PENDING (we requested the cancel, simulated-IOC flow); the
	// exchange confirms CANCELED with zero fill -> clean terminal -> safe close.
	cyc, ord := f.seedCycleOrder(t, state.CycleCancelPending, state.OrderCancelPending, "EX2")
	lock := f.seedLock(t, cyc)
	f.result("EX2", execution.StateCanceled, "0", "EX2", nil) // cancelled, zero fill

	if _, err := f.rec.ReconcileStartup(f.ctx); err != nil {
		t.Fatal(err)
	}
	if ordSt(t, f.db, ord) != string(state.OrderCancelled) {
		t.Errorf("order = %s, want CANCELLED", ordSt(t, f.db, ord))
	}
	if cycState(t, f.db, cyc) != string(state.CycleFailed) {
		t.Errorf("cycle = %s, want FAILED (safe close)", cycState(t, f.db, cyc))
	}
	if lockState(t, f.db, lock) != "RELEASED" {
		t.Errorf("lock = %s, want RELEASED", lockState(t, f.db, lock))
	}
}

func TestAmbiguousKeepsLockAndNeedsReconcile(t *testing.T) {
	f := setupR(t)
	cyc, ord := f.seedCycleOrder(t, state.CycleBuySubmitted, state.OrderSubmitted, "EX3")
	lock := f.seedLock(t, cyc)
	f.result("EX3", execution.StateUnknown, "0", "EX3", execution.ErrOrderUnknown) // not found = ambiguous

	if _, err := f.rec.ReconcileStartup(f.ctx); err != nil {
		t.Fatal(err)
	}
	if ordSt(t, f.db, ord) != string(state.OrderNeedsReconcile) {
		t.Errorf("order = %s, want NEEDS_RECONCILE", ordSt(t, f.db, ord))
	}
	if cycState(t, f.db, cyc) != string(state.CycleNeedsReconcile) {
		t.Errorf("cycle = %s, want NEEDS_RECONCILE", cycState(t, f.db, cyc))
	}
	// rule #8: lock must NOT be released for an ambiguous cycle.
	if lockState(t, f.db, lock) != "ACTIVE" {
		t.Errorf("lock = %s, want still ACTIVE (ambiguous cycle)", lockState(t, f.db, lock))
	}
}

func TestUnknownExchangeIdFoundByClientIdAttaches(t *testing.T) {
	f := setupR(t)
	f.fake.caps = exchanges.Capabilities{ClientOrderID: true, FetchByOrderID: true}
	// order with no exchange_order_id; identified via its local client order id.
	cyc, ord := f.seedCycleOrder(t, state.CycleBuySubmitted, state.OrderSubmitted, "")
	var loc string
	f.db.QueryRow("SELECT local_client_order_id FROM orders WHERE id=?", ord).Scan(&loc)
	f.result(loc, execution.StateOpen, "0", "EXR-attached", nil)

	if _, err := f.rec.ReconcileStartup(f.ctx); err != nil {
		t.Fatal(err)
	}
	var exoid string
	f.db.QueryRow("SELECT COALESCE(exchange_order_id,'') FROM orders WHERE id=?", ord).Scan(&exoid)
	if exoid != "EXR-attached" {
		t.Errorf("exchange_order_id = %q, want EXR-attached (positively identified)", exoid)
	}
	_ = cyc
}

func TestUnknownExchangeIdNotFoundNeedsReconcile(t *testing.T) {
	f := setupR(t)
	f.fake.caps = exchanges.Capabilities{ClientOrderID: true, FetchByOrderID: true}
	_, ord := f.seedCycleOrder(t, state.CycleBuySubmitted, state.OrderSubmitted, "")
	// no result configured -> fake returns ErrOrderUnknown for the client id.
	if _, err := f.rec.ReconcileStartup(f.ctx); err != nil {
		t.Fatal(err)
	}
	if ordSt(t, f.db, ord) != string(state.OrderNeedsReconcile) {
		t.Errorf("order = %s, want NEEDS_RECONCILE (not positively identified)", ordSt(t, f.db, ord))
	}
}

func TestUnsupportedCapabilityNeedsReconcile(t *testing.T) {
	f := setupR(t)
	f.fake.caps = exchanges.Capabilities{FetchByOrderID: false} // cannot GetOrder
	_, ord := f.seedCycleOrder(t, state.CycleBuySubmitted, state.OrderSubmitted, "EX6")
	if _, err := f.rec.ReconcileStartup(f.ctx); err != nil {
		t.Fatal(err)
	}
	if ordSt(t, f.db, ord) != string(state.OrderNeedsReconcile) {
		t.Errorf("order = %s, want NEEDS_RECONCILE (cannot verify)", ordSt(t, f.db, ord))
	}
}

func TestIdempotentRepeatedRun(t *testing.T) {
	f := setupR(t)
	cyc, _ := f.seedCycleOrder(t, state.CycleBuySubmitted, state.OrderSubmitted, "EX7")
	f.result("EX7", execution.StateUnknown, "0", "EX7", execution.ErrOrderUnknown)

	if _, err := f.rec.ReconcileStartup(f.ctx); err != nil {
		t.Fatal(err)
	}
	var events1 int
	f.db.QueryRow("SELECT COUNT(*) FROM cycle_state_events WHERE cycle_id=?", cyc).Scan(&events1)

	// Second run must not duplicate events or oscillate state.
	if _, err := f.rec.ReconcileStartup(f.ctx); err != nil {
		t.Fatal(err)
	}
	var events2 int
	f.db.QueryRow("SELECT COUNT(*) FROM cycle_state_events WHERE cycle_id=?", cyc).Scan(&events2)
	if events2 != events1 {
		t.Errorf("repeated run added events: %d -> %d (must be idempotent)", events1, events2)
	}
	if cycState(t, f.db, cyc) != string(state.CycleNeedsReconcile) {
		t.Errorf("cycle oscillated: %s", cycState(t, f.db, cyc))
	}
}

func TestStuckInFlightReported(t *testing.T) {
	f := setupR(t)
	f.db.Exec(`INSERT INTO exchange_requests (exchange_id, request_type, priority, status, payload, idempotency_key, inflight_at)
		VALUES (?, 'PLACE_ORDER', 100, 'IN_FLIGHT', '{}', ?, NOW(6))`, f.exID, fmt.Sprintf("if%d", rseq))
	f.db.Exec(`INSERT INTO exchange_requests (exchange_id, request_type, priority, status, payload, idempotency_key)
		VALUES (?, 'PLACE_ORDER', 100, 'DEAD', '{}', ?)`, f.exID, fmt.Sprintf("dead%d", rseq))
	rep, err := f.rec.ReconcileStartup(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.StuckInFlight < 1 || rep.DeadRequests < 1 {
		t.Errorf("report = %+v, want stuck/dead reported", rep)
	}
}

func TestApplyOrderOutcomeRollsBackOnStaleVersion(t *testing.T) {
	f := setupR(t)
	_, ord := f.seedCycleOrder(t, state.CycleBuySubmitted, state.OrderSubmitted, "")

	// Stale version (99) -> CAS fails -> whole tx rolls back, attach reverted.
	err := f.rec.applyOrderOutcome(f.ctx, orderRow{ID: ord, State: state.OrderSubmitted, Version: 99},
		OrderOutcome{Decision: AdvanceTerminal, TargetState: state.OrderFilled, AttachExchangeOrderID: "SHOULD-NOT-PERSIST"})
	if err == nil {
		t.Fatal("expected stale-version error")
	}
	var exoid string
	f.db.QueryRow("SELECT COALESCE(exchange_order_id,'') FROM orders WHERE id=?", ord).Scan(&exoid)
	if exoid == "SHOULD-NOT-PERSIST" {
		t.Error("exchange_order_id attach must have rolled back with the failed transition")
	}
	if ordSt(t, f.db, ord) != string(state.OrderSubmitted) {
		t.Errorf("order should be unchanged (SUBMITTED) after rollback, got %s", ordSt(t, f.db, ord))
	}
}
