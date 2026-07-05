package sellflow

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/migrate"
	"v3TradeBot/internal/queue"
)

// sellflow gated tests run CreateSell/RepriceSell against a real MariaDB. No exchange
// is ever contacted. Skipped unless V3_TEST_MYSQL_DSN is set.

var sseq int

type sfix struct {
	t      *testing.T
	ctx    context.Context
	db     *sql.DB
	store  *db.Store
	q      *queue.Queue
	exID   int64
	exCode string
}

func setupS(t *testing.T) *sfix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the sellflow integration test")
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
	sseq++
	code := fmt.Sprintf("sf_%d_%d", time.Now().UnixNano(), sseq)
	res, err := sqlDB.Exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'SF', 1)", code)
	if err != nil {
		t.Fatal(err)
	}
	exID, _ := res.LastInsertId()
	return &sfix{t: t, ctx: ctx, db: sqlDB, store: db.NewFromDB(sqlDB), q: queue.New(sqlDB, clock.NewSystem()), exID: exID, exCode: code}
}

// market builds a MarketConfig + the backing exchange_market row.
func (f *sfix) market(offsetBps int, tick, step, minQty, minAmt string) configstore.MarketConfig {
	f.t.Helper()
	sseq++
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	ex := func(q string, a ...any) sql.Result {
		r, err := f.db.Exec(q, a...)
		if err != nil {
			f.t.Fatalf("seed %q: %v", q, err)
		}
		return r
	}
	canonical := fmt.Sprintf("SF%d_%d/IRT", f.exID, sseq)
	b := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", fmt.Sprintf("SB%d_%d", f.exID, sseq)))
	qa := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", fmt.Sprintf("SQ%d_%d", f.exID, sseq)))
	m := last(ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", canonical, b, qa))
	em := last(ex("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol, enabled_for_sell_manage, tick_size, step_size, min_order_quantity, min_order_amount) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?)",
		f.exID, m, fmt.Sprintf("SFES%d_%d", f.exID, sseq), canonical, tick, step, minQty, minAmt))
	return configstore.MarketConfig{
		ExchangeMarketID: em, ExchangeID: f.exID, ExchangeCode: f.exCode, CanonicalSymbol: canonical,
		EnabledForSellManage: true, SellOffsetBps: offsetBps, RepriceIntervalSeconds: 5, OrderTimeoutMs: 3000, MaxRetries: 3,
		TickSize: dd(tick), StepSize: dd(step), MinOrderQuantity: dd(minQty), MinOrderAmount: dd(minAmt),
	}
}

// seedCycleBuy inserts a cycle (cycleState) + a filled entry_buy order; returns cycleID.
func (f *sfix) seedCycleBuy(m configstore.MarketConfig, cycleState, buyFilled, buyQuote string) int64 {
	f.t.Helper()
	sseq++
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	cyc := last(f.mustExec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state) VALUES (?, ?, ?, ?)",
		m.ExchangeMarketID, f.exID, m.CanonicalSymbol, cycleState))
	f.mustExec(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, order_type, quantity, filled_quantity, avg_fill_price, quote_spent, fee_amount, fee_asset)
		VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'FILLED', 'limit', ?, ?, '100', ?, '0', 'IRT')`,
		cyc, f.exID, m.ExchangeMarketID, fmt.Sprintf("c%d-buy", cyc), buyFilled, buyFilled, buyQuote)
	return cyc
}

func (f *sfix) seedLock(cycleID int64) int64 {
	f.t.Helper()
	sseq++
	id, _ := f.mustExec("INSERT INTO symbol_locks (scope, canonical_symbol, cycle_id, expires_at) VALUES (?, ?, ?, NOW(6)+INTERVAL 1 HOUR)",
		f.exCode, fmt.Sprintf("SL%d_%d", f.exID, sseq), cycleID).LastInsertId()
	return id
}

func (f *sfix) mustExec(q string, a ...any) sql.Result {
	f.t.Helper()
	r, err := f.db.Exec(q, a...)
	if err != nil {
		f.t.Fatalf("exec %q: %v", q, err)
	}
	return r
}

func dd(s string) decimal.Decimal {
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Zero
	}
	return d
}

func (f *sfix) cycleState(id int64) string {
	var s string
	f.db.QueryRow("SELECT state FROM cycles WHERE id=?", id).Scan(&s)
	return s
}
func (f *sfix) sellOrder(cycleID int64) (id int64, state, price, qty, side string) {
	f.db.QueryRow("SELECT id, state, COALESCE(limit_price,''), quantity, side FROM orders WHERE cycle_id=? AND role='exit_sell' ORDER BY id DESC LIMIT 1", cycleID).Scan(&id, &state, &price, &qty, &side)
	return
}

func TestCreateSellFromFullBuy(t *testing.T) {
	f := setupS(t)
	m := f.market(30, "0.01", "0.0001", "0.001", "1") // offset 30bps, tick 0.01
	cyc := f.seedCycleBuy(m, "BUY_FILLED", "0.5", "50")
	r, err := CreateSell(f.ctx, f.store, f.q, m, CreateParams{CycleID: cyc, BinanceRef: dd("100"), QuoteUnit: "IRT", ConfigVersion: 7})
	if err != nil {
		t.Fatalf("CreateSell: %v", err)
	}
	if !r.Price.Equal(dd("99.7")) || !r.Quantity.Equal(dd("0.5")) {
		t.Errorf("price/qty = %s/%s, want 99.7/0.5", r.Price, r.Quantity)
	}
	if f.cycleState(cyc) != "SELL_REQUEST_QUEUED" {
		t.Errorf("cycle = %s, want SELL_REQUEST_QUEUED", f.cycleState(cyc))
	}
	oid, ost, oprice, oqty, side := f.sellOrder(cyc)
	if oid == 0 || ost != "QUEUED" || side != "sell" || !dd(oprice).Equal(dd("99.7")) || !dd(oqty).Equal(dd("0.5")) {
		t.Errorf("sell order = id%d/%s/%s price %s qty %s", oid, ost, side, oprice, oqty)
	}
	// PLACE request is QUEUED with a sell payload.
	var status, payload string
	f.db.QueryRow("SELECT status, payload FROM exchange_requests WHERE id=?", r.RequestID).Scan(&status, &payload)
	if status != "QUEUED" || !contains(payload, `"side":"sell"`) {
		t.Errorf("sell request = %s payload %s", status, payload)
	}
}

func TestCreateSellFromPartialBuyUsesFilledQty(t *testing.T) {
	f := setupS(t)
	m := f.market(0, "0", "0", "0", "0")
	cyc := f.seedCycleBuy(m, "BUY_PARTIALLY_FILLED", "0.3", "30") // requested more, only 0.3 filled
	r, err := CreateSell(f.ctx, f.store, f.q, m, CreateParams{CycleID: cyc, BinanceRef: dd("100")})
	if err != nil {
		t.Fatal(err)
	}
	if !r.Quantity.Equal(dd("0.3")) {
		t.Errorf("sell qty = %s, want 0.3 (filled only, not requested)", r.Quantity)
	}
}

func TestCreateSellNoDuplicate(t *testing.T) {
	f := setupS(t)
	m := f.market(0, "0", "0", "0", "0")
	cyc := f.seedCycleBuy(m, "BUY_FILLED", "0.5", "50")
	if _, err := CreateSell(f.ctx, f.store, f.q, m, CreateParams{CycleID: cyc, BinanceRef: dd("100")}); err != nil {
		t.Fatal(err)
	}
	// The cycle is now SELL_REQUEST_QUEUED; a second create is a benign no-op (the
	// cycle-state guard and/or the active-order guard prevent a duplicate).
	_, err := CreateSell(f.ctx, f.store, f.q, m, CreateParams{CycleID: cyc, BinanceRef: dd("100")})
	if !isBenign(err) {
		t.Fatalf("second create = %v, want a benign skip", err)
	}
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM orders WHERE cycle_id=? AND role='exit_sell'", cyc).Scan(&n)
	if n != 1 {
		t.Errorf("sell orders = %d, want 1 (no duplicate)", n)
	}
}

func TestCreateSellExistsGuardOnReplace(t *testing.T) {
	f := setupS(t)
	m := f.market(0, "0", "0", "0", "0")
	// A reprice in progress: cycle SELL_REPRICE_PENDING with a still-active sell order.
	// A replacement create must be refused (ErrSellExists) until the old one is gone.
	cyc := f.seedCycleBuy(m, "SELL_REPRICE_PENDING", "0.5", "50")
	f.seedRestingSell(m, cyc, "CANCEL_PENDING", "EX-S")
	if _, err := CreateSell(f.ctx, f.store, f.q, m, CreateParams{CycleID: cyc, BinanceRef: dd("100")}); err != ErrSellExists {
		t.Fatalf("create with active sell = %v, want ErrSellExists", err)
	}
}

func TestCreateSellBelowMinimum(t *testing.T) {
	f := setupS(t)
	m := f.market(0, "0", "0", "1", "0") // min qty 1
	cyc := f.seedCycleBuy(m, "BUY_FILLED", "0.5", "50")
	_, err := CreateSell(f.ctx, f.store, f.q, m, CreateParams{CycleID: cyc, BinanceRef: dd("100")})
	if err != ErrBelowMinimum {
		t.Fatalf("create = %v, want ErrBelowMinimum", err)
	}
	if f.cycleState(cyc) != "BUY_FILLED" {
		t.Errorf("cycle changed despite below-min: %s", f.cycleState(cyc))
	}
}

func TestCreateSellRollbackOnDuplicateRequest(t *testing.T) {
	f := setupS(t)
	m := f.market(0, "0", "0", "0", "0")
	cyc := f.seedCycleBuy(m, "BUY_FILLED", "0.5", "50")
	// Pre-insert a request with the idempotency key CreateSell will use -> Enqueue
	// fails -> the whole tx rolls back (no sell order, cycle unchanged).
	f.mustExec("INSERT INTO exchange_requests (exchange_id, request_type, status, payload, idempotency_key) VALUES (?, 'PLACE_ORDER', 'QUEUED', '{}', ?)",
		f.exID, fmt.Sprintf("place-sell:c%d:s1", cyc))
	if _, err := CreateSell(f.ctx, f.store, f.q, m, CreateParams{CycleID: cyc, BinanceRef: dd("100")}); err == nil {
		t.Fatal("expected a duplicate-idempotency error")
	}
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM orders WHERE cycle_id=? AND role='exit_sell'", cyc).Scan(&n)
	if n != 0 {
		t.Errorf("sell orders after rollback = %d, want 0", n)
	}
	if f.cycleState(cyc) != "BUY_FILLED" {
		t.Errorf("cycle changed despite rollback: %s", f.cycleState(cyc))
	}
}

// seedRestingSell adds an exit_sell order in the given state with an exchange id.
func (f *sfix) seedRestingSell(m configstore.MarketConfig, cycleID int64, st, exoid string) int64 {
	f.t.Helper()
	sseq++
	id, _ := f.mustExec(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, exchange_order_id, state, order_type, limit_price, quantity)
		VALUES (?, ?, ?, 'sell', 'exit_sell', ?, ?, ?, 'limit', '99', '0.5')`,
		cycleID, f.exID, m.ExchangeMarketID, fmt.Sprintf("c%d-sell-1", cycleID), exoid, st).LastInsertId()
	return id
}

func TestRepriceIntervalGate(t *testing.T) {
	f := setupS(t)
	m := f.market(30, "0.01", "0", "0", "0")
	cyc := f.seedCycleBuy(m, "SELL_SUBMITTED", "0.5", "50")
	f.seedRestingSell(m, cyc, "ACKED", "EX-S")
	// last_reprice_at just now -> too soon.
	f.mustExec("UPDATE cycles SET last_reprice_at=NOW(6) WHERE id=?", cyc)
	if err := RepriceSell(f.ctx, f.store, f.q, m, cyc, 500); err != ErrRepriceTooSoon {
		t.Fatalf("reprice too-soon = %v, want ErrRepriceTooSoon", err)
	}
	// Age it beyond the interval -> reprice proceeds.
	f.mustExec("UPDATE cycles SET last_reprice_at = NOW(6) - INTERVAL 1 HOUR WHERE id=?", cyc)
	if err := RepriceSell(f.ctx, f.store, f.q, m, cyc, 500); err != nil {
		t.Fatalf("reprice = %v, want success", err)
	}
	if f.cycleState(cyc) != "SELL_REPRICE_PENDING" {
		t.Errorf("cycle = %s, want SELL_REPRICE_PENDING", f.cycleState(cyc))
	}
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE cycle_id=? AND request_type='CANCEL_ORDER'", cyc).Scan(&n)
	if n != 1 {
		t.Errorf("cancel requests = %d, want 1", n)
	}
}

// TestRepriceEmptyExchangeOrderIDReconciles (PR11 #7) — a resting sell with no usable
// exchange_order_id must not enqueue a blind CANCEL_ORDER(""): order+cycle → NEEDS_RECONCILE,
// lock preserved, no cancel queued.
func TestRepriceEmptyExchangeOrderIDReconciles(t *testing.T) {
	f := setupS(t)
	m := f.market(30, "0.01", "0", "0", "0")
	cyc := f.seedCycleBuy(m, "SELL_SUBMITTED", "0.5", "50")
	lock := f.seedLock(cyc)
	f.seedRestingSell(m, cyc, "ACKED", "") // empty exchange_order_id
	f.mustExec("UPDATE cycles SET last_reprice_at = NOW(6) - INTERVAL 1 HOUR WHERE id=?", cyc)

	if err := RepriceSell(f.ctx, f.store, f.q, m, cyc, 500); err != nil {
		t.Fatalf("RepriceSell = %v, want nil (resolved via reconcile, not an error)", err)
	}
	var cancels int
	f.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE cycle_id=? AND request_type='CANCEL_ORDER'", cyc).Scan(&cancels)
	if cancels != 0 {
		t.Errorf("cancel requests=%d, want 0 (no blind cancel with empty exchange_order_id)", cancels)
	}
	if _, st, _, _, _ := f.sellOrder(cyc); st != "NEEDS_RECONCILE" {
		t.Errorf("sell order=%s, want NEEDS_RECONCILE", st)
	}
	if f.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Errorf("cycle=%s, want NEEDS_RECONCILE", f.cycleState(cyc))
	}
	var lockSt string
	f.db.QueryRow("SELECT state FROM symbol_locks WHERE id=?", lock).Scan(&lockSt)
	if lockSt != "ACTIVE" {
		t.Errorf("lock=%s, want ACTIVE (preserved)", lockSt)
	}
}

func TestRepriceBlockedByInFlightOp(t *testing.T) {
	f := setupS(t)
	m := f.market(30, "0.01", "0", "0", "0")
	cyc := f.seedCycleBuy(m, "SELL_SUBMITTED", "0.5", "50")
	f.seedRestingSell(m, cyc, "ACKED", "EX-S")
	// A sell place is already IN_FLIGHT -> reprice must not proceed.
	f.mustExec("INSERT INTO exchange_requests (exchange_id, cycle_id, request_type, status, payload, idempotency_key) VALUES (?, ?, 'PLACE_ORDER', 'IN_FLIGHT', '{}', ?)",
		f.exID, cyc, fmt.Sprintf("inflight_%d", cyc))
	if err := RepriceSell(f.ctx, f.store, f.q, m, cyc, 500); err != ErrSellOpInFlight {
		t.Fatalf("reprice = %v, want ErrSellOpInFlight", err)
	}
}

func TestRepriceNoRestingSell(t *testing.T) {
	f := setupS(t)
	m := f.market(30, "0.01", "0", "0", "0")
	cyc := f.seedCycleBuy(m, "SELL_SUBMITTED", "0.5", "50") // no resting sell order seeded
	if err := RepriceSell(f.ctx, f.store, f.q, m, cyc, 500); err != ErrNoRestingSell {
		t.Fatalf("reprice = %v, want ErrNoRestingSell", err)
	}
}

func TestManagerCreatesSell(t *testing.T) {
	f := setupS(t)
	m := f.market(30, "0.01", "0.0001", "0.001", "1")
	cyc := f.seedCycleBuy(m, "BUY_FILLED", "0.5", "50")
	cache := configstore.NewCache()
	if err := cache.Reload(f.ctx, configstore.New(f.db)); err != nil {
		t.Fatal(err)
	}
	// RefPrice answers only for THIS market, so the manager (which scans all open
	// cycles in the shared test DB) acts solely on our cycle.
	ref := func(mc configstore.MarketConfig) (decimal.Decimal, string, string, bool) {
		if mc.ExchangeMarketID == m.ExchangeMarketID {
			return dd("100"), "IRT", "", true
		}
		return decimal.Zero, "", "", false
	}
	mgr := NewManager(f.store, f.q, cache, ref, clock.NewSystem(), nil)
	if err := mgr.Pass(f.ctx); err != nil {
		t.Fatal(err)
	}
	oid, ost, _, _, side := f.sellOrder(cyc)
	if oid == 0 || side != "sell" || ost != "QUEUED" {
		t.Errorf("manager did not create the sell: id%d/%s/%s", oid, ost, side)
	}
	if f.cycleState(cyc) != "SELL_REQUEST_QUEUED" {
		t.Errorf("cycle = %s, want SELL_REQUEST_QUEUED", f.cycleState(cyc))
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
