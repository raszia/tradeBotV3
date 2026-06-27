package orders

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/migrate"
	"v3TradeBot/internal/queue"
	"v3TradeBot/internal/state"
)

// orders processing tests run against a real MariaDB (no exchange ever contacted).
// Skipped unless V3_TEST_MYSQL_DSN is set.

var oseq int

type ofix struct {
	t      *testing.T
	ctx    context.Context
	db     *sql.DB
	store  *db.Store
	q      *queue.Queue
	exID   int64
	exCode string
}

func setupO(t *testing.T) *ofix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the orders integration test")
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
	oseq++
	code := fmt.Sprintf("ord_%d_%d", time.Now().UnixNano(), oseq)
	res, err := sqlDB.Exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'Ord', 1)", code)
	if err != nil {
		t.Fatal(err)
	}
	exID, _ := res.LastInsertId()
	return &ofix{t: t, ctx: ctx, db: sqlDB, store: db.NewFromDB(sqlDB), q: queue.New(sqlDB, clock.NewSystem()), exID: exID, exCode: code}
}

// seed creates a cycle + buy order (given states, exchange_order_id) and returns ids.
func (f *ofix) seed(cycleSt state.CycleState, orderSt state.OrderState, exchangeOrderID, qty string) (int64, int64) {
	f.t.Helper()
	oseq++
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, f.exID, oseq) }
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	ex := func(q string, a ...any) sql.Result {
		r, err := f.db.Exec(q, a...)
		if err != nil {
			f.t.Fatalf("seed %q: %v", q, err)
		}
		return r
	}
	b := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B")))
	qa := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q")))
	m := last(ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", u("M")+"/IRT", b, qa))
	em := last(ex("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, ?)", f.exID, m, u("ES"), u("M")+"/IRT"))
	cyc := last(ex("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state) VALUES (?, ?, ?, ?)", em, f.exID, u("M")+"/IRT", string(cycleSt)))
	var exoid any
	if exchangeOrderID != "" {
		exoid = exchangeOrderID
	}
	ord := last(ex(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, exchange_order_id, state, order_type, limit_price, quantity)
		VALUES (?, ?, ?, 'buy', 'entry_buy', ?, ?, ?, 'limit', '100', ?)`, cyc, f.exID, em, u("loc"), exoid, string(orderSt), qty))
	return cyc, ord
}

func (f *ofix) seedLock(cycleID int64) int64 {
	f.t.Helper()
	oseq++
	res, err := f.db.Exec("INSERT INTO symbol_locks (scope, canonical_symbol, cycle_id, expires_at) VALUES (?, ?, ?, NOW(6)+INTERVAL 1 HOUR)",
		f.exCode, fmt.Sprintf("S%d_%d", f.exID, oseq), cycleID)
	if err != nil {
		f.t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id
}

func (f *ofix) seedReq(cycleID, orderID int64, typ queue.RequestType, status string) int64 {
	f.t.Helper()
	oseq++
	res, err := f.db.Exec(`INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, request_type, status, payload, idempotency_key)
		VALUES (?, ?, ?, ?, ?, '{}', ?)`, f.exID, cycleID, orderID, string(typ), status, fmt.Sprintf("idem_%d_%d", f.exID, oseq))
	if err != nil {
		f.t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id
}

func (f *ofix) tx(fn func(tx *sql.Tx) error) {
	f.t.Helper()
	if err := f.store.WithTx(f.ctx, fn); err != nil {
		f.t.Fatalf("tx: %v", err)
	}
}

func (f *ofix) orderState(id int64) string {
	var s string
	f.db.QueryRow("SELECT state FROM orders WHERE id=?", id).Scan(&s)
	return s
}
func (f *ofix) cycleState(id int64) string {
	var s string
	f.db.QueryRow("SELECT state FROM cycles WHERE id=?", id).Scan(&s)
	return s
}
func (f *ofix) lockState(id int64) string {
	var s string
	f.db.QueryRow("SELECT state FROM symbol_locks WHERE id=?", id).Scan(&s)
	return s
}
func (f *ofix) reqStatus(id int64) string {
	var s string
	f.db.QueryRow("SELECT status FROM exchange_requests WHERE id=?", id).Scan(&s)
	return s
}

// scheduledFollowup returns (id, status, nextRetryInFuture) of the newest request of
// the given type for an order.
func (f *ofix) scheduledFollowup(orderID int64, typ queue.RequestType) (int64, string, bool) {
	var id int64
	var status string
	var nextAt sql.NullTime
	err := f.db.QueryRow(`SELECT id, status, next_retry_at FROM exchange_requests
		WHERE order_id=? AND request_type=? ORDER BY id DESC LIMIT 1`, orderID, string(typ)).Scan(&id, &status, &nextAt)
	if err != nil {
		return 0, "", false
	}
	return id, status, nextAt.Valid && nextAt.Time.After(time.Now())
}

// ---- OnPlaceAck ----

func TestOnPlaceAckSchedulesCancel(t *testing.T) {
	f := setupO(t)
	cyc, ord := f.seed(state.CycleBuyRequestQueued, state.OrderQueued, "", "0.5")
	placeReq := f.seedReq(cyc, ord, queue.TypePlaceOrder, "IN_FLIGHT")
	intent := BuyIntentPayload{LocalClientOrderID: "c1-buy", MakerWaitBeforeCancelMs: 2000}
	ack := execution.OrderAck{ExchangeOrderID: "EX-1", ClientOrderID: "c1-buy"}

	f.tx(func(tx *sql.Tx) error {
		return OnPlaceAck(f.ctx, tx, f.q, PlaceAckParams{
			RequestID: placeReq, OrderID: ord, CycleID: cyc, ExchangeID: f.exID, Symbol: "X/IRT", Ack: ack, Intent: intent, RawResp: json.RawMessage(`{}`),
		})
	})

	if f.orderState(ord) != "ACKED" || f.cycleState(cyc) != "BUY_SUBMITTED" {
		t.Errorf("states = ord:%s cyc:%s, want ACKED/BUY_SUBMITTED", f.orderState(ord), f.cycleState(cyc))
	}
	if f.reqStatus(placeReq) != "SUCCEEDED" {
		t.Errorf("place request = %s, want SUCCEEDED", f.reqStatus(placeReq))
	}
	var exoid string
	f.db.QueryRow("SELECT COALESCE(exchange_order_id,'') FROM orders WHERE id=?", ord).Scan(&exoid)
	if exoid != "EX-1" {
		t.Errorf("exchange_order_id = %q, want EX-1", exoid)
	}
	// The cancel of the remainder is SCHEDULED in the future (queued wait, not slept).
	id, status, future := f.scheduledFollowup(ord, queue.TypeCancelOrder)
	if id == 0 || status != "RETRY_SCHEDULED" || !future {
		t.Errorf("scheduled cancel = id:%d status:%s future:%v, want a future RETRY_SCHEDULED", id, status, future)
	}
}

func TestOnCancelResultSchedulesFinalStatus(t *testing.T) {
	f := setupO(t)
	cyc, ord := f.seed(state.CycleBuySubmitted, state.OrderAcked, "EX-2", "0.5")
	cancelReq := f.seedReq(cyc, ord, queue.TypeCancelOrder, "IN_FLIGHT")

	f.tx(func(tx *sql.Tx) error {
		return OnCancelResult(f.ctx, tx, f.q, CancelResultParams{
			RequestID: cancelReq, OrderID: ord, CycleID: cyc, ExchangeID: f.exID, Symbol: "X/IRT",
			ExchangeOrderID: "EX-2", RawResp: json.RawMessage(`{}`), FinalCheckDelay: 500 * time.Millisecond,
		})
	})

	if f.orderState(ord) != "CANCEL_PENDING" {
		t.Errorf("order = %s, want CANCEL_PENDING", f.orderState(ord))
	}
	if f.reqStatus(cancelReq) != "SUCCEEDED" {
		t.Errorf("cancel request = %s, want SUCCEEDED", f.reqStatus(cancelReq))
	}
	id, status, _ := f.scheduledFollowup(ord, queue.TypeGetOrder)
	if id == 0 || status != "RETRY_SCHEDULED" {
		t.Errorf("scheduled final-status = id:%d status:%s, want RETRY_SCHEDULED", id, status)
	}
	var payload string
	f.db.QueryRow("SELECT payload FROM exchange_requests WHERE id=?", id).Scan(&payload)
	if !contains(payload, `"purpose":"final_status"`) {
		t.Errorf("final-status payload missing purpose: %s", payload)
	}
}

// ---- ProcessFinalStatus ----

// finalStatus drives a cycle to CANCEL_PENDING (the normal pre-final state) with a
// lock and a GET_ORDER request, then runs ProcessFinalStatus with the given status.
func (f *ofix) finalStatus(st execution.OrderStatus, statusErr error, requested string) (cyc, ord, lock, req int64, out Outcome) {
	f.t.Helper()
	cyc, ord = f.seed(state.CycleBuySubmitted, state.OrderCancelPending, "EX-F", requested)
	lock = f.seedLock(cyc)
	req = f.seedReq(cyc, ord, queue.TypeGetOrder, "IN_FLIGHT")
	f.tx(func(tx *sql.Tx) error {
		var err error
		out, err = ProcessFinalStatus(f.ctx, tx, f.q, FinalStatusParams{
			RequestID: req, OrderID: ord, CycleID: cyc, Scope: f.exCode, Status: st, StatusErr: statusErr, RawResp: json.RawMessage(`{}`),
		})
		return err
	})
	return
}

func TestProcessFinalStatusFull(t *testing.T) {
	f := setupO(t)
	status := execution.OrderStatus{Status: execution.StateFilled, ExchangeOrderID: "EX-F",
		IntendedQty: d("0.5"), FilledQty: d("0.5"), RemainingQty: d("0"), AvgPrice: d("100"), Fee: d("0.001"), FeeAsset: "USDT", Liquidity: "taker"}
	cyc, ord, lock, req, out := f.finalStatus(status, nil, "0.5")

	if out.Class != ClassFull {
		t.Fatalf("class = %s, want full", out.Class)
	}
	if f.orderState(ord) != "FILLED" || f.cycleState(cyc) != "BUY_FILLED" {
		t.Errorf("states = ord:%s cyc:%s, want FILLED/BUY_FILLED", f.orderState(ord), f.cycleState(cyc))
	}
	if f.lockState(lock) != "ACTIVE" {
		t.Error("lock must stay ACTIVE for a filled buy (inventory to sell)")
	}
	if f.reqStatus(req) != "SUCCEEDED" {
		t.Errorf("request = %s, want SUCCEEDED", f.reqStatus(req))
	}
	// Fill accounting on the order + a fills row.
	var filled, avg, fee, feeAsset, mode, result string
	f.db.QueryRow(`SELECT filled_quantity, avg_fill_price, fee_amount, COALESCE(fee_asset,''), COALESCE(actual_execution_mode,''), COALESCE(fill_result,'')
		FROM orders WHERE id=?`, ord).Scan(&filled, &avg, &fee, &feeAsset, &mode, &result)
	if !decimal.RequireFromString(filled).Equal(d("0.5")) || !decimal.RequireFromString(avg).Equal(d("100")) {
		t.Errorf("filled/avg = %s/%s, want 0.5/100", filled, avg)
	}
	if !decimal.RequireFromString(fee).Equal(d("0.001")) || feeAsset != "USDT" || mode != "TAKER" || result != "full" {
		t.Errorf("fee/asset/mode/result = %s/%s/%s/%s, want 0.001/USDT/TAKER/full", fee, feeAsset, mode, result)
	}
	var fills int
	f.db.QueryRow("SELECT COUNT(*) FROM fills WHERE order_id=?", ord).Scan(&fills)
	if fills != 1 {
		t.Errorf("fills = %d, want 1", fills)
	}
}

func TestProcessFinalStatusPartial(t *testing.T) {
	f := setupO(t)
	status := execution.OrderStatus{Status: execution.StateCanceled, ExchangeOrderID: "EX-F",
		IntendedQty: d("1.0"), FilledQty: d("0.4"), RemainingQty: d("0.6"), AvgPrice: d("100"), Liquidity: "maker"}
	cyc, ord, lock, _, out := f.finalStatus(status, nil, "1.0")

	if out.Class != ClassPartial {
		t.Fatalf("class = %s, want partial", out.Class)
	}
	if f.orderState(ord) != "PARTIALLY_FILLED" || f.cycleState(cyc) != "BUY_PARTIALLY_FILLED" {
		t.Errorf("states = ord:%s cyc:%s, want PARTIALLY_FILLED/BUY_PARTIALLY_FILLED", f.orderState(ord), f.cycleState(cyc))
	}
	if f.lockState(lock) != "ACTIVE" {
		t.Error("lock must stay ACTIVE for a partial fill (sell the filled part)")
	}
	var filled, remaining, mode string
	f.db.QueryRow("SELECT filled_quantity, remaining_quantity, COALESCE(actual_execution_mode,'') FROM orders WHERE id=?", ord).Scan(&filled, &remaining, &mode)
	if !decimal.RequireFromString(filled).Equal(d("0.4")) || !decimal.RequireFromString(remaining).Equal(d("0.6")) || mode != "MAKER" {
		t.Errorf("filled/remaining/mode = %s/%s/%s, want 0.4/0.6/MAKER (continue only filled qty)", filled, remaining, mode)
	}
}

func TestProcessFinalStatusZeroFillCancels(t *testing.T) {
	f := setupO(t)
	status := execution.OrderStatus{Status: execution.StateCanceled, ExchangeOrderID: "EX-F",
		IntendedQty: d("0.5"), FilledQty: d("0"), RemainingQty: d("0.5")}
	cyc, ord, lock, _, out := f.finalStatus(status, nil, "0.5")

	if out.Class != ClassZero || !out.LockReleased {
		t.Fatalf("out = %+v, want zero + lock released", out)
	}
	if f.orderState(ord) != "CANCELLED" || f.cycleState(cyc) != "CANCELLED" {
		t.Errorf("states = ord:%s cyc:%s, want CANCELLED/CANCELLED (clean no-fill, not FAILED)", f.orderState(ord), f.cycleState(cyc))
	}
	if f.lockState(lock) != "RELEASED" {
		t.Errorf("lock = %s, want RELEASED on zero-fill", f.lockState(lock))
	}
	var msg string
	f.db.QueryRow("SELECT message FROM cycle_state_events WHERE cycle_id=? ORDER BY id DESC LIMIT 1", cyc).Scan(&msg)
	if msg != ReasonZeroFill {
		t.Errorf("cycle reason = %q, want %s", msg, ReasonZeroFill)
	}
	var fills int
	f.db.QueryRow("SELECT COUNT(*) FROM fills WHERE order_id=?", ord).Scan(&fills)
	if fills != 0 {
		t.Errorf("zero-fill must record no fill row, got %d", fills)
	}
}

func TestProcessFinalStatusRejectedIsAmbiguous(t *testing.T) {
	f := setupO(t)
	// "rejected" arriving at final status (after a successful place+cancel) is
	// contradictory -> ambiguous -> NEEDS_RECONCILE, lock HELD (never released on a
	// contradictory state).
	status := execution.OrderStatus{Status: execution.StateRejected, ExchangeOrderID: "EX-F", IntendedQty: d("0.5"), FilledQty: d("0")}
	cyc, ord, lock, _, out := f.finalStatus(status, nil, "0.5")
	if out.Class != ClassAmbiguous || out.LockReleased {
		t.Fatalf("out = %+v, want ambiguous + lock held", out)
	}
	if f.orderState(ord) != "NEEDS_RECONCILE" || f.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Errorf("states = ord:%s cyc:%s, want NEEDS_RECONCILE both", f.orderState(ord), f.cycleState(cyc))
	}
	if f.lockState(lock) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (contradictory state)", f.lockState(lock))
	}
}

func TestOnPlaceRejectedCleanlyFailsAndReleasesLock(t *testing.T) {
	f := setupO(t)
	// A definite place rejection (never placed, no exposure): order+cycle FAILED, lock
	// released, request FAILED.
	cyc, ord := f.seed(state.CycleBuyRequestQueued, state.OrderQueued, "", "0.5")
	lock := f.seedLock(cyc)
	req := f.seedReq(cyc, ord, queue.TypePlaceOrder, "IN_FLIGHT")
	f.tx(func(tx *sql.Tx) error {
		return OnPlaceRejected(f.ctx, tx, f.q, PlaceRejectedParams{RequestID: req, OrderID: ord, CycleID: cyc, Cause: "insufficient balance"})
	})
	if f.orderState(ord) != "FAILED" || f.cycleState(cyc) != "FAILED" {
		t.Errorf("states = ord:%s cyc:%s, want FAILED both", f.orderState(ord), f.cycleState(cyc))
	}
	if f.lockState(lock) != "RELEASED" {
		t.Errorf("lock = %s, want RELEASED (no exposure)", f.lockState(lock))
	}
	if f.reqStatus(req) != "FAILED" {
		t.Errorf("request = %s, want FAILED", f.reqStatus(req))
	}
}

func TestProcessFinalStatusMissingIsNotZeroFill(t *testing.T) {
	f := setupO(t)
	// GetOrder said the order is unknown/missing. That is NOT proof of zero fill.
	cyc, ord, lock, _, out := f.finalStatus(execution.OrderStatus{Status: execution.StateUnknown}, execution.ErrOrderUnknown, "0.5")
	if out.Class != ClassAmbiguous || out.LockReleased {
		t.Fatalf("out = %+v, want ambiguous + lock held", out)
	}
	if f.orderState(ord) != "NEEDS_RECONCILE" || f.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Errorf("states = ord:%s cyc:%s, want NEEDS_RECONCILE both", f.orderState(ord), f.cycleState(cyc))
	}
	if f.lockState(lock) != "ACTIVE" {
		t.Errorf("lock = %s, want still ACTIVE (ambiguous, no proof of no exposure)", f.lockState(lock))
	}
}

func TestProcessFinalStatusUnknownLiquidityIsUnknownMode(t *testing.T) {
	f := setupO(t)
	status := execution.OrderStatus{Status: execution.StateFilled, ExchangeOrderID: "EX-F",
		IntendedQty: d("0.5"), FilledQty: d("0.5"), RemainingQty: d("0"), AvgPrice: d("100")} // no Liquidity
	_, ord, _, _, _ := f.finalStatus(status, nil, "0.5")
	var mode string
	f.db.QueryRow("SELECT COALESCE(actual_execution_mode,'') FROM orders WHERE id=?", ord).Scan(&mode)
	if mode != "UNKNOWN" {
		t.Errorf("actual_execution_mode = %q, want UNKNOWN when the venue reports no liquidity", mode)
	}
}

func TestProcessFinalStatusIdempotent(t *testing.T) {
	f := setupO(t)
	cyc, ord := f.seed(state.CycleBuySubmitted, state.OrderCancelPending, "EX-F", "0.5")
	lock := f.seedLock(cyc)
	req := f.seedReq(cyc, ord, queue.TypeGetOrder, "IN_FLIGHT")
	status := execution.OrderStatus{Status: execution.StateFilled, ExchangeOrderID: "EX-F",
		IntendedQty: d("0.5"), FilledQty: d("0.5"), RemainingQty: d("0"), AvgPrice: d("100")}
	run := func() {
		f.tx(func(tx *sql.Tx) error {
			_, err := ProcessFinalStatus(f.ctx, tx, f.q, FinalStatusParams{RequestID: req, OrderID: ord, CycleID: cyc, Scope: f.exCode, Status: status, RawResp: json.RawMessage(`{}`)})
			return err
		})
	}
	run()
	run() // repeated processing must not duplicate fills or events

	var fills, oevents int
	f.db.QueryRow("SELECT COUNT(*) FROM fills WHERE order_id=?", ord).Scan(&fills)
	f.db.QueryRow("SELECT COUNT(*) FROM order_events WHERE order_id=? AND to_state='FILLED'", ord).Scan(&oevents)
	if fills != 1 {
		t.Errorf("fills after repeat = %d, want 1 (idempotent)", fills)
	}
	if oevents != 1 {
		t.Errorf("FILLED order_events after repeat = %d, want 1 (no duplicate event)", oevents)
	}
	_ = lock
}

func TestProcessFinalStatusRollbackOnFailure(t *testing.T) {
	f := setupO(t)
	cyc, ord := f.seed(state.CycleBuySubmitted, state.OrderCancelPending, "EX-F", "0.5")
	req := f.seedReq(cyc, ord, queue.TypeGetOrder, "IN_FLIGHT")
	status := execution.OrderStatus{Status: execution.StateFilled, ExchangeOrderID: "EX-F",
		IntendedQty: d("0.5"), FilledQty: d("0.5"), RemainingQty: d("0"), AvgPrice: d("100")}
	// A non-existent cycle id makes the fill insert (cycle_id FK) fail mid-tx; the
	// WHOLE transaction must roll back: no accounting persisted, request not succeeded.
	err := f.store.WithTx(f.ctx, func(tx *sql.Tx) error {
		_, perr := ProcessFinalStatus(f.ctx, tx, f.q, FinalStatusParams{RequestID: req, OrderID: ord, CycleID: 999999999, Scope: f.exCode, Status: status, RawResp: json.RawMessage(`{}`)})
		return perr
	})
	if err == nil {
		t.Fatal("expected an error from the FK violation")
	}
	if f.reqStatus(req) == "SUCCEEDED" {
		t.Error("request must NOT be SUCCEEDED when processing failed (atomic rollback)")
	}
	var filled sql.NullString
	f.db.QueryRow("SELECT filled_quantity FROM orders WHERE id=?", ord).Scan(&filled)
	if filled.Valid && decimal.RequireFromString(filled.String).IsPositive() {
		t.Error("order accounting must be rolled back on failure")
	}
	if f.orderState(ord) != "CANCEL_PENDING" {
		t.Errorf("order state changed despite rollback: %s", f.orderState(ord))
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
