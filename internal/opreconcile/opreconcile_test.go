package opreconcile

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/migrate"
)

var rseq int

type rfix struct {
	t      *testing.T
	ctx    context.Context
	db     *sql.DB
	r      *Resolver
	exID   int64
	emID   int64
	cycID  int64
	buyID  int64
	sellID int64
	base   string
}

func setupR(t *testing.T) *rfix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the opreconcile integration test")
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
	return &rfix{t: t, ctx: ctx, db: db, r: New(db, clock.NewSystem(), nil)}
}

func (f *rfix) exec(q string, a ...any) sql.Result {
	r, err := f.db.Exec(q, a...)
	if err != nil {
		f.t.Fatalf("exec %q: %v", q, err)
	}
	return r
}

func (f *rfix) lastID(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }

// seed creates a NEEDS_RECONCILE cycle with an entry_buy order (and optionally an
// exit_sell) plus an ACTIVE symbol lock. Quantities are decimal strings.
func (f *rfix) seed(buyQty, buyFilled string, withSell bool, sellQty, sellFilled string) {
	rseq++
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), rseq) }
	f.base = u("BAS")
	symbol := f.base + "/IRT"
	scope := u("scope")
	f.exID = f.lastID(f.exec("INSERT INTO exchanges (code, name, enabled, live_enabled) VALUES (?, 'R', 1, 0)", u("rx")))
	b := f.lastID(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", f.base))
	qa := f.lastID(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q")))
	m := f.lastID(f.exec("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", symbol, b, qa))
	f.emID = f.lastID(f.exec("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, ?)", f.exID, m, u("ES"), symbol))
	f.cycID = f.lastID(f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, version) VALUES (?, ?, ?, 'NEEDS_RECONCILE', 3)", f.emID, f.exID, symbol))
	f.buyID = f.lastID(f.exec(
		"INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, version, quantity, filled_quantity, limit_price) VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'NEEDS_RECONCILE', 2, ?, ?, '10')",
		f.cycID, f.exID, f.emID, u("bo"), buyQty, buyFilled))
	if withSell {
		f.sellID = f.lastID(f.exec(
			"INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, version, quantity, filled_quantity, limit_price) VALUES (?, ?, ?, 'sell', 'exit_sell', ?, 'NEEDS_RECONCILE', 2, ?, ?, '11')",
			f.cycID, f.exID, f.emID, u("so"), sellQty, sellFilled))
	}
	f.exec("INSERT INTO symbol_locks (scope, canonical_symbol, cycle_id, state, expires_at) VALUES (?, ?, ?, 'ACTIVE', NOW(6) + INTERVAL 600 SECOND)", scope, symbol, f.cycID)
}

func (f *rfix) cycleState() string {
	var s string
	f.db.QueryRow("SELECT state FROM cycles WHERE id=?", f.cycID).Scan(&s)
	return s
}
func (f *rfix) orderState(id int64) string {
	var s string
	f.db.QueryRow("SELECT state FROM orders WHERE id=?", id).Scan(&s)
	return s
}
func (f *rfix) lockState() string {
	var s string
	f.db.QueryRow("SELECT state FROM symbol_locks WHERE cycle_id=?", f.cycID).Scan(&s)
	return s
}
func (f *rfix) auditCount() int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM reconcile_resolutions WHERE cycle_id=?", f.cycID).Scan(&n)
	return n
}
func (f *rfix) fillCount(orderID int64) int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM fills WHERE order_id=?", orderID).Scan(&n)
	return n
}
func (f *rfix) cycleEventCount(to string) int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM cycle_state_events WHERE cycle_id=? AND to_state=?", f.cycID, to).Scan(&n)
	return n
}

func (f *rfix) buyFill(id string, qty, price string) *FillData {
	return &FillData{ExchangeFillID: id, Quantity: qty, Price: price, Side: "buy"}
}
func (f *rfix) sellFill(id string, qty, price string) *FillData {
	return &FillData{ExchangeFillID: id, Quantity: qty, Price: price, Side: "sell"}
}

// ---- tests ----

func TestCancelZeroExposureClosesAndReleasesLock(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "") // no fills => zero exposure
	res, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "no exposure", Operator: "op1"})
	if err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "CANCELLED" || f.orderState(f.buyID) != "CANCELLED" {
		t.Errorf("states = cyc:%s buy:%s, want CANCELLED/CANCELLED", f.cycleState(), f.orderState(f.buyID))
	}
	if f.lockState() != "RELEASED" || !res.LockReleased {
		t.Errorf("lock = %s (released=%v), want RELEASED", f.lockState(), res.LockReleased)
	}
	if f.auditCount() != 1 {
		t.Errorf("audit rows = %d, want 1", f.auditCount())
	}
	// State change went through the state machine (an event row exists).
	if f.cycleEventCount("CANCELLED") == 0 {
		t.Error("expected a cycle_state_events row for CANCELLED (state machine used)")
	}
}

func TestCancelZeroExposureRejectedWithOpenExposure(t *testing.T) {
	f := setupR(t)
	f.seed("1", "1", false, "", "") // buy filled 1, no sells => exposure 1
	_, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "x", Operator: "op1"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (open exposure)", err)
	}
	if f.cycleState() != "NEEDS_RECONCILE" || f.lockState() != "ACTIVE" {
		t.Error("rejected resolution must not mutate state or release the lock")
	}
}

func TestMarkBuyFilledRecordsFillHoldsLock(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	res, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionMarkBuyFilled, Fill: f.buyFill("bf1", "1", "10"), Reason: "confirmed buy", Operator: "op1"})
	if err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "BUY_FILLED" || f.orderState(f.buyID) != "FILLED" {
		t.Errorf("states = cyc:%s buy:%s, want BUY_FILLED/FILLED", f.cycleState(), f.orderState(f.buyID))
	}
	if f.fillCount(f.buyID) != 1 {
		t.Errorf("buy fills = %d, want 1", f.fillCount(f.buyID))
	}
	var filled string
	f.db.QueryRow("SELECT filled_quantity FROM orders WHERE id=?", f.buyID).Scan(&filled)
	if filled != "1.000000000000000000" {
		t.Errorf("buy filled_quantity = %s, want 1", filled)
	}
	if f.lockState() != "ACTIVE" || res.LockReleased {
		t.Error("buy fill keeps the lock (real inventory) — must NOT release")
	}
}

func TestMarkSellFilledClosesAndReleasesLock(t *testing.T) {
	f := setupR(t)
	f.seed("1", "1", true, "1", "0") // bought 1, sell order for 1, nothing sold yet
	res, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionMarkSellFilled, Fill: f.sellFill("sf1", "1", "12"), Reason: "confirmed full sell", Operator: "op1"})
	if err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "CLOSED" || f.orderState(f.sellID) != "FILLED" {
		t.Errorf("states = cyc:%s sell:%s, want CLOSED/FILLED", f.cycleState(), f.orderState(f.sellID))
	}
	if f.lockState() != "RELEASED" || !res.LockReleased {
		t.Errorf("full exit must release the lock; got %s", f.lockState())
	}
	var realized sql.NullString
	f.db.QueryRow("SELECT realized_quote FROM cycles WHERE id=?", f.cycID).Scan(&realized)
	if !realized.Valid {
		t.Error("close should write realized_quote accounting")
	}
}

func TestMarkSellPartialKeepsLock(t *testing.T) {
	f := setupR(t)
	f.seed("2", "2", true, "2", "0") // bought 2; sell 1 of 2
	res, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionMarkSellPartiallyFilled, Fill: f.sellFill("sp1", "1", "12"), Reason: "partial sell", Operator: "op1"})
	if err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "SELL_PARTIALLY_FILLED" || f.orderState(f.sellID) != "PARTIALLY_FILLED" {
		t.Errorf("states = cyc:%s sell:%s", f.cycleState(), f.orderState(f.sellID))
	}
	if f.lockState() != "ACTIVE" || res.LockReleased {
		t.Error("partial sell still holds inventory — lock must NOT be released")
	}
}

func TestDuplicateFillRejected(t *testing.T) {
	f := setupR(t)
	f.seed("1", "1", true, "1", "0")
	// Pre-existing fill with the same id on the sell order.
	f.exec("INSERT INTO fills (order_id, cycle_id, exchange_fill_id, quantity, price) VALUES (?, ?, 'dupe', '0.5', '12')", f.sellID, f.cycID)
	_, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionMarkSellPartiallyFilled, Fill: f.sellFill("dupe", "0.4", "12"), Reason: "x", Operator: "op1"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (duplicate fill)", err)
	}
	if f.cycleState() != "NEEDS_RECONCILE" {
		t.Error("duplicate-fill rejection must not mutate state")
	}
}

func TestInvalidQuantityRejected(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	for _, bad := range []string{"0", "-1", "abc", ""} {
		_, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionMarkBuyFilled, Fill: f.buyFill("q", bad, "10"), Reason: "x", Operator: "op1"})
		if !IsValidation(err) {
			t.Errorf("qty %q: err = %v, want ValidationError", bad, err)
		}
	}
	if f.cycleState() != "NEEDS_RECONCILE" {
		t.Error("invalid-qty rejections must not mutate state")
	}
}

func TestOversellRejected(t *testing.T) {
	f := setupR(t)
	f.seed("2", "1", true, "2", "0") // bought only 1
	_, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionMarkSellPartiallyFilled, Fill: f.sellFill("os", "2", "12"), Reason: "x", Operator: "op1"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (oversell)", err)
	}
}

func TestAttachExchangeOrderID(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	if _, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionAttachExchangeOrderID, OrderID: f.buyID, ExchangeOrderID: "EX-123", Reason: "found it", Operator: "op1"}); err != nil {
		t.Fatal(err)
	}
	var oid string
	f.db.QueryRow("SELECT exchange_order_id FROM orders WHERE id=?", f.buyID).Scan(&oid)
	if oid != "EX-123" {
		t.Errorf("exchange_order_id = %q, want EX-123", oid)
	}
	// No state change; lock held.
	if f.orderState(f.buyID) != "NEEDS_RECONCILE" || f.lockState() != "ACTIVE" {
		t.Error("attach must not change state or release the lock")
	}
	if f.auditCount() != 1 {
		t.Error("attach must be audited")
	}
}

func TestKeepNeedsReconcileLockNotReleased(t *testing.T) {
	f := setupR(t)
	f.seed("1", "1", false, "", "") // ambiguous: buy filled but unclear
	res, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionKeepNeedsReconcile, Reason: "still investigating", Operator: "op1"})
	if err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "NEEDS_RECONCILE" || f.lockState() != "ACTIVE" || res.LockReleased {
		t.Error("keep must leave the cycle in NEEDS_RECONCILE with the lock held")
	}
	if f.auditCount() != 1 {
		t.Error("keep must still write an audit row")
	}
}

func TestMarkFailedWithExposureKeepsLock(t *testing.T) {
	f := setupR(t)
	f.seed("1", "1", false, "", "") // exposure of 1, never sold
	res, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionMarkFailed, Reason: "unrecoverable", Operator: "op1"})
	if err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "FAILED" {
		t.Errorf("cycle = %s, want FAILED", f.cycleState())
	}
	// Exposure remains -> the lock must NOT be released on a button alone.
	if f.lockState() != "ACTIVE" || res.LockReleased {
		t.Error("FAILED with open exposure must KEEP the lock")
	}
	if len(res.Warnings) == 0 {
		t.Error("expected a warning about stranded exposure")
	}
}

func TestPreviewDoesNotMutate(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	plan, err := f.r.Preview(f.ctx, Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "x", Operator: "op1"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.NewCycleState != "CANCELLED" || !plan.LockReleased {
		t.Errorf("preview plan = %+v, want CANCELLED + lock released", plan)
	}
	// Nothing changed.
	if f.cycleState() != "NEEDS_RECONCILE" || f.lockState() != "ACTIVE" || f.auditCount() != 0 {
		t.Error("preview must not mutate state, lock, or write an audit row")
	}
}

func TestPreviewBalanceCrossCheckWarns(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	// Balance shows a big base holding, but cancel_zero_exposure implies ~0 held.
	f.exec("INSERT INTO wallet_balances_current (exchange_id, asset, available, locked, total) VALUES (?, ?, '5', '0', '5')", f.exID, f.base)
	plan, err := f.r.Preview(f.ctx, Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "x", Operator: "op1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Warnings) == 0 {
		t.Error("expected a balance cross-check warning (advisory)")
	}
}

func TestApplyRequiresReason(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	if _, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Operator: "op1"}); !IsValidation(err) {
		t.Errorf("missing reason err = %v, want ValidationError", err)
	}
}
