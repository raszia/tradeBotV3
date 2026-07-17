package opreconcile

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
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

// seed creates a NEEDS_RECONCILE cycle with an entry_buy order (and optionally an exit_sell) plus
// an ACTIVE symbol lock. Orders start in NEEDS_RECONCILE (ambiguous). Quantities are decimal strings.
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
		"INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, version, quantity, filled_quantity, quote_spent, avg_fill_price, limit_price) VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'NEEDS_RECONCILE', 2, ?, ?, ?*10, '10', '10')",
		f.cycID, f.exID, f.emID, u("bo"), buyQty, buyFilled, buyFilled))
	if withSell {
		f.sellID = f.lastID(f.exec(
			"INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, version, quantity, filled_quantity, limit_price) VALUES (?, ?, ?, 'sell', 'exit_sell', ?, 'NEEDS_RECONCILE', 2, ?, ?, '11')",
			f.cycID, f.exID, f.emID, u("so"), sellQty, sellFilled))
	}
	f.exec("INSERT INTO symbol_locks (scope, canonical_symbol, cycle_id, state, expires_at) VALUES (?, ?, ?, 'ACTIVE', NOW(6) + INTERVAL 600 SECOND)", scope, symbol, f.cycID)
}

// provenZero flips every order to a NEVER-SENT state (QUEUED, no venue id) so a zero recorded fill
// classifies as PROVEN_ZERO. (A NEEDS_RECONCILE order with no fill is UNKNOWN, not proven zero.)
func (f *rfix) provenZero() {
	f.exec("UPDATE orders SET state='QUEUED', exchange_order_id=NULL WHERE cycle_id=?", f.cycID)
}

func (f *rfix) setOrderState(id int64, st string) {
	f.exec("UPDATE orders SET state=? WHERE id=?", st, id)
}
func (f *rfix) setExoid(id int64, v string) {
	f.exec("UPDATE orders SET exchange_order_id=? WHERE id=?", v, id)
}

func (f *rfix) seedSell(qty, filled, st string) int64 {
	rseq++
	return f.lastID(f.exec(
		"INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, version, quantity, filled_quantity, limit_price) VALUES (?, ?, ?, 'sell', 'exit_sell', ?, ?, 2, ?, ?, '11')",
		f.cycID, f.exID, f.emID, fmt.Sprintf("so_x%d_%d", time.Now().UnixNano(), rseq), st, qty, filled))
}

func (f *rfix) seedReq(orderID int64, typ, status string) int64 {
	rseq++
	var oid any
	if orderID != 0 {
		oid = orderID
	}
	return f.lastID(f.exec(
		"INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, symbol, request_type, priority, status, payload, timeout_ms, max_retries, idempotency_key) VALUES (?,?,?,?,?,50,?,'{}',10000,5,?)",
		f.exID, f.cycID, oid, f.base+"/IRT", typ, status, fmt.Sprintf("req%d_%d_%d", orderID, time.Now().UnixNano(), rseq)))
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

func (f *rfix) buyFill(id, qty, price string) *FillData {
	return &FillData{ExchangeFillID: id, Quantity: qty, Price: price, Side: "buy"}
}
func (f *rfix) sellFill(id, qty, price string) *FillData {
	return &FillData{ExchangeFillID: id, Quantity: qty, Price: price, Side: "sell"}
}

// applyVia runs the real two-phase flow: Preview → carry the state fingerprint → Apply. Operator
// defaults to "op1". This is how the dashboard drives it (preview then apply-with-token).
func (f *rfix) applyVia(req Request) (Result, error) {
	if req.Operator == "" {
		req.Operator = "op1"
	}
	plan, err := f.r.Preview(f.ctx, req)
	if err != nil {
		return Result{}, err
	}
	req.ExpectedStateHash = plan.StateHash
	return f.r.Apply(f.ctx, req)
}

// ---- exposure classification (blocker 1) ----

func TestExposureRecordedZeroWithAmbiguousIsUnknown(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "") // buy filled 0 but state NEEDS_RECONCILE (may have executed)
	class, net, err := f.r.Classify(f.ctx, f.cycID)
	if err != nil {
		t.Fatal(err)
	}
	if class != ExposureUnknown {
		t.Errorf("class = %s, want UNKNOWN (recorded 0 on an ambiguous order is not proof)", class)
	}
	if net != "0" {
		t.Errorf("net = %s, want 0", net)
	}
}

func TestCancelZeroExposureRejectedForUnknown(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "") // UNKNOWN (order NEEDS_RECONCILE)
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "close"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (UNKNOWN exposure)", err)
	}
	if f.cycleState() != "NEEDS_RECONCILE" || f.lockState() != "ACTIVE" {
		t.Error("a refused zero-exposure resolution must not mutate state or release the lock")
	}
}

func TestMarkBuyZeroFilledRejectedForUnknown(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	f.setExoid(f.buyID, "EX-maybe") // reached the venue -> zero fill is unproven
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkBuyZeroFilled, Reason: "nothing"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (UNKNOWN exposure)", err)
	}
}

func TestCancelZeroExposureProvenZeroReleasesLock(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	f.provenZero() // buy never sent (QUEUED, no venue id) -> PROVEN_ZERO
	res, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "never sent"})
	if err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "CANCELLED" || f.orderState(f.buyID) != "CANCELLED" {
		t.Errorf("states = cyc:%s buy:%s, want CANCELLED/CANCELLED", f.cycleState(), f.orderState(f.buyID))
	}
	if f.lockState() != "RELEASED" || !res.LockReleased {
		t.Errorf("proven-zero cancel must release the lock; got %s", f.lockState())
	}
	if res.ExposureClass != ExposureProvenZero {
		t.Errorf("exposure class = %s, want PROVEN_ZERO", res.ExposureClass)
	}
	if f.cycleEventCount("CANCELLED") == 0 {
		t.Error("expected a cycle_state_events row (state machine used)")
	}
}

func TestCancelZeroExposureUnknownAllowedWithExternalConfirmation(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "") // UNKNOWN
	res, err := f.applyVia(Request{
		CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "verified on venue UI",
		ExternalResolutionConfirmed: true, ExternalResolutionReason: "exchange shows no open order and no position",
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "CANCELLED" || f.lockState() != "RELEASED" {
		t.Errorf("external-confirmed cancel should close + release; cyc:%s lock:%s", f.cycleState(), f.lockState())
	}
	if !res.ExternalConfirmed {
		t.Error("plan must record external confirmation")
	}
}

func TestCancelZeroExposureRejectedWithOpenExposure(t *testing.T) {
	f := setupR(t)
	f.seed("1", "1", false, "", "") // buy filled 1 -> OPEN
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "x"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (open exposure)", err)
	}
}

func TestInconsistentExposureRejected(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", true, "1", "1") // sold 1, bought 0 -> net -1 (INCONSISTENT)
	class, _, _ := f.r.Classify(f.ctx, f.cycID)
	if class != ExposureInconsistent {
		t.Fatalf("class = %s, want INCONSISTENT", class)
	}
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionKeepNeedsReconcile, Reason: "x"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (inconsistent data)", err)
	}
}

// ---- order ownership + role (blocker 3) ----

func TestBuyActionOnSellOrderRejected(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", true, "1", "0")
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkBuyFilled, OrderID: f.sellID, Fill: f.buyFill("x", "1", "10"), Reason: "wrong"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (buy action on a sell order)", err)
	}
}

func TestSellActionOnBuyOrderRejected(t *testing.T) {
	f := setupR(t)
	f.seed("1", "1", true, "1", "0")
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkSellFilled, OrderID: f.buyID, Fill: f.sellFill("x", "1", "12"), Reason: "wrong"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (sell action on a buy order)", err)
	}
}

func TestOrderFromAnotherCycleRejected(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	// A second, unrelated cycle + buy order.
	other := f.lastID(f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, version) VALUES (?, ?, ?, 'NEEDS_RECONCILE', 1)", f.emID, f.exID, f.base+"/IRT"))
	otherOrder := f.lastID(f.exec("INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, version, quantity, filled_quantity) VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'NEEDS_RECONCILE', 1, '1', '0')", other, f.exID, f.emID, fmt.Sprintf("other_bo_%d", time.Now().UnixNano())))
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkBuyFilled, OrderID: otherOrder, Fill: f.buyFill("x", "1", "10"), Reason: "wrong cycle"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (order belongs to another cycle)", err)
	}
}

func TestActiveRequestOwnershipDerivedFromOrder(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	f.provenZero()
	// A request whose CLAIMED cycle_id points at a DIFFERENT (real) cycle, but whose order_id is
	// THIS cycle's buy. Ownership must derive from the order, so it still blocks this cycle.
	otherCyc := f.lastID(f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, version) VALUES (?, ?, ?, 'NEEDS_RECONCILE', 1)", f.emID, f.exID, f.base+"/IRT"))
	f.exec("INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, symbol, request_type, priority, status, payload, timeout_ms, max_retries, idempotency_key) VALUES (?, ?, ?, ?, 'PLACE_ORDER', 50, 'IN_FLIGHT', '{}', 10000, 5, ?)",
		f.exID, otherCyc, f.buyID, f.base+"/IRT", fmt.Sprintf("lie_%d_%d", time.Now().UnixNano(), rseq))
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "close"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError — the request is owned via its order, not its (false) cycle_id", err)
	}
}

// ---- full/partial fills (blocker 4) ----

func TestBuyPartialFillClassification(t *testing.T) {
	f := setupR(t)
	f.seed("2", "0", false, "", "")
	res, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkBuyPartiallyFilled, Fill: f.buyFill("bp", "1", "10"), Reason: "partial"})
	if err != nil {
		t.Fatal(err)
	}
	if f.orderState(f.buyID) != "PARTIALLY_FILLED" || f.cycleState() != "BUY_PARTIALLY_FILLED" {
		t.Errorf("states = order:%s cycle:%s, want PARTIALLY_FILLED/BUY_PARTIALLY_FILLED", f.orderState(f.buyID), f.cycleState())
	}
	if f.lockState() != "ACTIVE" || res.LockReleased {
		t.Error("partial buy holds inventory — lock must be held")
	}
}

func TestBuyCompleteFillClassification(t *testing.T) {
	f := setupR(t)
	f.seed("2", "1", false, "", "") // already 1 of 2; fill the other 1 -> complete
	if _, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkBuyFilled, Fill: f.buyFill("bf", "1", "10"), Reason: "done"}); err != nil {
		t.Fatal(err)
	}
	if f.orderState(f.buyID) != "FILLED" || f.cycleState() != "BUY_FILLED" {
		t.Errorf("states = order:%s cycle:%s, want FILLED/BUY_FILLED", f.orderState(f.buyID), f.cycleState())
	}
}

func TestMarkBuyFilledRejectsPartial(t *testing.T) {
	f := setupR(t)
	f.seed("2", "0", false, "", "")
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkBuyFilled, Fill: f.buyFill("bf", "1", "10"), Reason: "x"}) // only 1 of 2
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (fill does not complete the order)", err)
	}
}

func TestMarkSellPartialClosingExposureRejected(t *testing.T) {
	f := setupR(t)
	f.seed("1", "1", true, "1", "0") // bought 1; a partial sell of the whole 1 would close it
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkSellPartiallyFilled, Fill: f.sellFill("sp", "1", "12"), Reason: "x"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (closes exposure — use mark_sell_filled)", err)
	}
}

func TestMarkSellFilledClosesAndReleasesLock(t *testing.T) {
	f := setupR(t)
	f.seed("1", "1", true, "1", "0")
	res, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkSellFilled, Fill: f.sellFill("sf", "1", "12"), Reason: "full exit"})
	if err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "CLOSED" || f.orderState(f.sellID) != "FILLED" {
		t.Errorf("states = cyc:%s sell:%s, want CLOSED/FILLED", f.cycleState(), f.orderState(f.sellID))
	}
	if f.lockState() != "RELEASED" || !res.LockReleased {
		t.Errorf("full exit must release the lock; got %s", f.lockState())
	}
}

// BLOCKER 1: zero cycle exposure alone is NOT sufficient to close — if the selected sell order is
// only PARTIALLY filled, its unfilled remainder may still be open on the venue. mark_sell_filled
// must REFUSE unless the remainder is confirmed cancelled or the order is fully filled.
func TestSellFilledRefusedWhenSelectedOrderHasExecutableRemainder(t *testing.T) {
	f := setupR(t)
	f.seed("2", "2", true, "1", "1") // bought 2; sell #1 already sold 1 (FILLED)
	f.setOrderState(f.sellID, "FILLED")
	sell2 := f.seedSell("2", "0", "NEEDS_RECONCILE") // sell #2 order for 2, unfilled
	// net exposure would reach 0 (2 - 1 - 1), but sell2 (qty 2) is only PARTIALLY filled after 1.
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkSellFilled, OrderID: sell2, Fill: f.sellFill("s2", "1", "12"), Reason: "close remainder"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (sell order still has an executable remainder)", err)
	}
	if f.cycleState() != "NEEDS_RECONCILE" || f.lockState() != "ACTIVE" {
		t.Error("a refused close must keep NEEDS_RECONCILE + hold the lock")
	}
}

// With the remainder confirmed cancelled externally, the close is allowed: the selected order is
// terminal CANCELLED (its recorded partial fill preserved), the cycle CLOSES, the lock releases.
func TestSellFilledPartialAllowedWithConfirmedCancelledRemainder(t *testing.T) {
	f := setupR(t)
	f.seed("2", "2", true, "1", "1")
	f.setOrderState(f.sellID, "FILLED")
	sell2 := f.seedSell("2", "0", "NEEDS_RECONCILE")
	res, err := f.applyVia(Request{
		CycleID: f.cycID, Action: ActionMarkSellFilled, OrderID: sell2, Fill: f.sellFill("s2b", "1", "12"),
		Reason: "close", ExternalResolutionConfirmed: true, ExternalResolutionReason: "venue shows the remainder cancelled",
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "CLOSED" || f.lockState() != "RELEASED" || !res.LockReleased {
		t.Errorf("confirmed close: cyc:%s lock:%s", f.cycleState(), f.lockState())
	}
	if f.orderState(sell2) != "CANCELLED" {
		t.Errorf("sell2 = %s, want CANCELLED (remainder cancelled, partial fill preserved) — never an active PARTIALLY_FILLED", f.orderState(sell2))
	}
	var filledOK bool
	f.db.QueryRow("SELECT filled_quantity = 1 FROM orders WHERE id=?", sell2).Scan(&filledOK)
	if !filledOK {
		t.Error("sell2 filled_quantity must be 1 (partial fill preserved)")
	}
}

// BLOCKER 2: a partial sell that COMPLETES the selected sell order while exposure remains, with NO
// other active sell, must land in a state that deterministically creates the next sell.
func TestSellPartialCompletesOrderRemainingExposureQueuesNextSell(t *testing.T) {
	f := setupR(t)
	f.seed("2", "2", true, "1", "0") // bought 2; a single sell order of qty 1
	// Fill the sell order completely (1 of 1). Remaining exposure = 2 - 1 = 1. No other active sell.
	res, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkSellPartiallyFilled, Fill: f.sellFill("sp1", "1", "12"), Reason: "partial exit"})
	if err != nil {
		t.Fatal(err)
	}
	if f.orderState(f.sellID) != "FILLED" {
		t.Errorf("sell order = %s, want FILLED", f.orderState(f.sellID))
	}
	if f.cycleState() != "SELL_REPRICE_PENDING" {
		t.Errorf("cycle = %s, want SELL_REPRICE_PENDING (remaining exposure, no active sell → next-sell path)", f.cycleState())
	}
	if f.lockState() != "ACTIVE" || res.LockReleased {
		t.Error("remaining exposure must hold the lock")
	}
}

// ---- active requests (blocker 2) ----

func TestActiveRequestBlocksClose(t *testing.T) {
	for _, tc := range []struct{ typ, status string }{
		{"PLACE_ORDER", "QUEUED"}, {"PLACE_ORDER", "CLAIMED"}, {"CANCEL_ORDER", "IN_FLIGHT"},
		{"PLACE_ORDER", "RETRY_SCHEDULED"}, {"GET_ORDER", "QUEUED"}, {"GET_ORDER", "IN_FLIGHT"},
	} {
		t.Run(tc.typ+"_"+tc.status, func(t *testing.T) {
			f := setupR(t)
			f.seed("1", "0", false, "", "")
			f.provenZero() // exposure proven zero...
			f.seedReq(f.buyID, tc.typ, tc.status)
			// ...but an active request can still execute -> resolving the cycle out is refused.
			_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "close"})
			if !IsValidation(err) {
				t.Fatalf("%s/%s: err = %v, want ValidationError (active request blocks close)", tc.typ, tc.status, err)
			}
			if f.cycleState() != "NEEDS_RECONCILE" || f.lockState() != "ACTIVE" {
				t.Error("blocked close must not mutate state or release the lock")
			}
		})
	}
}

func TestTerminalRequestDoesNotBlock(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	f.provenZero()
	f.seedReq(f.buyID, "PLACE_ORDER", "DEAD") // terminal -> not blocking
	if _, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "close"}); err != nil {
		t.Fatalf("a terminal (DEAD) request must not block: %v", err)
	}
	if f.cycleState() != "CANCELLED" {
		t.Errorf("cycle = %s, want CANCELLED", f.cycleState())
	}
}

// ---- terminal-order fill correction (blocker 5) ----

func TestFillOnTerminalOrderRejected(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	f.setOrderState(f.buyID, "CANCELLED")
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkBuyFilled, Fill: f.buyFill("x", "1", "10"), Reason: "x"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (fill on a terminal order must use correct_terminal_order_fill)", err)
	}
}

func TestCorrectTerminalOrderFill(t *testing.T) {
	for _, term := range []string{"CANCELLED", "FAILED"} {
		t.Run(term, func(t *testing.T) {
			f := setupR(t)
			f.seed("1", "0", false, "", "")
			f.setOrderState(f.buyID, term)
			req := Request{CycleID: f.cycID, Action: ActionCorrectTerminalOrderFill, OrderID: f.buyID, Fill: f.buyFill("disc", "1", "10"), Reason: "discovered a late fill", Operator: "op1"}
			// Preview and apply must report the SAME resulting order state.
			plan, perr := f.r.Preview(f.ctx, req)
			if perr != nil {
				t.Fatal(perr)
			}
			if plan.NewOrderState != "FILLED" {
				t.Errorf("preview new order state = %s, want FILLED", plan.NewOrderState)
			}
			req.ExpectedStateHash = plan.StateHash
			res, err := f.r.Apply(f.ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if res.NewOrderState != plan.NewOrderState {
				t.Errorf("apply order state %s != preview %s", res.NewOrderState, plan.NewOrderState)
			}
			if f.orderState(f.buyID) != "FILLED" {
				t.Errorf("order = %s, want FILLED (terminal correction recorded the discovered fill)", f.orderState(f.buyID))
			}
			if f.fillCount(f.buyID) != 1 {
				t.Errorf("fills = %d, want 1", f.fillCount(f.buyID))
			}
			// A FULL discovered buy fill advances the cycle to BUY_FILLED (not a dead end).
			if f.cycleState() != "BUY_FILLED" {
				t.Errorf("cycle = %s, want BUY_FILLED (full discovered buy fill)", f.cycleState())
			}
		})
	}
}

// BLOCKER 3: a discovered PARTIAL buy fill on a CANCELLED order records the fill, LEAVES the order
// terminal (never reactivated to an active PARTIALLY_FILLED), and advances the cycle to
// BUY_PARTIALLY_FILLED so sell management can proceed — without inserting the fill twice.
func TestCorrectTerminalPartialBuyKeepsOrderCancelledAdvancesCycle(t *testing.T) {
	f := setupR(t)
	f.seed("2", "0", false, "", "") // buy order qty 2
	f.setOrderState(f.buyID, "CANCELLED")
	res, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionCorrectTerminalOrderFill, OrderID: f.buyID, Fill: f.buyFill("bp1", "1", "10"), Reason: "discovered partial fill"})
	if err != nil {
		t.Fatal(err)
	}
	if f.orderState(f.buyID) != "CANCELLED" {
		t.Errorf("order = %s, want CANCELLED (a partial discovered fill must NOT reactivate a terminal order)", f.orderState(f.buyID))
	}
	var filledOK bool
	f.db.QueryRow("SELECT filled_quantity = 1 FROM orders WHERE id=?", f.buyID).Scan(&filledOK)
	if !filledOK {
		t.Error("filled_quantity must be 1")
	}
	if f.cycleState() != "BUY_PARTIALLY_FILLED" {
		t.Errorf("cycle = %s, want BUY_PARTIALLY_FILLED (cycle advances, not a dead end)", f.cycleState())
	}
	if f.lockState() != "ACTIVE" || res.LockReleased {
		t.Error("partial buy holds inventory — lock held")
	}
	if f.fillCount(f.buyID) != 1 {
		t.Errorf("fills = %d, want 1 (no duplicate insertion required)", f.fillCount(f.buyID))
	}
}

// A discovered SELL fill on a terminal sell that leaves remaining exposure queues the next sell.
func TestCorrectTerminalSellRemainingExposureQueuesNextSell(t *testing.T) {
	f := setupR(t)
	f.seed("2", "2", true, "1", "0") // bought 2; a sell order qty 1
	f.setOrderState(f.sellID, "CANCELLED")
	// Discovered fill of 1 on the cancelled sell. Remaining exposure = 2 - 1 = 1, no other active sell.
	if _, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionCorrectTerminalOrderFill, OrderID: f.sellID, Fill: f.sellFill("ts1", "1", "12"), Reason: "discovered sell fill"}); err != nil {
		t.Fatal(err)
	}
	if f.orderState(f.sellID) != "FILLED" {
		t.Errorf("sell order = %s, want FILLED (fill completed the order qty 1)", f.orderState(f.sellID))
	}
	if f.cycleState() != "SELL_REPRICE_PENDING" {
		t.Errorf("cycle = %s, want SELL_REPRICE_PENDING (remaining exposure → next-sell path)", f.cycleState())
	}
}

// A discovered SELL fill on a terminal sell that closes all exposure closes the cycle + releases lock.
func TestCorrectTerminalSellClosesExposure(t *testing.T) {
	f := setupR(t)
	f.seed("1", "1", true, "1", "0") // bought 1; a sell order qty 1
	f.setOrderState(f.sellID, "CANCELLED")
	res, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionCorrectTerminalOrderFill, OrderID: f.sellID, Fill: f.sellFill("ts2", "1", "12"), Reason: "discovered full exit"})
	if err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "CLOSED" || f.lockState() != "RELEASED" || !res.LockReleased {
		t.Errorf("closing sell fill: cyc:%s lock:%s", f.cycleState(), f.lockState())
	}
}

// ---- round 3: EVERY other sell order must be considered ----

// R3 B1: another sell order in the cycle is still executable (ACKED) with NO active queue request —
// recorded exposure hits zero but that other sell may fill later. mark_sell_filled must NOT close
// the cycle or release the lock.
func TestMarkSellFilledBlockedByAnotherExecutableSell(t *testing.T) {
	f := setupR(t)
	f.seed("1", "1", true, "1", "0") // bought 1; sell1 (selected) qty 1
	f.setOrderState(f.sellID, "SUBMITTED")
	f.seedSell("1", "0", "ACKED") // another sell, still executable, no queue request
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkSellFilled, OrderID: f.sellID, Fill: f.sellFill("sf", "1", "12"), Reason: "close"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (another executable sell blocks the close)", err)
	}
	if f.cycleState() != "NEEDS_RECONCILE" || f.lockState() != "ACTIVE" {
		t.Errorf("cycle=%s lock=%s, want NEEDS_RECONCILE/ACTIVE (not closed, lock held)", f.cycleState(), f.lockState())
	}
	if f.fillCount(f.sellID) != 0 {
		t.Error("no fill may be recorded when the close is blocked")
	}
}

// R3 B1: a sell order in NEEDS_RECONCILE must be treated as dangerous. The selected sell becomes
// FILLED with remaining exposure, but another sell is NEEDS_RECONCILE → the cycle must NOT move to
// SELL_REPRICE_PENDING (which would queue another exit) and the lock stays held.
func TestSellPartialBlockedByUnresolvedOtherSell(t *testing.T) {
	f := setupR(t)
	f.seed("2", "2", true, "1", "0") // bought 2; sell1 (selected) qty 1
	f.setOrderState(f.sellID, "SUBMITTED")
	f.seedSell("1", "0", "NEEDS_RECONCILE") // another sell, unresolved (may still execute)
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkSellPartiallyFilled, OrderID: f.sellID, Fill: f.sellFill("sp", "1", "12"), Reason: "partial"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (an unresolved sell blocks queuing a replacement)", err)
	}
	if f.cycleState() != "NEEDS_RECONCILE" || f.lockState() != "ACTIVE" {
		t.Errorf("cycle=%s lock=%s, want NEEDS_RECONCILE/ACTIVE", f.cycleState(), f.lockState())
	}
	if f.fillCount(f.sellID) != 0 {
		t.Error("no fill may be recorded when the operation is blocked")
	}
}

// R3 B2: correct_terminal_order_fill resolves the whole cycle, so an active request on ANOTHER order
// in the cycle must block it — for every active status, mutating AND read-only. No fill is inserted.
func TestCorrectTerminalBlockedByActiveRequestOnAnotherOrder(t *testing.T) {
	for _, tc := range []struct{ typ, status string }{
		{"PLACE_ORDER", "QUEUED"}, {"CANCEL_ORDER", "RETRY_SCHEDULED"},
		{"CANCEL_ORDER", "CLAIMED"}, {"GET_ORDER", "IN_FLIGHT"}, {"GET_ORDER", "QUEUED"},
	} {
		t.Run(tc.typ+"_"+tc.status, func(t *testing.T) {
			f := setupR(t)
			f.seed("2", "0", true, "1", "0")       // buy A (qty 2) + sell B (qty 1)
			f.setOrderState(f.buyID, "CANCELLED")  // the terminal order to correct
			f.seedReq(f.sellID, tc.typ, tc.status) // an active request on ANOTHER order (sell B)
			_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionCorrectTerminalOrderFill, OrderID: f.buyID, Fill: f.buyFill("d", "1", "10"), Reason: "discovered"})
			if !IsValidation(err) {
				t.Fatalf("%s/%s: err = %v, want ValidationError (active request on another order blocks whole-cycle resolution)", tc.typ, tc.status, err)
			}
			if f.cycleState() != "NEEDS_RECONCILE" || f.lockState() != "ACTIVE" {
				t.Errorf("cycle=%s lock=%s, want NEEDS_RECONCILE/ACTIVE", f.cycleState(), f.lockState())
			}
			if f.fillCount(f.buyID) != 0 {
				t.Error("no fill may be inserted when the whole-cycle request guard fails")
			}
		})
	}
}

// R3 B1: a corrected terminal SELL fill that would reduce exposure to zero, while another sell is
// still active, must NOT close the cycle or release the lock.
func TestCorrectTerminalSellBlockedByAnotherSell(t *testing.T) {
	for _, otherState := range []string{"ACKED", "NEEDS_RECONCILE"} {
		t.Run(otherState, func(t *testing.T) {
			f := setupR(t)
			f.seed("2", "2", true, "1", "1") // bought 2; sell1 already sold 1
			f.setOrderState(f.sellID, "FILLED")
			termSell := f.seedSell("1", "0", "CANCELLED") // the terminal sell to correct
			f.seedSell("1", "0", otherState)              // ANOTHER sell still active/unresolved
			_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionCorrectTerminalOrderFill, OrderID: termSell, Fill: f.sellFill("tc", "1", "12"), Reason: "discovered"})
			if !IsValidation(err) {
				t.Fatalf("other=%s: err = %v, want ValidationError (another sell may still execute)", otherState, err)
			}
			if f.cycleState() != "NEEDS_RECONCILE" || f.lockState() != "ACTIVE" {
				t.Errorf("cycle=%s lock=%s, want NEEDS_RECONCILE/ACTIVE", f.cycleState(), f.lockState())
			}
		})
	}
}

// ---- attach_exchange_order_id (blocker 8) ----

func TestAttachEmptyThenIdempotentThenConflict(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	// empty -> attach
	if _, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionAttachExchangeOrderID, OrderID: f.buyID, ExchangeOrderID: "EX-1", Reason: "found"}); err != nil {
		t.Fatal(err)
	}
	var oid string
	f.db.QueryRow("SELECT exchange_order_id FROM orders WHERE id=?", f.buyID).Scan(&oid)
	if oid != "EX-1" {
		t.Fatalf("exchange_order_id = %q, want EX-1", oid)
	}
	// same value -> idempotent success (no error)
	if _, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionAttachExchangeOrderID, OrderID: f.buyID, ExchangeOrderID: "EX-1", Reason: "again"}); err != nil {
		t.Fatalf("idempotent re-attach must succeed: %v", err)
	}
	// different value -> reject
	if _, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionAttachExchangeOrderID, OrderID: f.buyID, ExchangeOrderID: "EX-2", Reason: "overwrite"}); !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (conflicting overwrite)", err)
	}
}

// TestAttachRejectsWrongRoleOrder targets the targetOrder role gate directly (attach has no
// validateFill backstop): attaching a buy-order-id action to a sell order must be rejected.
func TestAttachRejectsWrongRoleOrder(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", true, "1", "0")
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionAttachExchangeOrderID, OrderID: f.sellID, ExchangeOrderID: "EX-9", Reason: "x"})
	if !IsValidation(err) {
		t.Fatalf("attach targeting a sell order must be rejected (role gate): %v", err)
	}
}

func TestAttachConflictsWithAnotherOrder(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", true, "1", "0")
	f.setExoid(f.sellID, "EX-DUP") // another order on the same exchange already has this id
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionAttachExchangeOrderID, OrderID: f.buyID, ExchangeOrderID: "EX-DUP", Reason: "x"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (id already on another order)", err)
	}
}

// BLOCKER 4 (deterministic): the DB-level UNIQUE (exchange_id, exchange_order_id) index (migration
// 035) rejects a duplicate venue id on two orders of the same exchange, regardless of the app path.
func TestExchangeOrderIdUniqueConstraint(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", true, "1", "0")
	f.setExoid(f.buyID, "EX-UNIQUE")
	if _, err := f.db.Exec("UPDATE orders SET exchange_order_id='EX-UNIQUE' WHERE id=?", f.sellID); err == nil {
		t.Fatal("the unique (exchange_id, exchange_order_id) index must reject a duplicate venue id on the same exchange")
	}
	// NULLs never conflict (unplaced orders): two NULLs on the same exchange are fine.
	if _, err := f.db.Exec("UPDATE orders SET exchange_order_id=NULL WHERE cycle_id=?", f.cycID); err != nil {
		t.Fatalf("multiple NULL exchange_order_id on one exchange must be allowed: %v", err)
	}
}

// BLOCKER 4 (concurrency): two different orders on the SAME exchange attaching the SAME
// exchange_order_id simultaneously — exactly one transaction may succeed (per-exchange FOR UPDATE
// serialization + the unique index).
func TestAttachConcurrentExactlyOneWins(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "") // exchange f.exID, cycle A, buy f.buyID
	cycB := f.lastID(f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, version) VALUES (?, ?, ?, 'NEEDS_RECONCILE', 1)", f.emID, f.exID, f.base+"/IRT"))
	buyB := f.lastID(f.exec("INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, version, quantity, filled_quantity) VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'NEEDS_RECONCILE', 1, '1', '0')",
		cycB, f.exID, f.emID, fmt.Sprintf("buyB_%d", time.Now().UnixNano())))
	const sameID = "EXT-RACE"
	reqA := Request{CycleID: f.cycID, Action: ActionAttachExchangeOrderID, OrderID: f.buyID, ExchangeOrderID: sameID, Reason: "raceA", Operator: "opA"}
	reqB := Request{CycleID: cycB, Action: ActionAttachExchangeOrderID, OrderID: buyB, ExchangeOrderID: sameID, Reason: "raceB", Operator: "opB"}
	pa, err := f.r.Preview(f.ctx, reqA)
	if err != nil {
		t.Fatal(err)
	}
	pb, err := f.r.Preview(f.ctx, reqB)
	if err != nil {
		t.Fatal(err)
	}
	reqA.ExpectedStateHash = pa.StateHash
	reqB.ExpectedStateHash = pb.StateHash

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); _, errs[0] = f.r.Apply(f.ctx, reqA) }()
	go func() { defer wg.Done(); _, errs[1] = f.r.Apply(f.ctx, reqB) }()
	wg.Wait()

	okCount := 0
	for _, e := range errs {
		if e == nil {
			okCount++
		}
	}
	if okCount != 1 {
		t.Fatalf("exactly one concurrent attach must succeed; got %d (errA=%v errB=%v)", okCount, errs[0], errs[1])
	}
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM orders WHERE exchange_id=? AND exchange_order_id=?", f.exID, sameID).Scan(&n)
	if n != 1 {
		t.Errorf("%d orders hold the exchange_order_id, want exactly 1", n)
	}
}

// ---- preview enforcement (blocker 6) ----

func TestApplyWithoutPreviewRejected(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	f.provenZero()
	// No ExpectedStateHash -> refused.
	_, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "close", Operator: "op1"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (preview required)", err)
	}
}

func TestApplyStaleFingerprintConflicts(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	f.provenZero()
	plan, err := f.r.Preview(f.ctx, Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "close", Operator: "op1"})
	if err != nil {
		t.Fatal(err)
	}
	// State drifts after the preview (cycle version bumps).
	f.exec("UPDATE cycles SET version=version+1 WHERE id=?", f.cycID)
	_, aerr := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "close", Operator: "op1", ExpectedStateHash: plan.StateHash})
	if !IsConflict(aerr) {
		t.Fatalf("err = %v, want ConflictError (state changed since preview)", aerr)
	}
	if f.cycleState() != "NEEDS_RECONCILE" {
		t.Error("a conflicting apply must not mutate state")
	}
}

func TestPreviewIncludesAllMutations(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", true, "1", "0")
	f.provenZero()
	f.seedReq(f.buyID, "GET_ORDER", "QUEUED") // an active request to surface
	plan, err := f.r.Preview(f.ctx, Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "close", Operator: "op1"})
	// cancel is blocked by the active request -> validation; but a keep preview shows all context.
	if err == nil {
		t.Fatal("expected the active request to block the cancel preview")
	}
	plan, err = f.r.Preview(f.ctx, Request{CycleID: f.cycID, Action: ActionKeepNeedsReconcile, Reason: "inspect", Operator: "op1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.RequestChanges) == 0 {
		t.Error("preview must list the active request(s)")
	}
	if plan.ExposureClass == "" || plan.ExecutionMode == "" || plan.LockChange == "" {
		t.Errorf("preview must include exposure/mode/lock fields, got %+v", plan)
	}
}

// ---- concurrency / rollback (blocker 9/10) ----

func TestConcurrentFillUpdateConflicts(t *testing.T) {
	f := setupR(t)
	f.seed("2", "0", false, "", "")
	plan, err := f.r.Preview(f.ctx, Request{CycleID: f.cycID, Action: ActionMarkBuyPartiallyFilled, Fill: f.buyFill("bp", "1", "10"), Reason: "x", Operator: "op1"})
	if err != nil {
		t.Fatal(err)
	}
	// A concurrent writer records a fill / bumps the order version between preview and apply.
	f.exec("UPDATE orders SET filled_quantity='0.5', version=version+1 WHERE id=?", f.buyID)
	_, aerr := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionMarkBuyPartiallyFilled, Fill: f.buyFill("bp", "1", "10"), Reason: "x", Operator: "op1", ExpectedStateHash: plan.StateHash})
	if !IsConflict(aerr) {
		t.Fatalf("err = %v, want ConflictError (concurrent order change)", aerr)
	}
}

func TestApplyCrashRollsBackAtomically(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	f.provenZero()
	f.r.faultBeforeCommit = func() error { return fmt.Errorf("injected: crash before reconcile commit") }
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "would close"})
	if err == nil {
		t.Fatal("apply must fail when the pre-commit fault fires")
	}
	if f.cycleState() != "NEEDS_RECONCILE" || f.lockState() != "ACTIVE" || f.auditCount() != 0 {
		t.Errorf("partial state after fault: cyc=%s lock=%s audits=%d (want NEEDS_RECONCILE/ACTIVE/0)", f.cycleState(), f.lockState(), f.auditCount())
	}
	f.r.faultBeforeCommit = nil
	if _, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "close now"}); err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "CANCELLED" || f.lockState() != "RELEASED" || f.auditCount() != 1 {
		t.Errorf("after clean apply: cyc=%s lock=%s audits=%d (want CANCELLED/RELEASED/1)", f.cycleState(), f.lockState(), f.auditCount())
	}
}

// ---- fill validation, keep, mark_failed, preview-no-mutate (retained) ----

func TestDuplicateFillRejected(t *testing.T) {
	f := setupR(t)
	f.seed("1", "1", true, "1", "0")
	f.exec("INSERT INTO fills (order_id, cycle_id, exchange_fill_id, quantity, price) VALUES (?, ?, 'dupe', '0.5', '12')", f.sellID, f.cycID)
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkSellPartiallyFilled, Fill: f.sellFill("dupe", "0.4", "12"), Reason: "x"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (duplicate fill)", err)
	}
}

func TestInvalidQuantityRejected(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	for _, bad := range []string{"0", "-1", "abc", ""} {
		_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkBuyFilled, Fill: f.buyFill("q", bad, "10"), Reason: "x"})
		if !IsValidation(err) {
			t.Errorf("qty %q: err = %v, want ValidationError", bad, err)
		}
	}
}

func TestOversellRejected(t *testing.T) {
	f := setupR(t)
	f.seed("2", "1", true, "2", "0") // bought only 1
	_, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkSellPartiallyFilled, Fill: f.sellFill("os", "2", "12"), Reason: "x"})
	if !IsValidation(err) {
		t.Fatalf("err = %v, want ValidationError (oversell)", err)
	}
}

func TestKeepNeedsReconcileLockNotReleased(t *testing.T) {
	f := setupR(t)
	f.seed("1", "1", false, "", "")
	res, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionKeepNeedsReconcile, Reason: "still investigating"})
	if err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "NEEDS_RECONCILE" || f.lockState() != "ACTIVE" || res.LockReleased {
		t.Error("keep must leave NEEDS_RECONCILE with the lock held")
	}
	if f.auditCount() != 1 {
		t.Error("keep must still write an audit row")
	}
}

func TestMarkFailedOpenExposureRefused(t *testing.T) {
	f := setupR(t)
	f.seed("1", "1", false, "", "")
	res, err := f.applyVia(Request{CycleID: f.cycID, Action: ActionMarkFailed, Reason: "unrecoverable"})
	if err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "NEEDS_RECONCILE" || f.lockState() != "ACTIVE" || res.LockReleased {
		t.Error("mark_failed with open exposure must stay NEEDS_RECONCILE + hold the lock")
	}
	if !res.Downgraded || !res.ExposureUnresolved {
		t.Errorf("plan should record Downgraded + ExposureUnresolved: %+v", res.Plan)
	}
}

func TestMarkFailedForcedWithExternalConfirmation(t *testing.T) {
	f := setupR(t)
	f.seed("1", "1", false, "", "")
	res, err := f.applyVia(Request{
		CycleID: f.cycID, Action: ActionMarkFailed, Reason: "give up",
		ExternalResolutionConfirmed: true, ExternalResolutionReason: "sold manually on the venue UI",
	})
	if err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "FAILED" || f.lockState() != "RELEASED" || !res.LockReleased {
		t.Errorf("forced FAILED should close + release; cyc:%s lock:%s", f.cycleState(), f.lockState())
	}
	var confirmed int
	var reason string
	f.db.QueryRow("SELECT external_resolution_confirmed, reason FROM reconcile_resolutions WHERE cycle_id=? ORDER BY id DESC LIMIT 1", f.cycID).Scan(&confirmed, &reason)
	if confirmed != 1 || !strings.Contains(reason, "external_resolution_confirmed") {
		t.Errorf("audit must record the external confirmation (confirmed=%d reason=%q)", confirmed, reason)
	}
}

func TestPreviewDoesNotMutate(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	f.provenZero()
	plan, err := f.r.Preview(f.ctx, Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "x", Operator: "op1"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.NewCycleState != "CANCELLED" || !plan.LockReleased || plan.StateHash == "" {
		t.Errorf("preview plan = %+v, want CANCELLED + lock released + a state hash", plan)
	}
	if f.cycleState() != "NEEDS_RECONCILE" || f.lockState() != "ACTIVE" || f.auditCount() != 0 {
		t.Error("preview must not mutate state, lock, or write an audit row")
	}
}

func TestApplyRequiresReason(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "")
	f.provenZero()
	if _, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Operator: "op1", ExpectedStateHash: "x"}); !IsValidation(err) {
		t.Errorf("missing reason err = %v, want ValidationError", err)
	}
}
