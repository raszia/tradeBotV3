package executor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/orders"
)

// End-to-end simulated-IOC flow through the REAL executor claim/process loop with a
// fake client: PLACE → (scheduled) CANCEL → (scheduled) GET_ORDER → final fills.
// No exchange is contacted. Gated on V3_TEST_MYSQL_DSN (via setup()).

// iocExec rebuilds the executor with an immediate final-status delay so the queued
// waits resolve fast in tests.
func (it *intg) iocExec(t *testing.T) {
	t.Helper()
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "ioc", AllowLiveExecution: true, FinalStatusDelay: 10 * time.Millisecond, Recovery: fastRecovery()})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
}

// seedBuyCycle inserts a cycle(BUY_REQUEST_QUEUED) + entry_buy order(QUEUED) + ACTIVE
// lock + QUEUED PLACE_ORDER request carrying a BuyIntentPayload (maker wait 0 so the
// cancel is immediately claimable). Returns cycle, order, placeRequest ids.
func (it *intg) seedBuyCycle(t *testing.T, qty string) (int64, int64, int64) {
	t.Helper()
	seedSeq++
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, it.exID, seedSeq) }
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	ex := func(q string, a ...any) sql.Result {
		r, err := it.db.Exec(q, a...)
		if err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
		return r
	}
	b := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B")))
	qa := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q")))
	m := last(ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", u("M")+"/IRT", b, qa))
	em := last(ex("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, ?)", it.exID, m, u("ES"), u("M")+"/IRT"))
	cyc := last(ex("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state) VALUES (?, ?, ?, 'BUY_REQUEST_QUEUED')", em, it.exID, u("M")+"/IRT"))
	ord := last(ex(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, order_type, limit_price, quantity)
		VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'QUEUED', 'limit', '100', ?)`, cyc, it.exID, em, u("loc"), qty))
	ex("INSERT INTO symbol_locks (scope, canonical_symbol, cycle_id, expires_at) VALUES (?, ?, ?, NOW(6)+INTERVAL 1 HOUR)", it.code, u("M")+"/IRT", cyc)
	intent := orders.BuyIntentPayload{Side: "buy", OrderType: "limit", SimulatedIOC: true,
		IntendedPrice: "100", IntendedQuantity: qty, LocalClientOrderID: u("loc"), MakerWaitBeforeCancelMs: 0}
	payload, _ := json.Marshal(intent)
	// `symbol` mirrors what buyflow stamps in production; the live guard proves the request
	// symbol against the registered market (PR20 correction #2), so it must be populated.
	// `symbol` mirrors what buyflow stamps in production; the live guard proves the request
	// symbol against the registered market (PR20 correction #2) and recovery compares it to
	// the venue-reported symbol, so it must be populated exactly as production does.
	req := last(ex(`INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, symbol, request_type, priority, status, payload, timeout_ms, max_retries, idempotency_key)
		VALUES (?, ?, ?, ?, 'PLACE_ORDER', 50, 'QUEUED', ?, 10000, 5, ?)`, it.exID, cyc, ord, u("M")+"/IRT", payload, u("idem")))
	return cyc, ord, req
}

func (it *intg) drive(n int) {
	for i := 0; i < n; i++ {
		it.exec.claimAndProcessAll(it.ctx)
		time.Sleep(15 * time.Millisecond)
	}
}

func (it *intg) cycleState(id int64) string {
	var s string
	it.db.QueryRow("SELECT state FROM cycles WHERE id=?", id).Scan(&s)
	return s
}
func (it *intg) orderState(id int64) string {
	var s string
	it.db.QueryRow("SELECT state FROM orders WHERE id=?", id).Scan(&s)
	return s
}
func (it *intg) lockStateByCycle(cycleID int64) string {
	var s string
	it.db.QueryRow("SELECT state FROM symbol_locks WHERE cycle_id=? ORDER BY id DESC LIMIT 1", cycleID).Scan(&s)
	return s
}

func TestSimulatedIOCFullFill(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, _ := it.seedBuyCycle(t, "0.5")
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXE-1", ClientOrderID: "c-buy"}
	it.fake.getStatus = execution.OrderStatus{Status: execution.StateFilled, ExchangeOrderID: "EXE-1",
		IntendedQty: decimal.RequireFromString("0.5"), FilledQty: decimal.RequireFromString("0.5"),
		RemainingQty: decimal.RequireFromString("0"), AvgPrice: decimal.RequireFromString("100"), Fee: decimal.RequireFromString("0.0005"), FeeAsset: "USDT", Liquidity: "maker"}
	it.drive(6)

	if it.orderState(ord) != "FILLED" || it.cycleState(cyc) != "BUY_FILLED" {
		t.Fatalf("states = ord:%s cyc:%s, want FILLED/BUY_FILLED", it.orderState(ord), it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Error("lock must stay ACTIVE for a filled buy (sell pending)")
	}
	var fills int
	it.db.QueryRow("SELECT COUNT(*) FROM fills WHERE order_id=?", ord).Scan(&fills)
	if fills != 1 {
		t.Errorf("fills = %d, want 1", fills)
	}
	var mode string
	it.db.QueryRow("SELECT COALESCE(actual_execution_mode,'') FROM orders WHERE id=?", ord).Scan(&mode)
	if mode != "MAKER" {
		t.Errorf("actual_execution_mode = %q, want MAKER", mode)
	}
}

func TestSimulatedIOCZeroFillReleasesLock(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, _ := it.seedBuyCycle(t, "0.5")
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXE-2"}
	it.fake.getStatus = execution.OrderStatus{Status: execution.StateCanceled, ExchangeOrderID: "EXE-2",
		IntendedQty: decimal.RequireFromString("0.5"), FilledQty: decimal.RequireFromString("0"), RemainingQty: decimal.RequireFromString("0.5")}
	it.drive(6)

	if it.orderState(ord) != "CANCELLED" || it.cycleState(cyc) != "CANCELLED" {
		t.Fatalf("states = ord:%s cyc:%s, want CANCELLED/CANCELLED", it.orderState(ord), it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "RELEASED" {
		t.Errorf("lock = %s, want RELEASED on zero-fill", it.lockStateByCycle(cyc))
	}
}

// TestSimulatedIOCCancelAmbiguousNeedsReconcile (PR19 round 2 #3): an ambiguous cancel is
// never re-sent blindly — a READ-ONLY recovery probe determines the real state. Here the fake
// reports an indeterminate (unknown) status for the probe AND the final status, so the flow
// resolves conservatively to NEEDS_RECONCILE with the lock HELD (never guesses a fill).
func TestSimulatedIOCCancelAmbiguousNeedsReconcile(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, _ := it.seedBuyCycle(t, "0.5")
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXE-3"}
	it.fake.cancelErr = context.DeadlineExceeded // ambiguous: we cannot tell if the cancel took
	it.drive(8)

	if it.orderState(ord) != "NEEDS_RECONCILE" || it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Fatalf("states = ord:%s cyc:%s, want NEEDS_RECONCILE both", it.orderState(ord), it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (ambiguous, keep held)", it.lockStateByCycle(cyc))
	}
}

func TestSimulatedIOCMissingFinalStatusNotZero(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, _ := it.seedBuyCycle(t, "0.5")
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXE-4"}
	it.fake.getErr = execution.ErrOrderUnknown // order missing -> NOT proof of zero fill
	it.drive(6)

	if it.orderState(ord) != "NEEDS_RECONCILE" || it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Fatalf("states = ord:%s cyc:%s, want NEEDS_RECONCILE both (missing != zero fill)", it.orderState(ord), it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE", it.lockStateByCycle(cyc))
	}
}
