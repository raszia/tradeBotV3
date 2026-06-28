package executor

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/sellflow"
	"v3TradeBot/internal/simexec"
)

// Full dry-run lifecycle through the REAL queue/executor/order-processing/sellflow
// boundaries with a SIMULATED client (simexec) — no real exchange call ever. Gated on
// V3_TEST_MYSQL_DSN (via setup()).

// simExec rebuilds the executor with a simulated client for the given scenario.
func (it *intg) simExec(t *testing.T, sc simexec.Scenario) {
	t.Helper()
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: simexec.New(it.code, sc)}, nil,
		Config{Name: "dry-run", AllowLiveExecution: true, FinalStatusDelay: 10 * time.Millisecond})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
}

func (it *intg) market(cyc int64) configstore.MarketConfig {
	var em int64
	var sym string
	it.db.QueryRow("SELECT exchange_market_id, canonical_symbol FROM cycles WHERE id=?", cyc).Scan(&em, &sym)
	return configstore.MarketConfig{ExchangeID: it.exID, ExchangeMarketID: em, CanonicalSymbol: sym, SellOffsetBps: 0, OrderTimeoutMs: 3000, MaxRetries: 3}
}

// enqueueSellStatus mimics the sell Manager's poll: enqueue a GET_ORDER (sell_status)
// for the cycle's resting sell so the executor processes its fills.
func (it *intg) enqueueSellStatus(t *testing.T, cyc int64) {
	t.Helper()
	var ordID int64
	var exoid string
	it.db.QueryRow("SELECT id, COALESCE(exchange_order_id,'') FROM orders WHERE cycle_id=? AND role='exit_sell' ORDER BY id DESC LIMIT 1", cyc).Scan(&ordID, &exoid)
	payload, _ := json.Marshal(orders.FollowupPayload{ExchangeOrderID: exoid, Purpose: orders.PurposeSellStatus, CycleID: cyc})
	_, err := it.db.Exec(`INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, request_type, status, payload, idempotency_key)
		VALUES (?, ?, ?, 'GET_ORDER', 'QUEUED', ?, ?)`, it.exID, cyc, ordID, payload, "sellpoll_"+exoid)
	if err != nil {
		t.Fatal(err)
	}
}

func (it *intg) dryCycle(t *testing.T, qty string) int64 {
	t.Helper()
	cyc, _, _ := it.seedBuyCycle(t, qty)
	it.db.Exec("UPDATE cycles SET dry_run=1 WHERE id=?", cyc)
	return cyc
}

func TestDryRunFullLifecycleBuySellClose(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.FullFill)
	cyc := it.dryCycle(t, "0.5")

	it.drive(6) // PLACE -> (scheduled) CANCEL -> GET_ORDER -> full fill
	if it.cycleState(cyc) != "BUY_FILLED" {
		t.Fatalf("buy did not fill: cycle=%s", it.cycleState(cyc))
	}
	var dry int
	it.db.QueryRow("SELECT dry_run FROM cycles WHERE id=?", cyc).Scan(&dry)
	if dry != 1 {
		t.Error("cycle should remain marked dry_run")
	}

	// Create the exit sell, send it, then poll its status to completion.
	if _, err := sellflow.CreateSell(it.ctx, it.store, it.q, it.market(cyc), sellflow.CreateParams{CycleID: cyc, BinanceRef: decimal.RequireFromString("100"), QuoteUnit: "IRT"}); err != nil {
		t.Fatalf("create sell: %v", err)
	}
	it.drive(3) // sell PLACE -> SELL_SUBMITTED (resting; no auto-cancel)
	if it.cycleState(cyc) != "SELL_SUBMITTED" {
		t.Fatalf("sell not submitted: cycle=%s", it.cycleState(cyc))
	}
	it.enqueueSellStatus(t, cyc)
	it.drive(3) // sell status -> full fill -> SELL_FILLED -> CLOSED

	if it.cycleState(cyc) != "CLOSED" {
		t.Fatalf("cycle did not close: %s", it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "RELEASED" {
		t.Errorf("lock should be released on close, got %s", it.lockStateByCycle(cyc))
	}
}

func TestDryRunZeroFillClosesClean(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.ZeroFill)
	cyc := it.dryCycle(t, "0.5")
	it.drive(6)
	if it.cycleState(cyc) != "CANCELLED" {
		t.Fatalf("zero-fill cycle = %s, want CANCELLED", it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "RELEASED" {
		t.Errorf("lock should be released on zero-fill, got %s", it.lockStateByCycle(cyc))
	}
}

func TestDryRunPartialBuySellsFilledQtyOnly(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.PartialFill)
	cyc := it.dryCycle(t, "0.5")
	it.drive(6)
	if it.cycleState(cyc) != "BUY_PARTIALLY_FILLED" {
		t.Fatalf("partial buy cycle = %s, want BUY_PARTIALLY_FILLED", it.cycleState(cyc))
	}
	// The exit sell uses only the FILLED quantity (0.25), not the requested 0.5.
	if _, err := sellflow.CreateSell(it.ctx, it.store, it.q, it.market(cyc), sellflow.CreateParams{CycleID: cyc, BinanceRef: decimal.RequireFromString("100"), QuoteUnit: "IRT"}); err != nil {
		t.Fatalf("create sell: %v", err)
	}
	var sellQty string
	it.db.QueryRow("SELECT quantity FROM orders WHERE cycle_id=? AND role='exit_sell'", cyc).Scan(&sellQty)
	if !decimal.RequireFromString(sellQty).Equal(decimal.RequireFromString("0.25")) {
		t.Errorf("sell quantity = %s, want 0.25 (filled qty only, not requested 0.5)", sellQty)
	}
}

func TestDryRunAmbiguousNeedsReconcile(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.Ambiguous)
	cyc := it.dryCycle(t, "0.5")
	it.drive(6)
	if it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Fatalf("ambiguous cycle = %s, want NEEDS_RECONCILE", it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("ambiguous must keep the lock, got %s", it.lockStateByCycle(cyc))
	}
}
