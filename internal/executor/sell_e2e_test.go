package executor

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/sellflow"
)

// End-to-end exit sell through the REAL executor loop: sellflow.CreateSell enqueues
// the sell PLACE; the executor sends it (resting), then a status poll (what the
// manager enqueues) drives the full fill → cycle CLOSED + lock released. Fake client.

func (it *intg) seedSellCycle(t *testing.T, buyFilled, buyQuote string) (int64, configstore.MarketConfig) {
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
	canonical := u("M") + "/IRT"
	b := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B")))
	qa := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q")))
	m := last(ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", canonical, b, qa))
	em := last(ex("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol, enabled_for_sell_manage, tick_size, step_size) VALUES (?, ?, ?, ?, 1, '0.01', '0.0001')",
		it.exID, m, u("ES"), canonical))
	cyc := last(ex("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state) VALUES (?, ?, ?, 'BUY_FILLED')", em, it.exID, canonical))
	ex(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, order_type, quantity, filled_quantity, avg_fill_price, quote_spent, fee_amount, fee_asset)
		VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'FILLED', 'limit', ?, ?, '100', ?, '0', 'IRT')`,
		cyc, it.exID, em, u("buy"), buyFilled, buyFilled, buyQuote)
	ex("INSERT INTO symbol_locks (scope, canonical_symbol, cycle_id, expires_at) VALUES (?, ?, ?, NOW(6)+INTERVAL 1 HOUR)", it.code, canonical, cyc)
	mc := configstore.MarketConfig{
		ExchangeMarketID: em, ExchangeID: it.exID, ExchangeCode: it.code, CanonicalSymbol: canonical,
		EnabledForSellManage: true, SellOffsetBps: 30, OrderTimeoutMs: 3000, MaxRetries: 3,
		TickSize: decimal.RequireFromString("0.01"), StepSize: decimal.RequireFromString("0.0001"),
	}
	return cyc, mc
}

func TestSellEndToEndFullExitCloses(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, mc := it.seedSellCycle(t, "0.5", "50")

	// Create the resting sell (0.5 @ ~99.7).
	r, err := sellflow.CreateSell(it.ctx, it.store, it.q, mc, sellflow.CreateParams{CycleID: cyc, BinanceRef: decimal.RequireFromString("100"), QuoteUnit: "IRT"})
	if err != nil {
		t.Fatalf("CreateSell: %v", err)
	}
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EX-S"}
	it.fake.getStatus = execution.OrderStatus{Status: execution.StateFilled, ExchangeOrderID: "EX-S",
		FilledQty: decimal.RequireFromString("0.5"), RemainingQty: decimal.RequireFromString("0"),
		AvgPrice: decimal.RequireFromString("110"), ExecutedQuote: decimal.RequireFromString("55")}

	// Executor sends the resting sell -> cycle SELL_SUBMITTED.
	it.drive(3)
	if it.cycleState(cyc) != "SELL_SUBMITTED" {
		t.Fatalf("after place: cycle = %s, want SELL_SUBMITTED", it.cycleState(cyc))
	}

	// The manager enqueues a status poll; here we enqueue it directly, then the
	// executor processes the full fill -> close.
	payload, _ := json.Marshal(orders.FollowupPayload{ExchangeOrderID: "EX-S", Purpose: orders.PurposeSellStatus, CycleID: cyc})
	it.db.Exec(`INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, request_type, status, payload, idempotency_key)
		VALUES (?, ?, ?, 'GET_ORDER', 'QUEUED', ?, ?)`, it.exID, cyc, r.OrderID, payload, fmt.Sprintf("poll_%d", cyc))
	it.drive(3)

	if it.cycleState(cyc) != "CLOSED" {
		t.Fatalf("cycle = %s, want CLOSED", it.cycleState(cyc))
	}
	if it.orderState(r.OrderID) != "FILLED" {
		t.Errorf("sell order = %s, want FILLED", it.orderState(r.OrderID))
	}
	if it.lockStateByCycle(cyc) != "RELEASED" {
		t.Errorf("lock = %s, want RELEASED after full exit", it.lockStateByCycle(cyc))
	}
	var realized string
	it.db.QueryRow("SELECT COALESCE(realized_quote,'') FROM cycles WHERE id=?", cyc).Scan(&realized)
	if !decimal.RequireFromString(realized).Equal(decimal.RequireFromString("5")) {
		t.Errorf("realized_quote = %s, want 5", realized)
	}
}
