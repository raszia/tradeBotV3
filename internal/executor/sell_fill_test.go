package executor

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/sellflow"
)

// placeAndPollSell drives a resting sell through place → (manager-style) status poll.
func (it *intg) placeAndPollSell(t *testing.T, cyc, orderID int64, ack execution.OrderAck, status execution.OrderStatus) {
	t.Helper()
	it.fake.placeAck = ack
	it.fake.getStatus = status
	it.drive(3) // send the resting sell
	payload, _ := json.Marshal(orders.FollowupPayload{ExchangeOrderID: ack.ExchangeOrderID, Purpose: orders.PurposeSellStatus, CycleID: cyc})
	if _, err := it.db.Exec(`INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, request_type, status, payload, idempotency_key)
		VALUES (?, ?, ?, 'GET_ORDER', 'QUEUED', ?, ?)`, it.exID, cyc, orderID, payload, fmt.Sprintf("poll_%d", cyc)); err != nil {
		t.Fatal(err)
	}
	it.drive(3)
}

func (it *intg) fillCount(orderID int64) int {
	var n int
	it.db.QueryRow("SELECT COUNT(*) FROM fills WHERE order_id=?", orderID).Scan(&n)
	return n
}

// TestSellFullFillDerivesAvgFromExecutedQuote (PR11 #5) — a full sell fill with no AvgPrice but
// a positive ExecutedQuote derives avg = quote/qty, records the fill, and closes with real PnL.
func TestSellFullFillDerivesAvgFromExecutedQuote(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, mc := it.seedSellCycle(t, "0.5", "50") // buy: 0.5 @ quote 50
	r, err := sellflow.CreateSell(it.ctx, it.store, it.q, mc, sellflow.CreateParams{CycleID: cyc, BinanceRef: decimal.RequireFromString("100"), QuoteUnit: "IRT"})
	if err != nil {
		t.Fatal(err)
	}
	it.placeAndPollSell(t, cyc, r.OrderID, execution.OrderAck{ExchangeOrderID: "EX-S"},
		execution.OrderStatus{Status: execution.StateFilled, ExchangeOrderID: "EX-S",
			FilledQty: decimal.RequireFromString("0.5"), RemainingQty: decimal.RequireFromString("0"),
			AvgPrice: decimal.RequireFromString("0"), ExecutedQuote: decimal.RequireFromString("55")}) // 55/0.5 = 110

	if it.cycleState(cyc) != "CLOSED" {
		t.Fatalf("cycle=%s, want CLOSED", it.cycleState(cyc))
	}
	var price, realized string
	it.db.QueryRow("SELECT price FROM fills WHERE order_id=?", r.OrderID).Scan(&price)
	it.db.QueryRow("SELECT COALESCE(realized_quote,'') FROM cycles WHERE id=?", cyc).Scan(&realized)
	if !decimal.RequireFromString(price).Equal(decimal.RequireFromString("110")) {
		t.Errorf("derived sell fill price=%s, want 110 (55/0.5)", price)
	}
	if !decimal.RequireFromString(realized).Equal(decimal.RequireFromString("5")) {
		t.Errorf("realized_quote=%s, want 5 (55-50)", realized)
	}
}

// TestSellFullFillNoCostBasisIsAmbiguous (PR11 #5) — a full sell fill with neither AvgPrice nor
// ExecutedQuote has no cost basis: ambiguous → NEEDS_RECONCILE, lock held, no fill, not closed.
func TestSellFullFillNoCostBasisIsAmbiguous(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, mc := it.seedSellCycle(t, "0.5", "50")
	r, err := sellflow.CreateSell(it.ctx, it.store, it.q, mc, sellflow.CreateParams{CycleID: cyc, BinanceRef: decimal.RequireFromString("100"), QuoteUnit: "IRT"})
	if err != nil {
		t.Fatal(err)
	}
	it.placeAndPollSell(t, cyc, r.OrderID, execution.OrderAck{ExchangeOrderID: "EX-S"},
		execution.OrderStatus{Status: execution.StateFilled, ExchangeOrderID: "EX-S",
			FilledQty: decimal.RequireFromString("0.5"), RemainingQty: decimal.RequireFromString("0"),
			AvgPrice: decimal.RequireFromString("0"), ExecutedQuote: decimal.RequireFromString("0")})

	if it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Fatalf("cycle=%s, want NEEDS_RECONCILE (no cost basis)", it.cycleState(cyc))
	}
	if it.orderState(r.OrderID) != "NEEDS_RECONCILE" {
		t.Errorf("sell order=%s, want NEEDS_RECONCILE", it.orderState(r.OrderID))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Error("ambiguous sell fill must keep the lock ACTIVE")
	}
	if n := it.fillCount(r.OrderID); n != 0 {
		t.Errorf("fills=%d, want 0 (no fill recorded without a cost basis)", n)
	}
}

// TestSellCloseRefusedOnMissingBuyAccounting (PR11 #6) — a valid sell fill whose cycle has
// missing/zero BUY cost basis must NOT close with bad PnL: divert to NEEDS_RECONCILE, lock held.
func TestSellCloseRefusedOnMissingBuyAccounting(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, mc := it.seedSellCycle(t, "0.5", "0") // buy quote_spent = 0 (no valid cost basis)
	r, err := sellflow.CreateSell(it.ctx, it.store, it.q, mc, sellflow.CreateParams{CycleID: cyc, BinanceRef: decimal.RequireFromString("100"), QuoteUnit: "IRT"})
	if err != nil {
		t.Fatal(err)
	}
	it.placeAndPollSell(t, cyc, r.OrderID, execution.OrderAck{ExchangeOrderID: "EX-S"},
		execution.OrderStatus{Status: execution.StateFilled, ExchangeOrderID: "EX-S",
			FilledQty: decimal.RequireFromString("0.5"), RemainingQty: decimal.RequireFromString("0"),
			AvgPrice: decimal.RequireFromString("110"), ExecutedQuote: decimal.RequireFromString("55")})

	if it.cycleState(cyc) == "CLOSED" {
		t.Fatal("cycle must NOT close with missing/zero buy accounting")
	}
	if it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Errorf("cycle=%s, want NEEDS_RECONCILE", it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Error("lock must be held when a close is refused for bad accounting")
	}
	var closedAt sql.NullTime // the cycle must not be stamped closed
	it.db.QueryRow("SELECT closed_at FROM cycles WHERE id=?", cyc).Scan(&closedAt)
	if closedAt.Valid {
		t.Error("closed_at must be NULL when close is refused for bad accounting")
	}
}
