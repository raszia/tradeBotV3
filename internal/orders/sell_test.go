package orders

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/queue"
	"v3TradeBot/internal/state"
)

// seedSell builds a cycle + FILLED entry_buy order (inventory) + an exit_sell order
// in the given states, plus an ACTIVE lock. Returns (cycle, buyOrder, sellOrder).
func (f *ofix) seedSell(cycleState state.CycleState, sellState state.OrderState, sellExoid, buyFilled, buyQuote string) (cyc, buyOrd, sellOrd int64) {
	f.t.Helper()
	cyc, buyOrd = f.seed(cycleState, state.OrderFilled, "", buyFilled)
	if _, err := f.db.Exec("UPDATE orders SET filled_quantity=?, quote_spent=?, fee_amount='0', fee_asset='IRT' WHERE id=?", buyFilled, buyQuote, buyOrd); err != nil {
		f.t.Fatal(err)
	}
	var em int64
	f.db.QueryRow("SELECT exchange_market_id FROM orders WHERE id=?", buyOrd).Scan(&em)
	var exoid any
	if sellExoid != "" {
		exoid = sellExoid
	}
	oseq++
	res, err := f.db.Exec(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, exchange_order_id, state, order_type, limit_price, quantity)
		VALUES (?, ?, ?, 'sell', 'exit_sell', ?, ?, ?, 'limit', '99', ?)`,
		cyc, f.exID, em, fmt.Sprintf("c%d-sell-1_%d", cyc, oseq), exoid, string(sellState), buyFilled)
	if err != nil {
		f.t.Fatal(err)
	}
	sellOrd, _ = res.LastInsertId()
	return
}

func sellStatus(status execution.NormalizedOrderState, filled, remaining, avg, quote string) execution.OrderStatus {
	return execution.OrderStatus{
		Status: status, ExchangeOrderID: "EX-S", FilledQty: d(filled), RemainingQty: d(remaining),
		AvgPrice: d(avg), ExecutedQuote: d(quote),
	}
}

func TestOnSellPlaceAckRestsNoCancel(t *testing.T) {
	f := setupO(t)
	cyc, _, sellOrd := f.seedSell(state.CycleSellRequestQueued, state.OrderQueued, "", "0.5", "50")
	req := f.seedReq(cyc, sellOrd, queue.TypePlaceOrder, "IN_FLIGHT")
	f.tx(func(tx *sql.Tx) error {
		return OnSellPlaceAck(f.ctx, tx, f.q, SellPlaceAckParams{RequestID: req, OrderID: sellOrd, CycleID: cyc,
			Ack: execution.OrderAck{ExchangeOrderID: "EX-S"}, RawResp: json.RawMessage(`{}`)})
	})
	if f.orderState(sellOrd) != "ACKED" || f.cycleState(cyc) != "SELL_SUBMITTED" {
		t.Errorf("states = ord:%s cyc:%s, want ACKED/SELL_SUBMITTED", f.orderState(sellOrd), f.cycleState(cyc))
	}
	// A resting sell schedules NO cancel (unlike the buy IOC).
	var cancels int
	f.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE order_id=? AND request_type='CANCEL_ORDER'", sellOrd).Scan(&cancels)
	if cancels != 0 {
		t.Errorf("scheduled cancels = %d, want 0 (sell rests)", cancels)
	}
}

func (f *ofix) runSellStatus(cyc, sellOrd int64, st execution.OrderStatus, statusErr error) SellOutcome {
	f.t.Helper()
	req := f.seedReq(cyc, sellOrd, queue.TypeGetOrder, "IN_FLIGHT")
	var out SellOutcome
	f.tx(func(tx *sql.Tx) error {
		var err error
		out, err = ProcessSellStatus(f.ctx, tx, f.q, SellStatusParams{RequestID: req, OrderID: sellOrd, CycleID: cyc, Scope: f.exCode, Status: st, StatusErr: statusErr, RawResp: json.RawMessage(`{}`)})
		return err
	})
	return out
}

func TestProcessSellStatusPartialKeepsManaging(t *testing.T) {
	f := setupO(t)
	cyc, _, sellOrd := f.seedSell(state.CycleSellSubmitted, state.OrderAcked, "EX-S", "0.5", "50")
	lock := f.seedLock(cyc)
	out := f.runSellStatus(cyc, sellOrd, sellStatus(execution.StateOpen, "0.2", "0.3", "110", "22"), nil)
	if out.Closed || out.Ambiguous {
		t.Fatalf("out = %+v, want partial (not closed/ambiguous)", out)
	}
	if f.orderState(sellOrd) != "PARTIALLY_FILLED" || f.cycleState(cyc) != "SELL_PARTIALLY_FILLED" {
		t.Errorf("states = ord:%s cyc:%s, want PARTIALLY_FILLED/SELL_PARTIALLY_FILLED", f.orderState(sellOrd), f.cycleState(cyc))
	}
	if f.lockState(lock) != "ACTIVE" {
		t.Error("lock must stay ACTIVE during a partial sell")
	}
	var filled string
	f.db.QueryRow("SELECT filled_quantity FROM orders WHERE id=?", sellOrd).Scan(&filled)
	if !decimal.RequireFromString(filled).Equal(d("0.2")) {
		t.Errorf("sell filled = %s, want 0.2", filled)
	}
}

func TestProcessSellStatusFullClosesWithPnL(t *testing.T) {
	f := setupO(t)
	cyc, _, sellOrd := f.seedSell(state.CycleSellSubmitted, state.OrderAcked, "EX-S", "0.5", "50")
	lock := f.seedLock(cyc)
	// Sell 0.5 @ 110 => proceeds 55; buy cost 50 => realized 5.
	out := f.runSellStatus(cyc, sellOrd, sellStatus(execution.StateFilled, "0.5", "0", "110", "55"), nil)
	if !out.Closed || !out.LockReleased {
		t.Fatalf("out = %+v, want closed + lock released", out)
	}
	if f.orderState(sellOrd) != "FILLED" || f.cycleState(cyc) != "CLOSED" {
		t.Errorf("states = ord:%s cyc:%s, want FILLED/CLOSED", f.orderState(sellOrd), f.cycleState(cyc))
	}
	if f.lockState(lock) != "RELEASED" {
		t.Errorf("lock = %s, want RELEASED after full exit", f.lockState(lock))
	}
	var avgSell, realized, closeReason, soldQty string
	f.db.QueryRow("SELECT COALESCE(avg_sell_price,''), COALESCE(realized_quote,''), COALESCE(close_reason,''), COALESCE(sold_quantity,'') FROM cycles WHERE id=?", cyc).
		Scan(&avgSell, &realized, &closeReason, &soldQty)
	if !decimal.RequireFromString(avgSell).Equal(d("110")) || !decimal.RequireFromString(realized).Equal(d("5")) {
		t.Errorf("avg_sell/realized = %s/%s, want 110/5", avgSell, realized)
	}
	if closeReason != CloseReasonExited || !decimal.RequireFromString(soldQty).Equal(d("0.5")) {
		t.Errorf("close_reason/sold = %s/%s", closeReason, soldQty)
	}
}

func TestProcessSellStatusMissingIsAmbiguous(t *testing.T) {
	f := setupO(t)
	cyc, _, sellOrd := f.seedSell(state.CycleSellSubmitted, state.OrderAcked, "EX-S", "0.5", "50")
	lock := f.seedLock(cyc)
	out := f.runSellStatus(cyc, sellOrd, execution.OrderStatus{Status: execution.StateUnknown}, execution.ErrOrderUnknown)
	if !out.Ambiguous || out.Closed {
		t.Fatalf("out = %+v, want ambiguous", out)
	}
	if f.orderState(sellOrd) != "NEEDS_RECONCILE" || f.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Errorf("states = ord:%s cyc:%s, want NEEDS_RECONCILE both", f.orderState(sellOrd), f.cycleState(cyc))
	}
	if f.lockState(lock) != "ACTIVE" {
		t.Error("lock must stay ACTIVE for an ambiguous sell")
	}
}

func TestProcessSellStatusIdempotent(t *testing.T) {
	f := setupO(t)
	cyc, _, sellOrd := f.seedSell(state.CycleSellSubmitted, state.OrderAcked, "EX-S", "0.5", "50")
	f.seedLock(cyc)
	st := sellStatus(execution.StateFilled, "0.5", "0", "110", "55")
	f.runSellStatus(cyc, sellOrd, st, nil)
	f.runSellStatus(cyc, sellOrd, st, nil) // repeat
	var fills, events int
	f.db.QueryRow("SELECT COUNT(*) FROM fills WHERE order_id=?", sellOrd).Scan(&fills)
	f.db.QueryRow("SELECT COUNT(*) FROM cycle_state_events WHERE cycle_id=? AND to_state='CLOSED'", cyc).Scan(&events)
	if fills != 1 || events != 1 {
		t.Errorf("after repeat: fills=%d closedEvents=%d, want 1/1 (idempotent)", fills, events)
	}
	if f.cycleState(cyc) != "CLOSED" {
		t.Errorf("cycle = %s, want CLOSED", f.cycleState(cyc))
	}
}

func TestOnSellCancelResultSchedulesStatus(t *testing.T) {
	f := setupO(t)
	cyc, _, sellOrd := f.seedSell(state.CycleSellRepricePending, state.OrderCancelPending, "EX-S", "0.5", "50")
	req := f.seedReq(cyc, sellOrd, queue.TypeCancelOrder, "IN_FLIGHT")
	f.tx(func(tx *sql.Tx) error {
		return OnSellCancelResult(f.ctx, tx, f.q, SellCancelParams{RequestID: req, OrderID: sellOrd, CycleID: cyc, ExchangeID: f.exID,
			Symbol: "X/IRT", ExchangeOrderID: "EX-S", RawResp: json.RawMessage(`{}`), FinalCheckDelay: 100 * time.Millisecond})
	})
	if f.reqStatus(req) != "SUCCEEDED" {
		t.Errorf("cancel request = %s, want SUCCEEDED", f.reqStatus(req))
	}
	// Order stays CANCEL_PENDING (the GET_ORDER decides the real outcome).
	if f.orderState(sellOrd) != "CANCEL_PENDING" {
		t.Errorf("order = %s, want CANCEL_PENDING", f.orderState(sellOrd))
	}
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE order_id=? AND request_type='GET_ORDER'", sellOrd).Scan(&n)
	if n != 1 {
		t.Errorf("scheduled sell-status checks = %d, want 1", n)
	}
}

func TestSellRepriceCancelPartialLeavesRepricePending(t *testing.T) {
	f := setupO(t)
	cyc, _, sellOrd := f.seedSell(state.CycleSellRepricePending, state.OrderCancelPending, "EX-S", "0.5", "50")
	lock := f.seedLock(cyc)
	// The reprice cancel captured a partial fill (0.2 of 0.5). Order CANCELLED; the
	// cycle stays SELL_REPRICE_PENDING so the manager sells the remaining 0.3.
	out := f.runSellStatus(cyc, sellOrd, sellStatus(execution.StateCanceled, "0.2", "0.3", "110", "22"), nil)
	if out.Closed {
		t.Fatalf("out = %+v, want not closed (partial)", out)
	}
	if f.orderState(sellOrd) != "CANCELLED" || f.cycleState(cyc) != "SELL_REPRICE_PENDING" {
		t.Errorf("states = ord:%s cyc:%s, want CANCELLED/SELL_REPRICE_PENDING", f.orderState(sellOrd), f.cycleState(cyc))
	}
	if f.lockState(lock) != "ACTIVE" {
		t.Error("lock must stay ACTIVE (still holding inventory)")
	}
}

func TestSellRepriceCancelRacedFullFillCloses(t *testing.T) {
	f := setupO(t)
	cyc, _, sellOrd := f.seedSell(state.CycleSellRepricePending, state.OrderCancelPending, "EX-S", "0.5", "50")
	lock := f.seedLock(cyc)
	// The cancel raced a FULL fill: the GET_ORDER shows filled -> close.
	out := f.runSellStatus(cyc, sellOrd, sellStatus(execution.StateFilled, "0.5", "0", "110", "55"), nil)
	if !out.Closed || !out.LockReleased {
		t.Fatalf("out = %+v, want closed + lock released", out)
	}
	if f.cycleState(cyc) != "CLOSED" || f.lockState(lock) != "RELEASED" {
		t.Errorf("cycle/lock = %s/%s, want CLOSED/RELEASED", f.cycleState(cyc), f.lockState(lock))
	}
}
