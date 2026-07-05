package executor

import (
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/sellflow"
)

// seedSellPlace creates a resting-sell cycle and enqueues its sell PLACE_ORDER (via the real
// sellflow.CreateSell, so the DB order role = exit_sell), returning the request + order ids.
func (it *intg) seedSellPlace(t *testing.T) (cycleID, orderID, reqID int64) {
	t.Helper()
	cyc, mc := it.seedSellCycle(t, "0.5", "50")
	r, err := sellflow.CreateSell(it.ctx, it.store, it.q, mc, sellflow.CreateParams{
		CycleID: cyc, BinanceRef: decimal.RequireFromString("100"), QuoteUnit: "IRT"})
	if err != nil {
		t.Fatalf("CreateSell: %v", err)
	}
	return cyc, r.OrderID, r.RequestID
}

func (it *intg) setSellPayload(t *testing.T, reqID int64, p orders.SellIntentPayload) {
	t.Helper()
	b, _ := json.Marshal(p)
	if _, err := it.db.Exec("UPDATE exchange_requests SET payload=? WHERE id=?", b, reqID); err != nil {
		t.Fatal(err)
	}
}

func validSell(loc string) orders.SellIntentPayload {
	return orders.SellIntentPayload{Side: "sell", OrderType: "limit", Price: "110", Quantity: "0.5", LocalClientOrderID: loc}
}

// TestInvalidSellPayloadNotSent (PR11 #3) — a malformed/zero/wrong sell payload must not be
// sent: PlaceOrder not called, request FAILED, order+cycle NEEDS_RECONCILE (inventory exists),
// lock HELD.
func TestInvalidSellPayloadNotSent(t *testing.T) {
	cases := []struct {
		name    string
		payload func(loc string) any // returns the payload to store (struct or raw json.RawMessage)
	}{
		{"malformed", func(string) any { return json.RawMessage(`{"side":"sell","price":{"x":1}}`) }},
		{"zero-price", func(loc string) any { p := validSell(loc); p.Price = "0"; return p }},
		{"zero-quantity", func(loc string) any { p := validSell(loc); p.Quantity = "0"; return p }},
		{"empty-client-id", func(loc string) any { p := validSell(loc); p.LocalClientOrderID = ""; return p }},
		{"wrong-side", func(loc string) any { p := validSell(loc); p.Side = "buy"; return p }}, // #2 reverse
		{"wrong-order-type", func(loc string) any { p := validSell(loc); p.OrderType = "market"; return p }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			it := setup(t)
			it.iocExec(t)
			cyc, ord, req := it.seedSellPlace(t)
			switch v := tc.payload("loc-" + tc.name).(type) {
			case json.RawMessage:
				if _, err := it.db.Exec("UPDATE exchange_requests SET payload=? WHERE id=?", []byte(v), req); err != nil {
					t.Fatal(err)
				}
			case orders.SellIntentPayload:
				it.setSellPayload(t, req, v)
			}
			it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "SHOULD-NOT-BE-SENT"}

			it.drive(3)

			if got := atomic.LoadInt32(&it.fake.placeCount); got != 0 {
				t.Errorf("placeCount=%d, want 0 (invalid sell must NOT be sent)", got)
			}
			if s := reqStatus(t, it.db, req); s != "FAILED" {
				t.Errorf("request status=%s, want FAILED", s)
			}
			if it.orderState(ord) != "NEEDS_RECONCILE" || it.cycleState(cyc) != "NEEDS_RECONCILE" {
				t.Errorf("order/cycle=%s/%s, want NEEDS_RECONCILE (inventory exists)", it.orderState(ord), it.cycleState(cyc))
			}
			if it.lockStateByCycle(cyc) != "ACTIVE" {
				t.Errorf("lock=%s, want ACTIVE (held — inventory to protect)", it.lockStateByCycle(cyc))
			}
		})
	}
}

// TestSellEmptyExchangeOrderIDGoesToReconcile (PR11 #4) — a sell PlaceOrder ack with no usable
// exchange_order_id must not leave an untrackable resting sell: order+cycle NEEDS_RECONCILE,
// lock HELD, and no blind follow-up GET_ORDER/CANCEL_ORDER queued.
func TestSellEmptyExchangeOrderIDGoesToReconcile(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, req := it.seedSellPlace(t)
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "", ClientOrderID: ""} // no usable id

	it.drive(3)

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 1 {
		t.Errorf("placeCount=%d, want 1 (the sell place happened)", got)
	}
	if s := reqStatus(t, it.db, req); s != "SUCCEEDED" {
		t.Errorf("sell PLACE request=%s, want SUCCEEDED (place happened)", s)
	}
	var followups int
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE cycle_id=? AND request_type IN ('GET_ORDER','CANCEL_ORDER')", cyc).Scan(&followups)
	if followups != 0 {
		t.Errorf("follow-up requests=%d, want 0 (no blind GetOrder(\"\")/CancelOrder(\"\"))", followups)
	}
	if it.orderState(ord) != "NEEDS_RECONCILE" || it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Errorf("order/cycle=%s/%s, want NEEDS_RECONCILE", it.orderState(ord), it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock=%s, want ACTIVE (preserved)", it.lockStateByCycle(cyc))
	}
}
