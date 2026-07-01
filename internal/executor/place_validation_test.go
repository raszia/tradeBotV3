package executor

import (
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/orders"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

// setPayload overwrites a request's payload (to inject an invalid buy intent).
func (it *intg) setPayload(t *testing.T, reqID int64, p orders.BuyIntentPayload) {
	t.Helper()
	b, _ := json.Marshal(p)
	if _, err := it.db.Exec("UPDATE exchange_requests SET payload=? WHERE id=?", b, reqID); err != nil {
		t.Fatal(err)
	}
}

func validIntent(loc string) orders.BuyIntentPayload {
	return orders.BuyIntentPayload{Side: "buy", OrderType: "limit", SimulatedIOC: true,
		IntendedPrice: "100", IntendedQuantity: "0.5", LocalClientOrderID: loc}
}

// TestInvalidBuyPayloadNotSent (PR10 #5) — a malformed/zero-value buy intent is rejected
// BEFORE PlaceOrder: nothing is sent, the request is FAILED, order+cycle FAILED, lock released.
func TestInvalidBuyPayloadNotSent(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*orders.BuyIntentPayload)
	}{
		// Routing is by the DB order role (entry_buy), so even a payload that lies about its
		// side reaches the buy handler and is rejected by Validate() — it can NOT slip into
		// the sell handler (PR10 #2).
		{"invalid-price", func(p *orders.BuyIntentPayload) { p.IntendedPrice = "abc" }},
		{"zero-price", func(p *orders.BuyIntentPayload) { p.IntendedPrice = "0" }},
		{"invalid-quantity", func(p *orders.BuyIntentPayload) { p.IntendedQuantity = "not-a-number" }},
		{"zero-quantity", func(p *orders.BuyIntentPayload) { p.IntendedQuantity = "0" }},
		{"empty-client-id", func(p *orders.BuyIntentPayload) { p.LocalClientOrderID = "" }},
		{"wrong-side-payload", func(p *orders.BuyIntentPayload) { p.Side = "sell" }},         // #2 A
		{"simulated-ioc-false", func(p *orders.BuyIntentPayload) { p.SimulatedIOC = false }}, // #2 B
		{"wrong-order-type", func(p *orders.BuyIntentPayload) { p.OrderType = "market" }},    // #2 C
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			it := setup(t)
			it.iocExec(t)
			cyc, ord, req := it.seedBuyCycle(t, "0.5")
			p := validIntent("loc-" + tc.name)
			tc.mutate(&p)
			it.setPayload(t, req, p)
			it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "SHOULD-NOT-BE-SENT"}

			it.drive(3)

			if got := atomic.LoadInt32(&it.fake.placeCount); got != 0 {
				t.Errorf("placeCount=%d, want 0 (invalid payload must NOT be sent)", got)
			}
			if s := reqStatus(t, it.db, req); s != "FAILED" {
				t.Errorf("request status=%s, want FAILED", s)
			}
			if it.orderState(ord) != "FAILED" || it.cycleState(cyc) != "FAILED" {
				t.Errorf("order/cycle=%s/%s, want FAILED/FAILED", it.orderState(ord), it.cycleState(cyc))
			}
			if it.lockStateByCycle(cyc) != "RELEASED" {
				t.Errorf("lock=%s, want RELEASED (no exposure — nothing was placed)", it.lockStateByCycle(cyc))
			}
		})
	}
}

// TestMalformedBuyPayloadRejectedCleanly (PR10 #1) — a buy PLACE_ORDER whose payload cannot be
// DECODED must not merely fail the request: it is rejected cleanly (request+order+cycle FAILED,
// lock RELEASED), never leaving the order/cycle/lock stuck. Routing is still by DB order role.
func TestMalformedBuyPayloadRejectedCleanly(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, req := it.seedBuyCycle(t, "0.5")
	// Valid JSON (the column is JSON NOT NULL) but undecodable into BuyIntentPayload:
	// intended_price is a string field but given an object.
	if _, err := it.db.Exec("UPDATE exchange_requests SET payload=? WHERE id=?", `{"side":"buy","intended_price":{"x":1}}`, req); err != nil {
		t.Fatal(err)
	}
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "SHOULD-NOT-BE-SENT"}

	it.drive(3)

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 0 {
		t.Errorf("placeCount=%d, want 0 (undecodable payload must NOT be sent)", got)
	}
	if s := reqStatus(t, it.db, req); s != "FAILED" {
		t.Errorf("request status=%s, want FAILED", s)
	}
	if it.orderState(ord) != "FAILED" || it.cycleState(cyc) != "FAILED" {
		t.Errorf("order/cycle=%s/%s, want FAILED/FAILED (not left stuck)", it.orderState(ord), it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "RELEASED" {
		t.Errorf("lock=%s, want RELEASED (no exposure — nothing was placed)", it.lockStateByCycle(cyc))
	}
}

// TestBuyOrderWithSellPayloadNotRoutedToSell (PR10 #2) — a DB entry-buy order whose payload
// lies (side=sell) is routed to the BUY handler by DB role and rejected by Validate(); it must
// NOT reach the sell handler, and no order is sent.
func TestBuyOrderWithSellPayloadNotRoutedToSell(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, req := it.seedBuyCycle(t, "0.5") // DB order role = entry_buy
	p := validIntent("loc-wrong-side")
	p.Side = "sell" // payload lies about side
	it.setPayload(t, req, p)
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "SHOULD-NOT-BE-SENT"}

	it.drive(3)

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 0 {
		t.Errorf("placeCount=%d, want 0 (wrong-side payload must not be sent)", got)
	}
	if s := reqStatus(t, it.db, req); s != "FAILED" {
		t.Errorf("request status=%s, want FAILED", s)
	}
	if it.orderState(ord) != "FAILED" || it.cycleState(cyc) != "FAILED" {
		t.Errorf("order/cycle=%s/%s, want FAILED/FAILED (buy handler rejected the sell payload)", it.orderState(ord), it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "RELEASED" {
		t.Errorf("lock=%s, want RELEASED", it.lockStateByCycle(cyc))
	}
}

// TestEmptyExchangeOrderIDGoesToReconcile (PR10 #6) — a PlaceOrder ack with no usable
// exchange_order_id must NOT schedule a blind CANCEL_ORDER("")/GET_ORDER(""): order+cycle go
// to NEEDS_RECONCILE and the lock is preserved.
func TestEmptyExchangeOrderIDGoesToReconcile(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, req := it.seedBuyCycle(t, "0.5")
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "", ClientOrderID: ""} // no usable id

	it.drive(4)

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 1 {
		t.Errorf("placeCount=%d, want 1 (the place itself happened)", got)
	}
	if got := atomic.LoadInt32(&it.fake.cancelCount); got != 0 {
		t.Errorf("cancelCount=%d, want 0 (no blind cancel with empty id)", got)
	}
	var followups int
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE cycle_id=? AND request_type IN ('CANCEL_ORDER','GET_ORDER')", cyc).Scan(&followups)
	if followups != 0 {
		t.Errorf("scheduled follow-up requests=%d, want 0 (no blind cancel/status)", followups)
	}
	if s := reqStatus(t, it.db, req); s != "SUCCEEDED" {
		t.Errorf("PLACE request status=%s, want SUCCEEDED (the place did happen)", s)
	}
	if it.orderState(ord) != "NEEDS_RECONCILE" || it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Errorf("order/cycle=%s/%s, want NEEDS_RECONCILE", it.orderState(ord), it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock=%s, want ACTIVE (preserved for the reconciler)", it.lockStateByCycle(cyc))
	}
}

// TestFullFillDerivesAvgFromExecutedQuote (PR10 #7) — a full fill reporting no AvgPrice but a
// positive ExecutedQuote derives avg = quote/qty and records a valid fill; order → FILLED.
func TestFullFillDerivesAvgFromExecutedQuote(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, _ := it.seedBuyCycle(t, "0.5")
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXE-Q"}
	it.fake.getStatus = execution.OrderStatus{Status: execution.StateFilled, ExchangeOrderID: "EXE-Q",
		IntendedQty: d("0.5"), FilledQty: d("0.5"), RemainingQty: d("0"),
		AvgPrice: d("0"), ExecutedQuote: d("50")} // 50 / 0.5 = 100

	it.drive(6)

	if it.orderState(ord) != "FILLED" || it.cycleState(cyc) != "BUY_FILLED" {
		t.Fatalf("states=%s/%s, want FILLED/BUY_FILLED", it.orderState(ord), it.cycleState(cyc))
	}
	var price, quote string
	if err := it.db.QueryRow("SELECT price, quote_amount FROM fills WHERE order_id=?", ord).Scan(&price, &quote); err != nil {
		t.Fatalf("expected a recorded fill: %v", err)
	}
	if !d(price).Equal(d("100")) {
		t.Errorf("derived fill price=%s, want 100 (executed_quote 50 / qty 0.5)", price)
	}
	var avg string
	it.db.QueryRow("SELECT COALESCE(avg_fill_price,'') FROM orders WHERE id=?", ord).Scan(&avg)
	if !d(avg).Equal(d("100")) {
		t.Errorf("order avg_fill_price=%s, want 100", avg)
	}
}

// TestFullFillNoCostBasisIsAmbiguous (PR10 #7) — a full fill with neither AvgPrice nor
// ExecutedQuote has no cost basis: it is ambiguous → NEEDS_RECONCILE, lock held, no fill row.
func TestFullFillNoCostBasisIsAmbiguous(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, _ := it.seedBuyCycle(t, "0.5")
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXE-N"}
	it.fake.getStatus = execution.OrderStatus{Status: execution.StateFilled, ExchangeOrderID: "EXE-N",
		IntendedQty: d("0.5"), FilledQty: d("0.5"), RemainingQty: d("0"),
		AvgPrice: d("0"), ExecutedQuote: d("0")} // no usable cost basis

	it.drive(6)

	if it.orderState(ord) != "NEEDS_RECONCILE" || it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Fatalf("states=%s/%s, want NEEDS_RECONCILE/NEEDS_RECONCILE", it.orderState(ord), it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Error("ambiguous full fill must keep the lock ACTIVE")
	}
	var fills int
	it.db.QueryRow("SELECT COUNT(*) FROM fills WHERE order_id=?", ord).Scan(&fills)
	if fills != 0 {
		t.Errorf("fills=%d, want 0 (never record a fill without a valid cost basis)", fills)
	}
}

// TestScheduledFollowupsHaveZeroRetryCount (PR10 #8) — the simulated-IOC scheduled next steps
// (CANCEL_ORDER, then GET_ORDER) are planned steps, not error retries: retry_count = 0.
func TestScheduledFollowupsHaveZeroRetryCount(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, _, _ := it.seedBuyCycle(t, "0.5")
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXE-S"}
	it.fake.getStatus = execution.OrderStatus{Status: execution.StateCanceled, ExchangeOrderID: "EXE-S",
		IntendedQty: d("0.5"), FilledQty: d("0"), RemainingQty: d("0.5")}

	it.drive(6)

	for _, typ := range []string{"CANCEL_ORDER", "GET_ORDER"} {
		var rc int
		if err := it.db.QueryRow("SELECT retry_count FROM exchange_requests WHERE cycle_id=? AND request_type=? ORDER BY id DESC LIMIT 1", cyc, typ).Scan(&rc); err != nil {
			t.Fatalf("expected a scheduled %s: %v", typ, err)
		}
		if rc != 0 {
			t.Errorf("%s retry_count=%d, want 0 (planned next step, not an error retry)", typ, rc)
		}
	}
}
