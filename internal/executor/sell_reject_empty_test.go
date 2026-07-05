package executor

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/queue"
)

// seedSellFollowup enqueues a QUEUED sell follow-up (CANCEL_ORDER / GET_ORDER) with a chosen
// purpose + exchange_order_id, returning the request id.
func (it *intg) seedSellFollowup(t *testing.T, cyc, ord int64, reqType queue.RequestType, purpose, exoid string) int64 {
	t.Helper()
	seedSeq++
	payload, _ := json.Marshal(orders.FollowupPayload{ExchangeOrderID: exoid, Purpose: purpose, CycleID: cyc})
	res, err := it.db.Exec(`INSERT INTO exchange_requests
		(exchange_id, cycle_id, order_id, request_type, priority, status, payload, timeout_ms, max_retries, idempotency_key)
		VALUES (?, ?, ?, ?, 40, 'QUEUED', ?, 10000, 5, ?)`,
		it.exID, cyc, ord, string(reqType), payload, fmt.Sprintf("sf_%d_%d", cyc, seedSeq))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id
}

// TestSellDefiniteRejectionKeepsLock (PR11 #1) — a DEFINITELY-rejected sell place must NOT
// behave like a buy rejection (which releases the lock): we still hold inventory from the buy
// leg. Request FAILED, order+cycle NEEDS_RECONCILE, lock ACTIVE (held).
func TestSellDefiniteRejectionKeepsLock(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, req := it.seedSellPlace(t)                // exit_sell order + valid sell PLACE + ACTIVE lock
	it.fake.placeErr = execution.ErrInsufficientBalance // a DEFINITE rejection

	it.drive(3)

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 1 {
		t.Errorf("placeCount=%d, want 1 (the place was attempted and definitively rejected)", got)
	}
	if s := reqStatus(t, it.db, req); s != "FAILED" {
		t.Errorf("request status=%s, want FAILED", s)
	}
	if it.orderState(ord) != "NEEDS_RECONCILE" || it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Errorf("order/cycle=%s/%s, want NEEDS_RECONCILE (inventory still held)", it.orderState(ord), it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock=%s, want ACTIVE — a rejected SELL must NOT release the lock (inventory unresolved)", it.lockStateByCycle(cyc))
	}
}

// TestSellCancelEmptyExchangeOrderIDReconciles (PR11 #2A) — the executor is the final boundary:
// a sell CANCEL_ORDER with an empty exchange_order_id must NOT call CancelOrder("").
func TestSellCancelEmptyExchangeOrderIDReconciles(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	ord, cyc, _ := it.seedSellOrder(t, "SELL_REPRICE_PENDING", "CANCEL_PENDING")
	it.seedLock(t, cyc)
	req := it.seedSellFollowup(t, cyc, ord, queue.TypeCancelOrder, orders.PurposeSellReprice, "") // empty id

	it.drive(3)

	if got := atomic.LoadInt32(&it.fake.cancelCount); got != 0 {
		t.Errorf("cancelCount=%d, want 0 (must never call CancelOrder(\"\"))", got)
	}
	assertReconciledLockHeld(t, it, req, ord, cyc)
}

// TestSellStatusEmptyExchangeOrderIDReconciles (PR11 #2B) — a sell GET_ORDER with an empty
// exchange_order_id must NOT call GetOrder("").
func TestSellStatusEmptyExchangeOrderIDReconciles(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	ord, cyc, _ := it.seedSellOrder(t, "SELL_SUBMITTED", "ACKED")
	it.seedLock(t, cyc)
	req := it.seedSellFollowup(t, cyc, ord, queue.TypeGetOrder, orders.PurposeSellStatus, "") // empty id

	it.drive(3)

	if got := atomic.LoadInt32(&it.fake.getCount); got != 0 {
		t.Errorf("getCount=%d, want 0 (must never call GetOrder(\"\"))", got)
	}
	assertReconciledLockHeld(t, it, req, ord, cyc)
}

// assertReconciledLockHeld: request terminal (FAILED or DEAD), order+cycle NEEDS_RECONCILE,
// lock ACTIVE.
func assertReconciledLockHeld(t *testing.T, it *intg, req, ord, cyc int64) {
	t.Helper()
	if s := reqStatus(t, it.db, req); s != "FAILED" && s != "DEAD" {
		t.Errorf("request status=%s, want FAILED or DEAD", s)
	}
	if it.orderState(ord) != "NEEDS_RECONCILE" || it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Errorf("order/cycle=%s/%s, want NEEDS_RECONCILE", it.orderState(ord), it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock=%s, want ACTIVE (held)", it.lockStateByCycle(cyc))
	}
}
