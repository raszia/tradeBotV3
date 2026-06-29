package executor

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/queue"
	"v3TradeBot/internal/state"
)

// Crash / rollback recovery tests (PR26 correction). They use the no-network fake client +
// a TEST-ONLY post-send fault hook to force the completion transaction to roll back AFTER
// the exchange "send", then drive the sweeper to verify conservative recovery. No real
// venue, no API key, no real order.

// execFault rebuilds the executor with the fault hook (and a tiny final-status delay).
func (it *intg) execFault(fault func() error) {
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "test-exec", AllowLiveExecution: true, FinalStatusDelay: 5 * time.Millisecond, faultAfterSend: fault})
}

func (it *intg) seedLock(t *testing.T, cycleID int64) {
	t.Helper()
	var sym string
	it.db.QueryRow("SELECT canonical_symbol FROM cycles WHERE id=?", cycleID).Scan(&sym)
	if _, err := it.db.Exec(
		"INSERT INTO symbol_locks (scope, canonical_symbol, cycle_id, state, expires_at) VALUES (?, ?, ?, 'ACTIVE', NOW(6)+INTERVAL 1 HOUR)",
		fmt.Sprintf("%s_%d", it.code, cycleID), sym, cycleID); err != nil {
		t.Fatal(err)
	}
}

// seedCancel seeds a CLAIMED CANCEL_ORDER request (simulated-IOC / reprice cancel) with the
// FollowupPayload the cancel handler expects.
func (it *intg) seedCancel(t *testing.T, orderID, cycleID int64, exchangeOrderID string) queue.Claimed {
	t.Helper()
	seedSeq++
	payload := fmt.Sprintf(`{"exchange_order_id":%q,"local_client_order_id":"loc"}`, exchangeOrderID)
	res, err := it.db.Exec(`INSERT INTO exchange_requests
		(exchange_id, cycle_id, order_id, request_type, priority, status, payload, timeout_ms, max_retries, idempotency_key)
		VALUES (?, ?, ?, 'CANCEL_ORDER', 40, 'CLAIMED', ?, 10000, 5, ?)`,
		it.exID, cycleID, orderID, payload, fmt.Sprintf("cidem_%d_%d", time.Now().UnixNano(), seedSeq))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return queue.Claimed{ID: id, ExchangeID: it.exID, ExchangeCode: it.code, Type: queue.TypeCancelOrder,
		Payload: []byte(payload), OrderID: &orderID, CycleID: &cycleID, Symbol: "X/IRT", TimeoutMS: 10000, MaxRetries: 5}
}

// seedSellOrder seeds a cycle + an exit_sell order in the given states; returns the order,
// cycle, and exchange_market ids.
func (it *intg) seedSellOrder(t *testing.T, cycleSt, orderSt string) (orderID, cycleID, emID int64) {
	t.Helper()
	seedSeq++
	last := func(r interface{ LastInsertId() (int64, error) }) int64 { id, _ := r.LastInsertId(); return id }
	ex := func(q string, a ...any) interface{ LastInsertId() (int64, error) } {
		r, err := it.db.Exec(q, a...)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), seedSeq) }
	b := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("SB")))
	qa := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("SQ")))
	m := last(ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", u("SM")+"/IRT", b, qa))
	emID = last(ex("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, ?)", it.exID, m, u("SES"), u("SM")+"/IRT"))
	cycleID = last(ex("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state) VALUES (?, ?, ?, ?)", emID, it.exID, u("SM")+"/IRT", cycleSt))
	orderID = last(ex(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, order_type, limit_price, quantity)
		VALUES (?, ?, ?, 'sell', 'exit_sell', ?, ?, 'limit', '110', '1')`, cycleID, it.exID, emID, u("sloc"), orderSt))
	return orderID, cycleID, emID
}

func (it *intg) lockState(cycleID int64) string {
	var s string
	it.db.QueryRow("SELECT state FROM symbol_locks WHERE cycle_id=? ORDER BY id DESC LIMIT 1", cycleID).Scan(&s)
	return s
}

// backdateInflight ages a request's inflight_at past its timeout so SweepStuck reclaims it.
func (it *intg) backdateInflight(reqID int64) {
	it.db.Exec("UPDATE exchange_requests SET inflight_at = NOW(6) - INTERVAL 1 HOUR WHERE id=?", reqID)
}

func (it *intg) sweep(t *testing.T) {
	if _, err := it.q.SweepStuck(it.ctx, 0); err != nil {
		t.Fatal(err)
	}
}

// 1. Crash/rollback after DB commit, before send: the committed QUEUED request is
// recoverable, claimed + sent EXACTLY once, and re-processing creates no duplicate send.
func TestCrashAfterCommitBeforeSendRecoverableNoDuplicate(t *testing.T) {
	it := setup(t)
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EX1", Status: execution.StateOpen}
	cyc, ord, req := it.seedBuyCycle(t, "0.5") // commits a QUEUED PLACE + cycle + order + lock
	if reqStatus(t, it.db, req) != "QUEUED" {
		t.Fatalf("committed request must be QUEUED (recoverable), got %s", reqStatus(t, it.db, req))
	}
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	it.drive(3) // simulate restart: claim → send (once) → schedule cancel
	if got := atomic.LoadInt32(&it.fake.placeCount); got != 1 {
		t.Fatalf("placeCount = %d, want exactly 1 (no duplicate send)", got)
	}
	if reqStatus(t, it.db, req) != "SUCCEEDED" {
		t.Errorf("place request = %s, want SUCCEEDED", reqStatus(t, it.db, req))
	}
	it.drive(2) // re-driving must not re-send (the request is no longer claimable)
	if got := atomic.LoadInt32(&it.fake.placeCount); got != 1 {
		t.Errorf("re-drive re-sent the place: placeCount = %d, want 1", got)
	}
	// Exactly one cycle/order/request for this scope — no duplicate created.
	var nReq int
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE cycle_id=? AND request_type='PLACE_ORDER'", cyc).Scan(&nReq)
	if nReq != 1 {
		t.Errorf("PLACE requests for cycle = %d, want 1", nReq)
	}
	_ = ord
}

// 2. Crash after MarkInFlight commits, before any exchange response: the request is stuck
// IN_FLIGHT → sweeper dead-letters the MUTATING request (never re-sends) and pushes the
// order to NEEDS_RECONCILE; the symbol lock stays held.
func TestCrashAfterMarkInFlightBeforeResponse(t *testing.T) {
	it := setup(t)
	orderID, cycleID := it.seedBuyOrder(t, string(state.CycleBuySubmitted), string(state.OrderSubmitted))
	it.seedLock(t, cycleID)
	c := it.seedPlace(t, orderID, cycleID, orders.BuyIntentPayload{Side: "buy", IntendedQuantity: "1"})
	// Simulate "crashed right after MarkInFlight, before the PlaceOrder call returned".
	if err := it.q.MarkInFlight(it.ctx, c.ID); err != nil {
		t.Fatal(err)
	}
	it.backdateInflight(c.ID)
	it.sweep(t)

	if reqStatus(t, it.db, c.ID) != "DEAD" {
		t.Errorf("stuck mutating request = %s, want DEAD (never re-sent)", reqStatus(t, it.db, c.ID))
	}
	if ordState(t, it.db, orderID) != "NEEDS_RECONCILE" {
		t.Errorf("owning order = %s, want NEEDS_RECONCILE", ordState(t, it.db, orderID))
	}
	if it.lockState(cycleID) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (held — exposure unknown)", it.lockState(cycleID))
	}
	if got := atomic.LoadInt32(&it.fake.placeCount); got != 0 {
		t.Errorf("placeCount = %d, want 0 (sweeper must never send)", got)
	}
}

// 3. PlaceOrder SUCCEEDS on the venue but the completion transaction rolls back: the send
// happened once, the request stays IN_FLIGHT, the order/cycle are NOT advanced; the sweeper
// then dead-letters it + reconciles the order WITHOUT a second send; the lock stays held.
func TestPlaceSucceedsCompletionRollsBackThenSweepReconciles(t *testing.T) {
	it := setup(t)
	orderID, cycleID := it.seedBuyOrder(t, string(state.CycleBuyRequestQueued), string(state.OrderQueued))
	it.seedLock(t, cycleID)
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EX1", Status: execution.StateOpen}
	it.execFault(func() error { return errors.New("injected: completion tx failure after send") })
	c := it.seedPlace(t, orderID, cycleID, orders.BuyIntentPayload{Side: "buy", IntendedQuantity: "1", LocalClientOrderID: "loc"})
	it.exec.process(it.ctx, c)

	// Sent exactly once; completion rolled back → request IN_FLIGHT; order/cycle unchanged.
	if got := atomic.LoadInt32(&it.fake.placeCount); got != 1 {
		t.Fatalf("placeCount = %d, want 1 (sent once)", got)
	}
	if reqStatus(t, it.db, c.ID) != "IN_FLIGHT" {
		t.Errorf("request = %s, want IN_FLIGHT (completion rolled back, not SUCCEEDED)", reqStatus(t, it.db, c.ID))
	}
	if ordState(t, it.db, orderID) != string(state.OrderQueued) {
		t.Errorf("order = %s, want unchanged QUEUED (completion rolled back)", ordState(t, it.db, orderID))
	}
	// Recovery: the sweeper dead-letters the stuck mutating request, never re-sending it.
	it.backdateInflight(c.ID)
	it.sweep(t)
	if reqStatus(t, it.db, c.ID) != "DEAD" {
		t.Errorf("after sweep request = %s, want DEAD (no re-send)", reqStatus(t, it.db, c.ID))
	}
	if ordState(t, it.db, orderID) != "NEEDS_RECONCILE" {
		t.Errorf("order = %s, want NEEDS_RECONCILE", ordState(t, it.db, orderID))
	}
	if it.lockState(cycleID) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (held)", it.lockState(cycleID))
	}
	if got := atomic.LoadInt32(&it.fake.placeCount); got != 1 {
		t.Errorf("placeCount = %d, want 1 (NEVER re-sent)", got)
	}
}

// 4. CancelOrder SUCCEEDS but the completion transaction rolls back: the cancel is not
// blindly retried; the order/cycle resolve to NEEDS_RECONCILE (final status must come from
// GET_ORDER or the operator), and the lock is NOT released (exposure not proven zero).
func TestCancelSucceedsCompletionRollsBackThenSweepReconciles(t *testing.T) {
	it := setup(t)
	orderID, cycleID := it.seedBuyOrder(t, string(state.CycleBuySubmitted), string(state.OrderAcked))
	it.seedLock(t, cycleID)
	it.execFault(func() error { return errors.New("injected: cancel completion tx failure after send") })
	c := it.seedCancel(t, orderID, cycleID, "EX1")
	it.exec.process(it.ctx, c)

	if got := atomic.LoadInt32(&it.fake.cancelCount); got != 1 {
		t.Fatalf("cancelCount = %d, want 1 (sent once)", got)
	}
	if reqStatus(t, it.db, c.ID) != "IN_FLIGHT" {
		t.Errorf("cancel request = %s, want IN_FLIGHT (completion rolled back)", reqStatus(t, it.db, c.ID))
	}
	it.backdateInflight(c.ID)
	it.sweep(t)
	if reqStatus(t, it.db, c.ID) != "DEAD" {
		t.Errorf("after sweep cancel = %s, want DEAD (not blindly retried)", reqStatus(t, it.db, c.ID))
	}
	if got := ordState(t, it.db, orderID); got != "NEEDS_RECONCILE" {
		t.Errorf("order = %s, want NEEDS_RECONCILE (not assumed-cancelled/zero-fill)", got)
	}
	if it.lockState(cycleID) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (not released — exposure not proven zero)", it.lockState(cycleID))
	}
	if got := atomic.LoadInt32(&it.fake.cancelCount); got != 1 {
		t.Errorf("cancelCount = %d, want 1 (never re-sent)", got)
	}
}

// 5. Crash during a sell reprice after the cancel request became IN_FLIGHT: no replacement
// sell is created blindly; the stuck mutating cancel dead-letters + the order goes to
// NEEDS_RECONCILE (final status required); no oversell; the lock stays held.
func TestCrashDuringSellRepriceCancelInFlight(t *testing.T) {
	it := setup(t)
	orderID, cycleID, emID := it.seedSellOrder(t, string(state.CycleSellRepricePending), string(state.OrderCancelPending))
	it.seedLock(t, cycleID)
	c := it.seedCancel(t, orderID, cycleID, "SX1")
	// Simulate "crashed after the reprice cancel went IN_FLIGHT".
	if err := it.q.MarkInFlight(it.ctx, c.ID); err != nil {
		t.Fatal(err)
	}
	var sellsBefore int
	it.db.QueryRow("SELECT COUNT(*) FROM orders WHERE exchange_market_id=? AND role='exit_sell'", emID).Scan(&sellsBefore)

	it.backdateInflight(c.ID)
	it.sweep(t)

	if reqStatus(t, it.db, c.ID) != "DEAD" {
		t.Errorf("sell cancel = %s, want DEAD", reqStatus(t, it.db, c.ID))
	}
	if ordState(t, it.db, orderID) != "NEEDS_RECONCILE" {
		t.Errorf("sell order = %s, want NEEDS_RECONCILE", ordState(t, it.db, orderID))
	}
	var sellsAfter int
	it.db.QueryRow("SELECT COUNT(*) FROM orders WHERE exchange_market_id=? AND role='exit_sell'", emID).Scan(&sellsAfter)
	if sellsAfter != sellsBefore {
		t.Errorf("a replacement sell was created blindly: %d -> %d (oversell risk)", sellsBefore, sellsAfter)
	}
	if it.lockState(cycleID) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (held)", it.lockState(cycleID))
	}
}
