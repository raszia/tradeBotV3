package executor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/orders"
)

// PR20 round-5 executor tests: the first-order checklist runs AFTER the send (#2), and
// pre-network "definitely not sent" errors never become ambiguous outcomes (#3).

// seedActiveSession makes a live run session ACTIVE for the fixture's exchange + a market, and
// returns the session id. The market is the one the order under test uses.
func (it *intg) seedActiveSession(t *testing.T, marketID int64) int64 {
	t.Helper()
	res, err := it.db.Exec(`INSERT INTO live_run_sessions (operator, exchange_id, exchange_market_id, canonical_symbol, preflight_hash, status)
		VALUES ('op', ?, ?, 'X/IRT', REPEAT('a',64), 'ACTIVE')`, it.exID, marketID)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id
}

func (it *intg) orderMarketID(t *testing.T, orderID int64) int64 {
	t.Helper()
	var em int64
	if err := it.db.QueryRow("SELECT exchange_market_id FROM orders WHERE id=?", orderID).Scan(&em); err != nil {
		t.Fatal(err)
	}
	return em
}

// TestFirstOrderChecklistRecordedAfterSend: nothing writes the checklist BEFORE the send — at
// PlaceOrder time the session's checklist is still NULL; it is populated only after the send
// (so no synchronous checklist work sits between the final guard and MarkInFlight).
func TestFirstOrderChecklistRecordedAfterSend(t *testing.T) {
	it := setup(t)
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXT-CL", Status: execution.StateOpen}
	it.liveFakeExec(t, Config{})
	_, ord, req := it.seedBuyCycle(t, "0.5")
	it.enableAllLive()
	em := it.orderMarketID(t, ord)
	sess := it.seedActiveSession(t, em)

	var checklistAtSendTime []byte
	it.fake.onPlace = func() {
		// Inside PlaceOrder (the network boundary): the checklist must NOT be written yet.
		it.db.QueryRow("SELECT first_order_checklist_json FROM live_run_sessions WHERE id=?", sess).Scan(&checklistAtSendTime)
	}

	it.exec.handlePlace(it.ctx, it.claimOf(t, req), it.fake)

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 1 {
		t.Fatalf("PlaceOrder count = %d, want 1", got)
	}
	if checklistAtSendTime != nil {
		t.Error("first-order checklist was written BEFORE/at the send — it must run only after the send")
	}
	var after []byte
	it.db.QueryRow("SELECT first_order_checklist_json FROM live_run_sessions WHERE id=?", sess).Scan(&after)
	if after == nil {
		t.Error("first-order checklist must be recorded after a successful first order")
	}
}

// --- #3: definitely-not-sent classification -------------------------------------------------

// TestPreNetworkErrorNotAmbiguous: a pre-network failure returned by the adapter is classified
// as definitely-not-sent and never becomes an ambiguous recovery probe. Temporary → the
// request is re-queued (proven-unexecuted); permanent buy → clean fail with the exposure proof.
func TestPreNetworkErrorNotAmbiguous(t *testing.T) {
	t.Run("temporary (credential blip) -> requeue, no probe", func(t *testing.T) {
		it := setup(t)
		it.fake.placeErr = execution.NotSent(errors.New("credential db temporarily unavailable"))
		it.liveFakeExec(t, Config{})
		_, ord, req := it.seedBuyCycle(t, "0.5")
		it.enableAllLive()

		it.exec.handlePlace(it.ctx, it.claimOf(t, req), it.fake)

		if probes := it.probeCount(t, ord); probes != 0 {
			t.Errorf("ambiguous probes = %d, want 0 (definitely not sent)", probes)
		}
		if s := reqStatus(t, it.db, req); s != "RETRY_SCHEDULED" {
			t.Errorf("temporary pre-network failure = %s, want RETRY_SCHEDULED (proven-unexecuted requeue)", s)
		}
		if st := it.orderState(ord); st == "NEEDS_RECONCILE" {
			t.Error("a definitely-not-sent temporary failure must not push the order to NEEDS_RECONCILE")
		}
	})

	t.Run("permanent (invalid symbol) buy -> clean fail, no probe", func(t *testing.T) {
		it := setup(t)
		it.fake.placeErr = execution.NotSentPermanent(errors.New("invalid symbol"))
		it.liveFakeExec(t, Config{})
		cyc, ord, req := it.seedBuyCycle(t, "0.5")
		it.enableAllLive()

		it.exec.handlePlace(it.ctx, it.claimOf(t, req), it.fake)

		if probes := it.probeCount(t, ord); probes != 0 {
			t.Errorf("ambiguous probes = %d, want 0 (definitely not sent)", probes)
		}
		if it.orderState(ord) != "FAILED" || it.cycleState(cyc) != "FAILED" {
			t.Errorf("permanent pre-network buy failure: order=%s cycle=%s, want FAILED (no exposure proven)",
				it.orderState(ord), it.cycleState(cyc))
		}
		if it.lockStateByCycle(cyc) != "RELEASED" {
			t.Errorf("lock = %s, want RELEASED (definitely not sent → no exposure)", it.lockStateByCycle(cyc))
		}
	})
}

// TestExpiredSendContextNoAdapterCall: if the context is cancelled after MarkInFlight but
// before the adapter is invoked, the client is NEVER called and the request is recovered as
// definitely-unsent (no ambiguous probe).
func TestExpiredSendContextNoAdapterCall(t *testing.T) {
	it := setup(t)
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXT-X", Status: execution.StateOpen}
	ctx, cancel := context.WithCancel(it.ctx)
	it.liveFakeExec(t, Config{hookAfterMarkInFlight: func() { cancel() }}) // cancel in the exact window
	_, ord, req := it.seedBuyCycle(t, "0.5")
	it.enableAllLive()

	it.exec.handlePlace(ctx, it.claimOf(t, req), it.fake)

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 0 {
		t.Errorf("PlaceOrder called %d times after the send context was cancelled, want 0", got)
	}
	if probes := it.probeCount(t, ord); probes != 0 {
		t.Errorf("ambiguous probes = %d, want 0 (definitely not sent)", probes)
	}
	if s := reqStatus(t, it.db, req); s != "RETRY_SCHEDULED" {
		t.Errorf("cancelled-before-send request = %s, want RETRY_SCHEDULED (proven-unexecuted)", s)
	}
}

// probeCount counts ambiguous-place recovery probes scheduled for an order.
func (it *intg) probeCount(t *testing.T, orderID int64) int {
	t.Helper()
	var n int
	it.db.QueryRow(`SELECT COUNT(*) FROM exchange_requests WHERE order_id=? AND request_type='GET_ORDER'
		AND JSON_EXTRACT(payload,'$.purpose')=?`, orderID, orders.PurposeAmbiguousPlaceProbe).Scan(&n)
	return n
}

var _ = time.Second
