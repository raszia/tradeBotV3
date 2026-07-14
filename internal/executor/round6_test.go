package executor

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/orders"
)

// PR20 round-6 executor tests: pre-handler failures never strand (#1), temporary ErrNotSent
// uses real backoff while a proven rate limit uses the cooldown deadline (#5).

// TestPreHandlerBadCancelPayloadNeedsReconcile: a malformed CANCEL payload (permanent) must not
// fail only the queue row — the order/cycle go NEEDS_RECONCILE with the lock HELD.
func TestPreHandlerBadCancelPayloadNeedsReconcile(t *testing.T) {
	it := setup(t)
	it.liveFakeExec(t, Config{})
	_, ord, _ := it.seedBuyCycle(t, "0.5")
	it.enableAllLive()
	cyc := it.cycleOf(t, ord)
	it.db.Exec("UPDATE orders SET state='ACKED', exchange_order_id='EXT-C' WHERE id=?", ord)
	// A CANCEL_ORDER with a non-JSON payload.
	res, err := it.db.Exec(`INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, symbol, request_type, status, payload, timeout_ms, max_retries, idempotency_key)
		VALUES (?, ?, ?, 'X/IRT', 'CANCEL_ORDER', 'CLAIMED', '123', 10000, 5, ?)`, it.exID, cyc, ord, "badc_"+time.Now().Format("150405.000000"))
	if err != nil {
		t.Fatal(err)
	}
	reqID, _ := res.LastInsertId()
	it.exec.handleCancel(it.ctx, it.claimOf(t, reqID), it.fake)

	if it.orderState(ord) != "NEEDS_RECONCILE" || it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Errorf("malformed cancel: order=%s cycle=%s, want NEEDS_RECONCILE (not stranded)", it.orderState(ord), it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (held — a venue order may be open)", it.lockStateByCycle(cyc))
	}
	if s := reqStatus(t, it.db, reqID); s != "DEAD" {
		t.Errorf("request = %s, want DEAD", s)
	}
}

// TestPreHandlerOrderRoleTempFailureRecoverable: a TEMPORARY failure reading the order role
// (before dispatch) re-queues with backoff — the cycle is never stranded, the request stays
// recoverable, and retry_at is in the FUTURE (not immediately exhausted).
func TestPreHandlerOrderRoleTempFailureRecoverable(t *testing.T) {
	it := setup(t)
	it.liveFakeExec(t, Config{ExchangeTuningFor: func(string) (int, time.Duration) { return 0, 2 * time.Second }})
	cyc, ord, req := it.seedBuyCycle(t, "0.5")
	it.enableAllLive()
	c := it.claimOf(t, req)
	// Simulate a temporary DB failure inside dispatch by disposing through the same path the
	// dispatcher uses for a transient order-role read error.
	it.exec.requeueUnsent(it.ctx, c, orders.KindUnknown,
		execution.NotSent(errors.New("cannot read order role: temporary db error")), true)

	if s := reqStatus(t, it.db, req); s != "RETRY_SCHEDULED" {
		t.Errorf("temporary pre-handler failure = %s, want RETRY_SCHEDULED (recoverable)", s)
	}
	if it.cycleState(cyc) == "FAILED" || it.orderState(ord) == "FAILED" {
		t.Errorf("temporary pre-handler failure must not strand: order=%s cycle=%s", it.orderState(ord), it.cycleState(cyc))
	}
	var nextRetry time.Time
	it.db.QueryRow("SELECT next_retry_at FROM exchange_requests WHERE id=?", req).Scan(&nextRetry)
	if !nextRetry.After(time.Now()) {
		t.Errorf("next_retry_at = %v, want a FUTURE time (real backoff, not immediate)", nextRetry)
	}
}

// TestTemporaryNotSentUsesBackoffNotCooldownZero: a temporary local ErrNotSent (no rate limit)
// must schedule a real future backoff — never retry_at = now (which would burn all retries in a
// second). And last_error must preserve the real reason (not "rate-limited").
func TestTemporaryNotSentUsesBackoffNotCooldownZero(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	_, _, req := it.seedBuyCycle(t, "0.5")
	c := it.claimOf(t, req)
	c.RetryCount = 0
	it.db.Exec("UPDATE exchange_requests SET status='IN_FLIGHT' WHERE id=?", req) // post-MarkInFlight
	before := time.Now()
	it.exec.requeueUnsent(it.ctx, c, orders.KindEntryBuy,
		execution.NotSent(errors.New("credential db temporarily unavailable")), false)

	var nextRetry time.Time
	var lastErr string
	it.db.QueryRow("SELECT next_retry_at, COALESCE(last_error,'') FROM exchange_requests WHERE id=?", req).Scan(&nextRetry, &lastErr)
	if !nextRetry.After(before.Add(200 * time.Millisecond)) {
		t.Errorf("next_retry_at = %v, want a real future backoff (not ~now)", nextRetry)
	}
	if contains(lastErr, "rate-limited") || contains(lastErr, "rate limit") {
		t.Errorf("last_error = %q — a credential failure must NOT be labelled a rate limit", lastErr)
	}
	if !contains(lastErr, "credential db") {
		t.Errorf("last_error = %q, want the real credential failure reason preserved", lastErr)
	}
}

// TestProvenRateLimitUsesCooldownDeadline: a venue-proven rate limit retries at the cooldown
// deadline (arming the cooldown), NOT the plain backoff.
func TestProvenRateLimitUsesCooldownDeadline(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	_, _, req := it.seedBuyCycle(t, "0.5")
	c := it.claimOf(t, req)
	it.db.Exec("UPDATE exchange_requests SET status='IN_FLIGHT' WHERE id=?", req) // post-MarkInFlight
	rlErr := &exchanges.NormalizedAPIError{Exchange: it.code, Op: "PlaceOrder", StatusCode: 429,
		Category: exchanges.CatRateLimit, Err: execution.ErrRateLimited,
		RateLimit: &exchanges.RateLimitInfo{RetryAfter: 10 * time.Minute, Source: exchanges.RLSourceHeader}}
	it.exec.requeueUnsent(it.ctx, c, orders.KindEntryBuy, rlErr, false)

	if !it.exec.parked(it.code) {
		t.Error("a proven rate limit must arm the exchange cooldown")
	}
	var nextRetry time.Time
	it.db.QueryRow("SELECT next_retry_at FROM exchange_requests WHERE id=?", req).Scan(&nextRetry)
	if d := time.Until(nextRetry); d < 9*time.Minute {
		t.Errorf("next_retry_at is %v away, want ~10m (the cooldown deadline)", d)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

var _ = atomic.LoadInt32
