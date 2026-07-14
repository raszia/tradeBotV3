package executor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/live"
)

type exec_OrderRequest = execution.OrderRequest
type exec_OrderAck = execution.OrderAck

var (
	exec_NotSent   = execution.NotSent
	exec_StateOpen = execution.StateOpen
)

// installClient rebuilds the executor with a custom client under it.code (live guard wired).
func (it *intg) installClient(t *testing.T, client exchanges.PrivateClient) {
	t.Helper()
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: client}, nil,
		Config{Name: "preparer", AllowLiveExecution: true, ExecutionMode: "live",
			Guard: live.NewGuard(it.store.DB(), clock.NewSystem(), nil), Recovery: fastRecovery(), FinalStatusDelay: 5 * time.Millisecond})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
}

// PR20 round-7 executor tests: malformed mutating rows are finalized (never stranded), and
// every stale mutating IN_FLIGHT request reaches a terminal decision (never IN_FLIGHT forever).

// seedRawMutation inserts a mutating request row directly (bypassing Enqueue), with optional
// NULL order/cycle, in a given status. Returns the request id.
func (it *intg) seedRawMutation(t *testing.T, typ string, cycleID, orderID *int64, status string, inflightAgo time.Duration) int64 {
	t.Helper()
	var inflight any
	if inflightAgo > 0 {
		inflight = time.Now().Add(-inflightAgo)
	}
	res, err := it.db.Exec(`INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, symbol, request_type, status, payload, timeout_ms, max_retries, inflight_at, idempotency_key)
		VALUES (?, ?, ?, 'X/IRT', ?, ?, '{}', 1000, 5, ?, ?)`,
		it.exID, cycleID, orderID, typ, status, inflight, "raw_"+typ+"_"+time.Now().Format("150405.000000000"))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id
}

// TestSweepMalformedMutationFinalizes: a QUEUED PLACE_ORDER with valid cycle_id and NULL
// order_id is finalized DEAD, its cycle → NEEDS_RECONCILE, lock HELD.
func TestSweepMalformedMutationFinalizes(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, _, _ := it.seedBuyCycle(t, "0.5")
	req := it.seedRawMutation(t, "PLACE_ORDER", &cyc, nil, "QUEUED", 0)

	it.exec.sweepMalformedMutations(it.ctx, 50)

	if s := reqStatus(t, it.db, req); s != "DEAD" {
		t.Errorf("malformed PLACE_ORDER = %s, want DEAD", s)
	}
	if it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Errorf("cycle = %s, want NEEDS_RECONCILE", it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (held — no order to prove zero exposure)", it.lockStateByCycle(cyc))
	}
	if got := placeCount(it); got != 0 {
		t.Errorf("PlaceOrder called %d times for a malformed row, want 0", got)
	}
}

// TestStaleMutationWithNullOrderFinalized: a stale IN_FLIGHT PLACE_ORDER with NULL order_id must
// not stay IN_FLIGHT — it goes DEAD, cycle NEEDS_RECONCILE, lock HELD.
func TestStaleMutationWithNullOrderFinalized(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, _, _ := it.seedBuyCycle(t, "0.5")
	req := it.seedRawMutation(t, "PLACE_ORDER", &cyc, nil, "IN_FLIGHT", time.Hour)

	// A NULL-order row has no order to key on → it is handled by the malformed sweep, not the
	// order-JOINed stale-recovery query. Run the production sweep sequence (round 9 #1).
	it.recoverySweeps(it.ctx)

	if s := reqStatus(t, it.db, req); s != "DEAD" {
		t.Errorf("stale NULL-order PLACE_ORDER = %s, want DEAD (not IN_FLIGHT forever)", s)
	}
	if it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Errorf("cycle = %s, want NEEDS_RECONCILE", it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE", it.lockStateByCycle(cyc))
	}
}

// TestStaleCancelWithNullCycleDeterministic: a stale IN_FLIGHT CANCEL with NULL cycle_id is
// deterministically finalized (request DEAD) — no silent return leaves it IN_FLIGHT.
func TestStaleCancelWithNullCycleDeterministic(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	req := it.seedRawMutation(t, "CANCEL_ORDER", nil, nil, "IN_FLIGHT", time.Hour)
	// NULL order AND NULL cycle → no order to JOIN → handled by the malformed sweep (round 9 #1).
	it.recoverySweeps(it.ctx)
	if s := reqStatus(t, it.db, req); s != "DEAD" {
		t.Errorf("stale NULL-cycle CANCEL = %s, want DEAD (deterministic, no silent return)", s)
	}
}

// TestFinalizeStaleConservative: the finalization primitive used when orderRecoveryInfo is
// unavailable (ErrNoRows) or the recovery hard limit expires — DEAD + cycle NEEDS_RECONCILE +
// lock HELD, and only from IN_FLIGHT.
func TestFinalizeStaleConservative(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, _, _ := it.seedBuyCycle(t, "0.5")
	req := it.seedRawMutation(t, "PLACE_ORDER", &cyc, nil, "IN_FLIGHT", time.Hour)

	it.exec.finalizeStaleConservative(it.ctx, req, &cyc, "order recovery info unavailable")

	if s := reqStatus(t, it.db, req); s != "DEAD" {
		t.Errorf("request = %s, want DEAD", s)
	}
	if it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Errorf("cycle = %s, want NEEDS_RECONCILE", it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (held)", it.lockStateByCycle(cyc))
	}
}

// TestStaleRecoveryExpiredBound: a fresh stale IN_FLIGHT (within the hard limit) is retried, not
// finalized; an old one past the limit is finalized.
func TestStaleRecoveryExpiredBound(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	if it.exec.staleRecoveryExpired(time.Now().Add(-time.Minute)) {
		t.Error("a 1-minute-stale request must NOT be past the recovery hard limit yet")
	}
	if !it.exec.staleRecoveryExpired(time.Now().Add(-24 * time.Hour)) {
		t.Error("a 24-hour-stale request MUST be past the recovery hard limit")
	}
}

func placeCount(it *intg) int32 {
	return atomic.LoadInt32(&it.fake.placeCount)
}

// --- PR20 round-7 #2: two-stage prepare/send via MutationPreparer ---------------------------

// preparerFake wraps fakeClient and implements exchanges.MutationPreparer, so tests can prove
// the token/auth work happens in Prepare (before MarkInFlight) and each network call is paced.
type preparerFake struct {
	*fakeClient
	prepareErr   error
	prepareCount int32
	sendCount    int32
}

func (p *preparerFake) PreparePlace(_ context.Context, _ exec_OrderRequest) (exchanges.PreparedMutation, error) {
	atomic.AddInt32(&p.prepareCount, 1)
	if p.prepareErr != nil {
		return nil, p.prepareErr
	}
	return &preparedFake{p: p}, nil
}
func (p *preparerFake) PrepareCancel(_ context.Context, _ string) (exchanges.PreparedMutation, error) {
	atomic.AddInt32(&p.prepareCount, 1)
	if p.prepareErr != nil {
		return nil, p.prepareErr
	}
	return &preparedFake{p: p, cancel: true}, nil
}

type preparedFake struct {
	p      *preparerFake
	cancel bool
}

func (f *preparedFake) Send(ctx context.Context) (exec_OrderAck, error) {
	atomic.AddInt32(&f.p.sendCount, 1)
	if f.cancel {
		return exec_OrderAck{}, f.p.fakeClient.cancelErr
	}
	if err := ctx.Err(); err != nil {
		return exec_OrderAck{}, err
	}
	return f.p.fakeClient.placeAck, f.p.fakeClient.placeErr
}

// TestPrepareFailureLeavesRequestNotInFlight: a Bitpin-style preparation failure (auth/token,
// definitely-not-sent) happens BEFORE MarkInFlight — the request is never IN_FLIGHT ("maybe
// sent"), the order Send is never called, and no ambiguous probe is created.
func TestPrepareFailureLeavesRequestNotInFlight(t *testing.T) {
	it := setup(t)
	pf := &preparerFake{fakeClient: it.fake, prepareErr: exec_NotSent(errors.New("bitpin token refresh timed out"))}
	it.installClient(t, pf)
	it.liveControls(0, true)
	it.db.Exec("UPDATE exchanges SET live_enabled=1")
	it.db.Exec("UPDATE exchange_markets SET live_enabled=1")
	_, ord, req := it.seedBuyCycle(t, "0.5")
	it.enableAllLive()

	it.exec.handlePlace(it.ctx, it.claimOf(t, req), pf)

	if n := atomic.LoadInt32(&pf.sendCount); n != 0 {
		t.Errorf("order Send called %d times after a prepare failure, want 0", n)
	}
	if s := reqStatus(t, it.db, req); s == "IN_FLIGHT" {
		t.Errorf("request left IN_FLIGHT after a prepare failure — it was definitely not sent")
	}
	if s := reqStatus(t, it.db, req); s != "RETRY_SCHEDULED" {
		t.Errorf("temporary prepare failure = %s, want RETRY_SCHEDULED (requeued, still CLAIMED→retry)", s)
	}
	if it.probeCount(t, ord) != 0 {
		t.Errorf("ambiguous probes = %d, want 0 (definitely not sent)", it.probeCount(t, ord))
	}
}

// TestPreparerWithoutAuthCallPacesOrderOnly (round 8 #3): a preparer that makes NO preparation
// network call (like an in-memory-credential adapter, or Bitpin reusing a fresh cached token) does
// not invoke the pacing hook, so it consumes exactly ONE slot — the order send. Pacing reservations
// equal actual HTTP calls; a preparation method call alone reserves nothing. (The real-Bitpin
// 1-vs-2 total is proven in TestBitpinExecutorPacingMatchesActualHTTPCalls.)
func TestPreparerWithoutAuthCallPacesOrderOnly(t *testing.T) {
	it := setup(t)
	pf := &preparerFake{fakeClient: it.fake}
	pf.placeAck = exec_OrderAck{ExchangeOrderID: "EXT-P", Status: exec_StateOpen}
	const perSec = 4
	interval := time.Second / perSec
	frozen := time.Now().UTC().Truncate(time.Second)
	it.installClient(t, pf)
	it.exec.cfg.ExchangeTuningFor = func(string) (int, time.Duration) { return perSec, 0 }
	it.exec.nowFn = func() time.Time { return frozen }
	it.liveControls(0, true)
	it.db.Exec("UPDATE exchanges SET live_enabled=1")
	it.db.Exec("UPDATE exchange_markets SET live_enabled=1")
	_, _, req := it.seedBuyCycle(t, "0.5")
	it.enableAllLive()

	it.exec.handlePlace(it.ctx, it.claimOf(t, req), pf)

	it.exec.pacers.mu.Lock()
	next := it.exec.pacers.next[it.code]
	it.exec.pacers.mu.Unlock()
	slots := int(next.Sub(frozen) / interval)
	if slots != 1 {
		t.Errorf("a preparer with no auth network call consumed %d pacing slots, want 1 (order send only)", slots)
	}
	if n := atomic.LoadInt32(&pf.prepareCount); n != 1 {
		t.Errorf("PreparePlace called %d times, want 1", n)
	}
	if n := atomic.LoadInt32(&pf.sendCount); n != 1 {
		t.Errorf("Send called %d times, want 1", n)
	}
}
