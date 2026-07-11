package executor

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/live"
	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/queue"
	"v3TradeBot/internal/simexec"
	"v3TradeBot/internal/state"
)

// PR19 round 2 — automatic read-only recovery of AMBIGUOUS mutation timeouts, end-to-end
// through the REAL queue/executor/order-processing boundaries with the SIMULATED client. No
// real exchange call ever. Gated on V3_TEST_MYSQL_DSN (via setup()).

// --- ambiguous PLACE timeout recovery ------------------------------------------------------

// TestRecoverPlaceTimeoutAcceptedFull: PlaceOrder times out but the order WAS accepted and fully
// filled. The read-only probe (by client_order_id) discovers it and the cycle reaches BUY_FILLED
// with exactly one recorded fill — no blind resend.
func TestRecoverPlaceTimeoutAcceptedFull(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.PlaceTimeoutAcceptedFull)
	cyc := it.dryCycle(t, "0.5")
	ord := it.orderIDOf(t, cyc)
	it.drive(12)

	if it.cycleState(cyc) != "BUY_FILLED" {
		t.Fatalf("accepted-timeout full = %s, want BUY_FILLED (recovered by client_order_id)", it.cycleState(cyc))
	}
	if n := it.fillCount(ord); n != 1 {
		t.Errorf("fills = %d, want 1 (recovered fill recorded once)", n)
	}
	if p := it.placeReqCount(t, ord); p != 1 {
		t.Errorf("PLACE_ORDER requests = %d, want 1 (never blindly re-placed)", p)
	}
}

// TestRecoverPlaceTimeoutAcceptedOpen: accepted but resting OPEN with zero fill → recovery cancels
// the remainder → clean zero-fill CANCELLED, lock released.
func TestRecoverPlaceTimeoutAcceptedOpen(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.PlaceTimeoutAcceptedOpen)
	cyc := it.dryCycle(t, "0.5")
	it.drive(14)
	if it.cycleState(cyc) != "CANCELLED" {
		t.Fatalf("accepted-timeout open = %s, want CANCELLED (recovered, nothing filled)", it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "RELEASED" {
		t.Errorf("lock = %s, want RELEASED (zero fill)", it.lockStateByCycle(cyc))
	}
}

// TestRecoverPlaceTimeoutAcceptedPartial: accepted with a partial fill → recovery records the
// partial and continues (BUY_PARTIALLY_FILLED), lock held.
func TestRecoverPlaceTimeoutAcceptedPartial(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.PlaceTimeoutAcceptedPartial)
	cyc := it.dryCycle(t, "0.5")
	ord := it.orderIDOf(t, cyc)
	it.drive(14)
	if it.cycleState(cyc) != "BUY_PARTIALLY_FILLED" {
		t.Fatalf("accepted-timeout partial = %s, want BUY_PARTIALLY_FILLED", it.cycleState(cyc))
	}
	var filled string
	it.db.QueryRow("SELECT filled_quantity FROM orders WHERE id=?", ord).Scan(&filled)
	if filled != "0.250000000000000000" {
		t.Errorf("recovered filled_quantity = %s, want 0.25", filled)
	}
}

// TestRecoverPlaceTimeoutNotAccepted (PR19 round 3 #1): the place timed out and the order is not
// found. Because simexec models an UNRELIABLE negative (an accepted order can be briefly invisible),
// a "not found" is NEVER proof of non-placement: after bounded read-only retries the order remains
// unresolved → NEEDS_RECONCILE with the lock HELD (NOT a clean fail, NOT lock-released), and it is
// never blindly re-placed.
func TestRecoverPlaceTimeoutNotAccepted(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.PlaceTimeoutNotAccepted)
	cyc := it.dryCycle(t, "0.5")
	ord := it.orderIDOf(t, cyc)
	it.drive(14)
	if it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Fatalf("not-found-after-timeout = %s, want NEEDS_RECONCILE (not proof of non-placement)", it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (outcome still ambiguous)", it.lockStateByCycle(cyc))
	}
	if p := it.placeReqCount(t, ord); p != 1 {
		t.Errorf("PLACE_ORDER requests = %d, want 1 (never blindly re-placed)", p)
	}
}

// TestRecoverPlaceTimeoutDelayedVisibility (PR19 round 3 #1): an order accepted+filled but INVISIBLE
// on the first probes must NOT be failed on the first "not found" — the recovery keeps probing and
// recovers the fill once the order becomes visible (eventual consistency).
func TestRecoverPlaceTimeoutDelayedVisibility(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.PlaceTimeoutAcceptedFullDelayed)
	cyc := it.dryCycle(t, "0.5")
	ord := it.orderIDOf(t, cyc)
	it.drive(20)
	if it.cycleState(cyc) != "BUY_FILLED" {
		t.Fatalf("delayed-visibility recovery = %s, want BUY_FILLED (recovered after early not-founds)", it.cycleState(cyc))
	}
	if n := it.fillCount(ord); n != 1 {
		t.Errorf("fills = %d, want 1", n)
	}
}

// --- ambiguous CANCEL timeout recovery -----------------------------------------------------

func (it *intg) driveCancelTimeout(t *testing.T, sc simexec.Scenario, wantCycle string) *intg {
	t.Helper()
	it.simExec(t, sc)
	cyc := it.dryCycle(t, "0.5")
	it.drive(16)
	if it.cycleState(cyc) != wantCycle {
		t.Fatalf("scenario %s = %s, want %s", sc, it.cycleState(cyc), wantCycle)
	}
	return it
}

// TestRecoverCancelTimeoutButCanceled: the IOC cancel times out but the order WAS canceled with
// zero fill → recovery reads the real state and closes clean (CANCELLED).
func TestRecoverCancelTimeoutButCanceled(t *testing.T) {
	it := setup(t)
	it.driveCancelTimeout(t, simexec.CancelTimeoutButCanceled, "CANCELLED")
}

// TestRecoverCancelTimeoutFilledBeforeCancel: filled before the cancel took → follow the
// filled-order path (BUY_FILLED).
func TestRecoverCancelTimeoutFilledBeforeCancel(t *testing.T) {
	it := setup(t)
	it.driveCancelTimeout(t, simexec.CancelTimeoutFilledBeforeCancel, "BUY_FILLED")
}

// TestRecoverCancelTimeoutPartialThenCanceled: partially filled then remainder canceled →
// record the ACTUAL filled qty and continue (BUY_PARTIALLY_FILLED); sell-manage only that qty.
func TestRecoverCancelTimeoutPartialThenCanceled(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.CancelTimeoutPartialThenCanceled)
	cyc := it.dryCycle(t, "0.5")
	ord := it.orderIDOf(t, cyc)
	it.drive(16)
	if it.cycleState(cyc) != "BUY_PARTIALLY_FILLED" {
		t.Fatalf("cancel-timeout partial = %s, want BUY_PARTIALLY_FILLED", it.cycleState(cyc))
	}
	var filled string
	it.db.QueryRow("SELECT filled_quantity FROM orders WHERE id=?", ord).Scan(&filled)
	if filled != "0.250000000000000000" {
		t.Errorf("recovered filled_quantity = %s, want the actual acquired 0.25", filled)
	}
}

// TestRecoverCancelTimeoutStillOpenBoundedThenReconcile: the cancel timed out AND the order is
// still open every probe. The executor re-cancels (proven, not blind) a BOUNDED number of times,
// then gives up to NEEDS_RECONCILE — never an infinite loop.
func TestRecoverCancelTimeoutStillOpenBoundedThenReconcile(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.CancelTimeoutStillOpen)
	cyc := it.dryCycle(t, "0.5")
	ord := it.orderIDOf(t, cyc)
	it.drive(24)
	if it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Fatalf("still-open cancel timeout = %s, want NEEDS_RECONCILE after bounded re-cancels", it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (unresolved)", it.lockStateByCycle(cyc))
	}
	// Bounded: the number of CANCEL_ORDER requests is capped (initial IOC cancel + a bounded
	// number of proven re-cancels), NOT unbounded.
	var cancels int
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE order_id=? AND request_type='CANCEL_ORDER'", ord).Scan(&cancels)
	if cancels < 2 || cancels > fastRecovery().MaxAttempts+2 {
		t.Errorf("CANCEL_ORDER requests = %d, want between 2 and %d (bounded re-cancel)", cancels, fastRecovery().MaxAttempts+2)
	}
}

// fastRecovery is the compressed recovery window tests use so bounded backoff resolves in
// milliseconds instead of seconds.
func fastRecovery() RecoveryConfig {
	return RecoveryConfig{MaxAttempts: 4, InitialDelay: 5 * time.Millisecond, MaxDelay: 20 * time.Millisecond, TotalTimeout: 5 * time.Second}
}

// --- restart / multi-instance --------------------------------------------------------------

// TestRecoveryContinuesAcrossInstances: instance A's place times out (probe scheduled, place
// DEAD); a BRAND-NEW executor instance B (a restart / second process) resolves the recovery from
// the persisted queue + sim state, reaching BUY_FILLED. Recovery is not tied to the process that
// started it.
func TestRecoveryContinuesAcrossInstances(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.PlaceTimeoutAcceptedFull) // instance A
	cyc := it.dryCycle(t, "0.5")
	ord := it.orderIDOf(t, cyc)

	it.drive(2) // A: place times out → schedule probe, DEAD the place
	if it.placeReqStatusForOrder(t, ord) != "DEAD" {
		t.Fatalf("place not consumed by instance A; state=%s", it.cycleState(cyc))
	}

	// Instance B: a fresh executor + fresh simexec client (as a restart / 2nd process builds).
	it.simExec(t, simexec.PlaceTimeoutAcceptedFull)
	it.drive(12)
	if it.cycleState(cyc) != "BUY_FILLED" {
		t.Errorf("after restart, recovery reached %s, want BUY_FILLED (persistent, cross-instance)", it.cycleState(cyc))
	}
	if n := it.fillCount(ord); n != 1 {
		t.Errorf("fills = %d, want 1 (no double-apply across instances)", n)
	}
}

// TestRecoveredFillNotDoubleApplied: re-running the final-status GET_ORDER after a recovered
// fill does NOT insert a second fill row (idempotent — deterministic exchange_fill_id + UNIQUE).
func TestRecoveredFillNotDoubleApplied(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.PlaceTimeoutAcceptedFull)
	cyc := it.dryCycle(t, "0.5")
	ord := it.orderIDOf(t, cyc)
	it.drive(12)
	if it.cycleState(cyc) != "BUY_FILLED" || it.fillCount(ord) != 1 {
		t.Fatalf("precondition: cyc=%s fills=%d, want BUY_FILLED/1", it.cycleState(cyc), it.fillCount(ord))
	}
	// Re-enqueue a duplicate final-status read (as a concurrent/re-run recovery would) and drive.
	var exoid string
	it.db.QueryRow("SELECT COALESCE(exchange_order_id,'') FROM orders WHERE id=?", ord).Scan(&exoid)
	payload, _ := json.Marshal(orders.FollowupPayload{ExchangeOrderID: exoid, Purpose: orders.PurposeFinalStatus, CycleID: cyc})
	it.db.Exec(`INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, request_type, status, payload, idempotency_key)
		VALUES (?, ?, ?, 'GET_ORDER', 'QUEUED', ?, ?)`, it.exID, cyc, ord, payload, fmt.Sprintf("dupfinal_%d", time.Now().UnixNano()))
	it.drive(4)
	if n := it.fillCount(ord); n != 1 {
		t.Errorf("fills after duplicate final-status = %d, want 1 (idempotent, no double-apply)", n)
	}
}

// --- fail-closed final live guard (#6) -----------------------------------------------------

func (it *intg) liveFaultedExec(t *testing.T, fault func() error) {
	t.Helper()
	guard := live.NewGuard(it.store.DB(), clock.NewSystem(), nil)
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "live-fault", AllowLiveExecution: true, ExecutionMode: "live", Guard: guard, cycleModeFault: fault, Recovery: fastRecovery()})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
}

// TestLiveGuardFailsClosedOnCycleModeError: a LIVE executor that cannot confirm a cycle's mode
// (the dry_run lookup errors) sends NOTHING — no PlaceOrder, no CancelOrder — and the request
// stays CLAIMED (recoverable), never falsely marked sent/failed.
func TestLiveGuardFailsClosedOnCycleModeError(t *testing.T) {
	it := setup(t)
	it.liveControls(0, true)
	it.liveFaultedExec(t, func() error { return errors.New("simulated cycle-mode DB error") })
	orderID, cycleID := it.seedBuyOrder(t, string(state.CycleBuyRequestQueued), string(state.OrderQueued))

	// PLACE must not be sent.
	cP := it.seedPlace(t, orderID, cycleID, orders.BuyIntentPayload{Side: "buy", OrderType: "limit", SimulatedIOC: true, IntendedPrice: "100", IntendedQuantity: "1", LocalClientOrderID: "loc"})
	it.exec.process(it.ctx, cP)
	if n := atomic.LoadInt32(&it.fake.placeCount); n != 0 {
		t.Errorf("PlaceOrder called %d times when cycle mode unconfirmable, want 0", n)
	}
	if s := reqStatus(t, it.db, cP.ID); s != "CLAIMED" {
		t.Errorf("place request = %s, want CLAIMED (recoverable, not falsely sent/failed)", s)
	}

	// CANCEL must not be sent either.
	fp := orders.FollowupPayload{ExchangeOrderID: "EX1"}
	payload, _ := json.Marshal(fp)
	res, err := it.db.Exec(`INSERT INTO exchange_requests
		(exchange_id, cycle_id, order_id, request_type, priority, status, payload, timeout_ms, max_retries, idempotency_key)
		VALUES (?, ?, ?, 'CANCEL_ORDER', 50, 'CLAIMED', ?, 10000, 5, ?)`,
		it.exID, cycleID, orderID, payload, fmt.Sprintf("idemcxl_%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	cid, _ := res.LastInsertId()
	cC := queue.Claimed{ID: cid, ExchangeID: it.exID, ExchangeCode: it.code, Type: queue.TypeCancelOrder,
		Payload: payload, OrderID: &orderID, CycleID: &cycleID, TimeoutMS: 10000, MaxRetries: 5}
	it.exec.process(it.ctx, cC)
	if n := atomic.LoadInt32(&it.fake.cancelCount); n != 0 {
		t.Errorf("CancelOrder called %d times when cycle mode unconfirmable, want 0", n)
	}
	if s := reqStatus(t, it.db, cC.ID); s != "CLAIMED" {
		t.Errorf("cancel request = %s, want CLAIMED (recoverable)", s)
	}
}

// --- small helpers -------------------------------------------------------------------------

func (it *intg) orderIDOf(t *testing.T, cyc int64) int64 {
	t.Helper()
	var id int64
	if err := it.db.QueryRow("SELECT id FROM orders WHERE cycle_id=? AND role='entry_buy' ORDER BY id DESC LIMIT 1", cyc).Scan(&id); err != nil {
		t.Fatalf("orderIDOf: %v", err)
	}
	return id
}

func (it *intg) placeReqCount(t *testing.T, orderID int64) int {
	t.Helper()
	var n int
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE order_id=? AND request_type='PLACE_ORDER'", orderID).Scan(&n)
	return n
}

func (it *intg) placeReqStatusForOrder(t *testing.T, orderID int64) string {
	t.Helper()
	var s string
	it.db.QueryRow("SELECT status FROM exchange_requests WHERE order_id=? AND request_type='PLACE_ORDER' ORDER BY id ASC LIMIT 1", orderID).Scan(&s)
	return s
}

// --- #4: exact client_order_id_sent persisted BEFORE the network call ----------------------

// TestClientOrderIDSentPersistedBeforeSend: the adapter-normalized client id is COMMITTED to
// client_order_id_sent before PlaceOrder is called, and the SAME value is what the adapter sends.
func TestClientOrderIDSentPersistedBeforeSend(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	orderID, cycleID := it.seedBuyOrder(t, string(state.CycleBuyRequestQueued), string(state.OrderQueued))
	it.fake.normalizeCID = func(s string) string { // simulate a venue that truncates to 8 chars
		if len(s) > 8 {
			return s[:8]
		}
		return s
	}
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXN", Status: execution.StateOpen}
	var persistedAtSend string
	it.fake.onPlace = func() {
		it.db.QueryRow("SELECT COALESCE(client_order_id_sent,'') FROM orders WHERE id=?", orderID).Scan(&persistedAtSend)
	}
	c := it.seedPlace(t, orderID, cycleID, orders.BuyIntentPayload{Side: "buy", OrderType: "limit", SimulatedIOC: true,
		IntendedPrice: "100", IntendedQuantity: "1", LocalClientOrderID: "verylongclientid-1234567890", MakerWaitBeforeCancelMs: 2000})
	it.exec.process(it.ctx, c)

	want := "verylong" // truncated to 8
	if persistedAtSend != want {
		t.Errorf("client_order_id_sent AT the moment of send = %q, want %q (persisted+committed BEFORE the network call)", persistedAtSend, want)
	}
	if it.fake.lastSentCID != want {
		t.Errorf("PlaceOrder received client id %q, want the normalized %q (send exactly the persisted value)", it.fake.lastSentCID, want)
	}
}

// --- #1: provably-not-placed ONLY on a reliable negative -----------------------------------

func (it *intg) faultlessExecWithCaps(t *testing.T, caps exchanges.Capabilities) {
	t.Helper()
	it.fake.capsOverride = &caps
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "caps", AllowLiveExecution: true, FinalStatusDelay: 5 * time.Millisecond, Recovery: fastRecovery()})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
}

// TestProvablyNotPlacedOnlyOnReliableNegative: a place probe that keeps getting ErrOrderUnknown
// resolves to a CLEAN FAIL (no exposure, lock released) ONLY because the venue declares a RELIABLE
// negative (ReliableNotFound); the contrasting unreliable-venue case is covered by
// TestRecoverPlaceTimeoutNotAccepted (→ NEEDS_RECONCILE).
func TestProvablyNotPlacedOnlyOnReliableNegative(t *testing.T) {
	it := setup(t)
	it.faultlessExecWithCaps(t, exchanges.Capabilities{PlaceOrder: true, LookupByClientOrderID: true, FetchByOrderID: true, ReliableNotFound: true})
	cyc, ord, _ := it.seedBuyCycle(t, "0.5")
	it.fake.placeErr = execution.ErrAckTimeout // ambiguous place
	it.fake.getErr = execution.ErrOrderUnknown // and the order is never found (reliable negative)
	it.drive(16)
	if it.cycleState(cyc) != "FAILED" {
		t.Fatalf("reliable-negative not-placed = %s, want FAILED (provably not placed, no exposure)", it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "RELEASED" {
		t.Errorf("lock = %s, want RELEASED", it.lockStateByCycle(cyc))
	}
	if p := it.placeReqCount(t, ord); p != 1 {
		t.Errorf("PLACE_ORDER requests = %d, want 1 (never re-placed)", p)
	}
}

// --- #4: a recovered order whose immutable fields disagree is NEVER attached ---------------

// TestRecoveredMismatchNotAttached (#4): a probe that finds an order whose SIDE disagrees must not
// attach it — it goes to NEEDS_RECONCILE. (Quantity is NOT an exact-identity check — venues round
// and partial fills are smaller — so a quantity difference alone would NOT reject.)
func TestRecoveredMismatchNotAttached(t *testing.T) {
	it := setup(t)
	it.faultlessExecWithCaps(t, exchanges.Capabilities{PlaceOrder: true, LookupByClientOrderID: true, FetchByOrderID: true})
	cyc, ord, _ := it.seedBuyCycle(t, "0.5") // a BUY order
	it.fake.placeErr = execution.ErrAckTimeout
	// The probe "finds" an order — but it is a SELL (wrong side) → must never be attached.
	it.fake.getStatus = execution.OrderStatus{Status: execution.StateFilled, ExchangeOrderID: "EXX",
		Side: "sell", IntendedQty: decimal.RequireFromString("0.5"), FilledQty: decimal.RequireFromString("0.5"),
		AvgPrice: decimal.RequireFromString("100")}
	it.drive(10)
	if it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Fatalf("wrong-side recovered order = %s, want NEEDS_RECONCILE (never attach a different order)", it.cycleState(cyc))
	}
	if it.fillCount(ord) != 0 {
		t.Errorf("fills = %d, want 0 (mismatched order not recorded)", it.fillCount(ord))
	}
}

// --- #3: crash-after-send recovery of stale mutating IN_FLIGHT ------------------------------

// simPlaced records an order at the simulator (as the exchange would after accepting a place) and
// stamps client_order_id_sent, then marks the PLACE request stale-IN_FLIGHT — modelling a CRASH
// after the exchange accepted the send but before the response was handled.
func (it *intg) crashDuringPlace(t *testing.T, sc simexec.Scenario, cyc, ord, placeReq int64) {
	t.Helper()
	var loc string
	it.db.QueryRow("SELECT local_client_order_id FROM orders WHERE id=?", ord).Scan(&loc)
	it.db.Exec("UPDATE orders SET client_order_id_sent=? WHERE id=?", loc, ord)
	sim := simexec.New(it.db, it.code, sc)
	if _, err := sim.PlaceOrder(it.ctx, execution.OrderRequest{ClientOrderID: loc, Symbol: "X/IRT", Side: "buy",
		Quantity: decimal.RequireFromString("0.5"), LimitPrice: decimal.RequireFromString("100"), OrderType: "limit"}); err != nil && !errors.Is(err, execution.ErrAckTimeout) {
		t.Fatalf("sim place: %v", err)
	}
	it.db.Exec("UPDATE exchange_requests SET status='IN_FLIGHT', inflight_at=NOW(6)-INTERVAL 3600 SECOND, claimed_by='crashed' WHERE id=?", placeReq)
}

// TestCrashStalePlaceRecoveredByProbe: a PLACE left stuck IN_FLIGHT by a crash (order accepted at
// the exchange) is recovered read-only — a probe by client_order_id finds the order and completes
// it WITHOUT a duplicate placement.
func TestCrashStalePlaceRecoveredByProbe(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.FullFill)
	cyc, ord, placeReq := it.seedBuyCycle(t, "0.5")
	it.db.Exec("UPDATE cycles SET dry_run=1 WHERE id=?", cyc)
	it.crashDuringPlace(t, simexec.FullFill, cyc, ord, placeReq)

	// Crash recovery converts the stale mutation to a READ-ONLY probe (never a resend).
	it.exec.recoverStaleMutating(it.ctx, 5)
	if s := reqStatus(t, it.db, placeReq); s != "DEAD" {
		t.Fatalf("stale place request = %s, want DEAD (consumed, converted to a probe)", s)
	}
	var probes int
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE order_id=? AND request_type='GET_ORDER' AND JSON_EXTRACT(payload,'$.purpose')=?", ord, orders.PurposeAmbiguousPlaceProbe).Scan(&probes)
	if probes != 1 {
		t.Fatalf("scheduled place-probe = %d, want 1", probes)
	}
	it.drive(12)
	if it.cycleState(cyc) != "BUY_FILLED" {
		t.Errorf("after crash recovery, cycle = %s, want BUY_FILLED", it.cycleState(cyc))
	}
	if it.placeReqCount(t, ord) != 1 {
		t.Errorf("PLACE_ORDER requests = %d, want 1 (recovered without duplicate placement)", it.placeReqCount(t, ord))
	}
	if it.fillCount(ord) != 1 {
		t.Errorf("fills = %d, want 1", it.fillCount(ord))
	}
}

// TestStaleRecoveryAndSweepNoRace (PR19 round 4 #1): TWO executor instances run stale-mutation
// recovery while the generic sweeper runs concurrently. The atomic FOR-UPDATE-SKIP-LOCKED claim +
// idempotency key guarantee EXACTLY ONE recovery probe, the mutation is never re-sent, and the
// order is not prematurely reconciled — the single probe still drives it to completion.
func TestStaleRecoveryAndSweepNoRace(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.FullFill) // instance A
	cyc, ord, placeReq := it.seedBuyCycle(t, "0.5")
	it.db.Exec("UPDATE cycles SET dry_run=1 WHERE id=?", cyc)
	it.crashDuringPlace(t, simexec.FullFill, cyc, ord, placeReq) // order accepted at sim; place stuck IN_FLIGHT

	// A second executor instance (same DB, fresh simexec client) + the generic sweeper.
	execB := New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: simexec.New(it.db, it.code, simexec.FullFill)}, nil,
		Config{Name: "instB", AllowLiveExecution: true, FinalStatusDelay: 5 * time.Millisecond, Recovery: fastRecovery()})
	if err := execB.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); it.exec.recoverStaleMutating(it.ctx, 0) }()
	go func() { defer wg.Done(); execB.recoverStaleMutating(it.ctx, 0) }()
	go func() { defer wg.Done(); it.q.SweepStuck(it.ctx, 0) }()
	wg.Wait()

	var probes int
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE order_id=? AND request_type='GET_ORDER' AND JSON_EXTRACT(payload,'$.purpose')=?", ord, orders.PurposeAmbiguousPlaceProbe).Scan(&probes)
	if probes != 1 {
		t.Errorf("recovery probes = %d, want EXACTLY 1 (no race)", probes)
	}
	if s := reqStatus(t, it.db, placeReq); s != "DEAD" {
		t.Errorf("place request = %s, want DEAD (consumed once, never re-sent)", s)
	}
	if os := ordState(t, it.db, ord); os == "NEEDS_RECONCILE" {
		t.Error("order prematurely moved to NEEDS_RECONCILE (recovery should own it)")
	}
	// The single probe still continues the state machine to completion.
	it.drive(12)
	if it.cycleState(cyc) != "BUY_FILLED" {
		t.Errorf("after recovery cycle = %s, want BUY_FILLED", it.cycleState(cyc))
	}
}

// TestCrashStaleCancelRecoveredByProbe: a CANCEL left stuck IN_FLIGHT by a crash is recovered
// read-only (a probe by exchange_order_id), never assuming the cancel took and never re-cancelling
// blindly.
func TestCrashStaleCancelRecoveredByProbe(t *testing.T) {
	it := setup(t)
	it.simExec(t, simexec.FullFill)
	cyc, ord, placeReq := it.seedBuyCycle(t, "0.5")
	it.db.Exec("UPDATE cycles SET dry_run=1 WHERE id=?", cyc)

	// Drive the place so the order is ACKED with a SIM exchange id and the IOC cancel is scheduled.
	it.drive(2)
	var exoid string
	it.db.QueryRow("SELECT COALESCE(exchange_order_id,'') FROM orders WHERE id=?", ord).Scan(&exoid)
	if exoid == "" {
		t.Fatalf("buy not placed; state=%s", it.cycleState(cyc))
	}
	_ = placeReq
	// Mark the scheduled IOC cancel as stale-IN_FLIGHT (crash mid-cancel).
	res, _ := it.db.Exec("UPDATE exchange_requests SET status='IN_FLIGHT', inflight_at=NOW(6)-INTERVAL 3600 SECOND, claimed_by='crashed' WHERE order_id=? AND request_type='CANCEL_ORDER'", ord)
	if n, _ := res.RowsAffected(); n == 0 {
		t.Fatal("no CANCEL_ORDER to crash")
	}
	it.exec.recoverStaleMutating(it.ctx, 5)
	var probes int
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE order_id=? AND request_type='GET_ORDER' AND JSON_EXTRACT(payload,'$.purpose')=?", ord, orders.PurposeAmbiguousCancelProbe).Scan(&probes)
	if probes != 1 {
		t.Fatalf("scheduled cancel-probe = %d, want 1", probes)
	}
	it.drive(12)
	if it.cycleState(cyc) != "BUY_FILLED" {
		t.Errorf("after cancel crash recovery, cycle = %s, want BUY_FILLED (order was filled)", it.cycleState(cyc))
	}
}

// --- PR19 round 4 correction: transient cancel-probe failures use the RECOVERY WINDOW --------

// transientCancelSetup drives a buy place + ambiguous IOC cancel with a fake client whose
// GetOrder always fails with a RETRYABLE error, so every recovery probe hits the transient
// branch. Returns (cycle, order) after the initial place+cancel have been processed.
func (it *intg) transientCancelSetup(t *testing.T, rec RecoveryConfig, perEx map[string]RecoveryConfig) (int64, int64) {
	t.Helper()
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "transient", AllowLiveExecution: true, FinalStatusDelay: 5 * time.Millisecond,
			Recovery: rec, RecoveryPerExchange: perEx})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	cyc, ord, _ := it.seedBuyCycle(t, "0.5")
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXT-1", Status: execution.StateOpen}
	it.fake.cancelErr = execution.ErrAckTimeout // ambiguous cancel -> recovery probe scheduled
	it.fake.getErr = execution.ErrRateLimited   // every probe lookup fails TRANSIENTLY
	return cyc, ord
}

func (it *intg) cancelProbeRows(t *testing.T, ord int64) (total, dead, succeeded, queueRetried int) {
	t.Helper()
	rows, err := it.db.Query(`SELECT status, retry_count FROM exchange_requests
		WHERE order_id=? AND request_type='GET_ORDER' AND JSON_EXTRACT(payload,'$.purpose')=?`,
		ord, orders.PurposeAmbiguousCancelProbe)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var st string
		var rc int
		if err := rows.Scan(&st, &rc); err != nil {
			t.Fatal(err)
		}
		total++
		switch st {
		case "DEAD":
			dead++
		case "SUCCEEDED":
			succeeded++
		}
		if rc > 0 {
			queueRetried++
		}
	}
	return
}

// TestTransientCancelProbeUsesRecoveryWindowNotQueueRetries: transient GetOrder failures on a
// cancel probe consume attempts of the per-exchange RECOVERY window (fresh persisted probe rows
// with an attempt-scoped key), NEVER the generic queue retry policy (retry_count stays 0 on
// every probe). Exhausting MaxAttempts atomically yields a DEAD probe + NEEDS_RECONCILE with
// the lock HELD, and no additional CANCEL_ORDER is emitted for a failed read-only probe.
func TestTransientCancelProbeUsesRecoveryWindowNotQueueRetries(t *testing.T) {
	it := setup(t)
	cyc, ord := it.transientCancelSetup(t, fastRecovery(), nil) // MaxAttempts=4
	it.drive(20)

	if it.orderState(ord) != "NEEDS_RECONCILE" || it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Fatalf("states = ord:%s cyc:%s, want NEEDS_RECONCILE (window exhausted)", it.orderState(ord), it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE throughout and after exhaustion", it.lockStateByCycle(cyc))
	}
	total, dead, succeeded, queueRetried := it.cancelProbeRows(t, ord)
	// Window semantics: probe a0..a<MaxAttempts> = MaxAttempts+1 rows; the last is the one that
	// exhausted the window (DEAD, atomically with NEEDS_RECONCILE); every earlier probe was
	// consumed (SUCCEEDED) when it scheduled its successor.
	if total != fastRecovery().MaxAttempts+1 {
		t.Errorf("probe rows = %d, want %d (window attempts, not in-row queue retries)", total, fastRecovery().MaxAttempts+1)
	}
	if dead != 1 || succeeded != total-1 {
		t.Errorf("probe statuses dead=%d succeeded=%d of %d, want exactly 1 DEAD (the atomic exhaustion) and the rest SUCCEEDED", dead, succeeded, total)
	}
	if queueRetried != 0 {
		t.Errorf("%d probe rows have retry_count > 0 — transient failures must NOT use the queue retry policy", queueRetried)
	}
	// A failed read-only probe must never emit another CANCEL_ORDER.
	var cancels int
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE order_id=? AND request_type='CANCEL_ORDER'", ord).Scan(&cancels)
	if cancels != 1 {
		t.Errorf("CANCEL_ORDER rows = %d, want 1 (the original IOC cancel only)", cancels)
	}
}

// TestTransientCancelProbeTotalTimeout: the wall-clock TotalTimeout bound (per-exchange PARTIAL
// override — everything else inherits the configured global) exhausts the window even when
// MaxAttempts is far from spent; exhaustion is the same atomic DEAD + NEEDS_RECONCILE.
func TestTransientCancelProbeTotalTimeout(t *testing.T) {
	it := setup(t)
	perEx := map[string]RecoveryConfig{it.code: {TotalTimeout: time.Millisecond}} // partial: only TotalTimeout
	cyc, ord := it.transientCancelSetup(t, fastRecovery(), perEx)
	it.drive(12)

	if it.orderState(ord) != "NEEDS_RECONCILE" || it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Fatalf("states = ord:%s cyc:%s, want NEEDS_RECONCILE (TotalTimeout exhausted)", it.orderState(ord), it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE", it.lockStateByCycle(cyc))
	}
	total, dead, _, _ := it.cancelProbeRows(t, ord)
	if total != 1 || dead != 1 {
		t.Errorf("probe rows = %d (dead %d), want exactly 1 DEAD probe (first probe already beyond the 1ms window)", total, dead)
	}
}

// TestRecoveryConfigPartialOverrideInheritsConfiguredGlobal (unit): the executor-side merge
// inherits UNSET override fields from the CONFIGURED global window — never hard defaults —
// and probeBackoff respects the configured cap (jittered in [cap/2, cap] once saturated).
func TestRecoveryConfigPartialOverrideInheritsConfiguredGlobal(t *testing.T) {
	global := RecoveryConfig{MaxAttempts: 9, InitialDelay: 2 * time.Second, MaxDelay: 8 * time.Second, TotalTimeout: 10 * time.Minute}
	e := &Executor{cfg: Config{
		Recovery:            global,
		RecoveryPerExchange: map[string]RecoveryConfig{"slowex": {TotalTimeout: 30 * time.Minute}}, // partial
	}}
	rc := e.recoveryConfig("slowex")
	if rc.TotalTimeout != 30*time.Minute {
		t.Errorf("override TotalTimeout = %v, want 30m", rc.TotalTimeout)
	}
	if rc.MaxAttempts != 9 || rc.InitialDelay != 2*time.Second || rc.MaxDelay != 8*time.Second {
		t.Errorf("partial override inherited %d/%v/%v, want the CONFIGURED global 9/2s/8s (not hard defaults 6/1s/30s)", rc.MaxAttempts, rc.InitialDelay, rc.MaxDelay)
	}
	if other := e.recoveryConfig("unlisted"); other != global {
		t.Errorf("unlisted exchange = %+v, want the global config verbatim", other)
	}
	// Backoff cap: deep attempts must stay within (cap/2, cap] — the configured 8s, not 30s.
	for attempt := 1; attempt <= 12; attempt++ {
		d := e.probeBackoff("slowex", attempt)
		if d > global.MaxDelay {
			t.Fatalf("attempt %d backoff %v exceeds the configured cap %v", attempt, d, global.MaxDelay)
		}
		if d <= 0 {
			t.Fatalf("attempt %d backoff %v must be positive", attempt, d)
		}
	}
	if d := e.probeBackoff("slowex", 10); d < global.MaxDelay/2 {
		t.Errorf("saturated backoff %v below jitter floor %v", d, global.MaxDelay/2)
	}
	// First attempt is jittered around InitialDelay: within (initial/2, initial].
	if d := e.probeBackoff("slowex", 1); d < global.InitialDelay/2 || d > global.InitialDelay {
		t.Errorf("attempt-1 backoff %v outside (initial/2, initial] = (%v, %v]", d, global.InitialDelay/2, global.InitialDelay)
	}
}
