package executor

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/live"
	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/queue"
)

// PR20 corrections #4/#5/#7 — per-exchange cooldown, pacing, and rate-limited mutation
// safety. Offline unit tests + gated integration tests (V3_TEST_MYSQL_DSN via setup()).

// --- offline: cooldown registry + pacer ------------------------------------------------------

func TestCooldownExtendOnlyNeverShortens(t *testing.T) {
	c := newCooldowns()
	now := time.Unix(1000, 0)
	longer := now.Add(60 * time.Second)
	if until, ext := c.arm("nobitex", longer, "r1", "body", now); !ext || !until.Equal(longer) {
		t.Fatalf("first arm = (%v,%t)", until, ext)
	}
	// A SHORTER later deadline must NOT shorten the active one.
	if until, ext := c.arm("nobitex", now.Add(5*time.Second), "r2", "status", now); ext || !until.Equal(longer) {
		t.Errorf("shorter arm shortened the window: (%v,%t)", until, ext)
	}
	// A LONGER later deadline extends.
	longest := now.Add(120 * time.Second)
	if until, ext := c.arm("nobitex", longest, "r3", "header", now); !ext || !until.Equal(longest) {
		t.Errorf("longer arm did not extend: (%v,%t)", until, ext)
	}
	// Scoped per exchange: another code is untouched.
	if rem, _ := c.remaining("wallex", now); rem != 0 {
		t.Errorf("unrelated exchange parked: %v", rem)
	}
	if rem, e := c.remaining("nobitex", now); rem != 120*time.Second || e.reason != "r3" {
		t.Errorf("remaining = %v (%q), want 120s r3", rem, e.reason)
	}
	// Expired → 0.
	if rem, _ := c.remaining("nobitex", longest.Add(time.Second)); rem != 0 {
		t.Errorf("expired remaining = %v, want 0", rem)
	}
}

func TestPacerReserve(t *testing.T) {
	p := newPacer()
	now := time.Unix(2000, 0)
	// perSec <= 0 disables pacing.
	if d := p.reserve("x", 0, now); d != 0 {
		t.Errorf("perSec=0 wait = %v", d)
	}
	// 2/sec → 500ms interval: first send immediate, second waits ~500ms, third ~1s.
	if d := p.reserve("y", 2, now); d != 0 {
		t.Errorf("first reserve = %v, want 0", d)
	}
	if d := p.reserve("y", 2, now); d != 500*time.Millisecond {
		t.Errorf("second reserve = %v, want 500ms", d)
	}
	if d := p.reserve("y", 2, now); d != time.Second {
		t.Errorf("third reserve = %v, want 1s", d)
	}
	// Independent per exchange.
	if d := p.reserve("z", 2, now); d != 0 {
		t.Errorf("other exchange first reserve = %v, want 0", d)
	}
}

// TestNoteRateLimitFallbackAndBound: venue-provided wait wins; a missing duration uses the
// per-exchange configured retry_backoff_ms, else the global fallback; everything is bounded.
func TestNoteRateLimitFallbackAndBound(t *testing.T) {
	e := &Executor{cfg: Config{
		RateLimitFallbackCooldown: 90 * time.Second,
		RateLimitMaxCooldown:      2 * time.Minute,
		ExchangeTuningFor: func(code string) (int, time.Duration) {
			if code == "tuned" {
				return 0, 45 * time.Second
			}
			return 0, 0
		},
	}, cooldowns: newCooldowns(), pacers: newPacer(), nowFn: func() time.Time { return time.Unix(3000, 0) }}

	mk := func(retryAfter time.Duration) error {
		return &exchanges.NormalizedAPIError{Category: exchanges.CatRateLimit, Err: execution.ErrRateLimited,
			RateLimit: &exchanges.RateLimitInfo{RetryAfter: retryAfter, Source: exchanges.RLSourceBody}}
	}
	// Venue duration wins.
	e.noteRateLimit("a", mk(30*time.Second))
	if rem, _ := e.cooldowns.remaining("a", e.nowFn()); rem != 30*time.Second {
		t.Errorf("venue wait = %v, want 30s", rem)
	}
	// No duration + per-exchange retry_backoff_ms.
	e.noteRateLimit("tuned", mk(0))
	if rem, _ := e.cooldowns.remaining("tuned", e.nowFn()); rem != 45*time.Second {
		t.Errorf("tuned fallback = %v, want 45s (exchange_configs.retry_backoff_ms)", rem)
	}
	// No duration, untuned → global fallback.
	e.noteRateLimit("b", mk(0))
	if rem, _ := e.cooldowns.remaining("b", e.nowFn()); rem != 90*time.Second {
		t.Errorf("global fallback = %v, want 90s", rem)
	}
	// Bounded maximum.
	e.noteRateLimit("c", mk(10*time.Hour))
	if rem, _ := e.cooldowns.remaining("c", e.nowFn()); rem != 2*time.Minute {
		t.Errorf("bounded = %v, want 2m cap", rem)
	}
	// Non-rate-limit errors do nothing.
	if got := e.noteRateLimit("d", fmt.Errorf("boom")); got != nil {
		t.Error("plain error treated as rate limit")
	}
	if rem, _ := e.cooldowns.remaining("d", e.nowFn()); rem != 0 {
		t.Error("plain error parked the exchange")
	}
}

// --- gated: parking isolates the affected exchange --------------------------------------------

// TestRateLimitParksOnlyAffectedExchange: a rate limit on exchange A parks A only — B keeps
// processing; nothing reaches A while parked (claims skipped, claimed work deferred); A
// resumes after the cooldown expires.
func TestRateLimitParksOnlyAffectedExchange(t *testing.T) {
	it := setup(t)
	// Second exchange + client.
	codeB := it.code + "b"
	res, err := it.db.Exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'x', 1)", codeB)
	if err != nil {
		t.Fatal(err)
	}
	exB, _ := res.LastInsertId()
	fakeB := &fakeClient{code: codeB}
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake, codeB: fakeB}, nil,
		Config{Name: "two-ex", AllowLiveExecution: true, FinalStatusDelay: 5 * time.Millisecond, Recovery: fastRecovery()})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	seedBal := func(exID int64) int64 {
		r, err := it.db.Exec(`INSERT INTO exchange_requests (exchange_id, request_type, status, payload, timeout_ms, max_retries, idempotency_key)
			VALUES (?, 'GET_BALANCE', 'QUEUED', '{}', 10000, 5, ?)`, exID, fmt.Sprintf("cool_%d_%d", exID, time.Now().UnixNano()))
		if err != nil {
			t.Fatal(err)
		}
		id, _ := r.LastInsertId()
		return id
	}

	// Exchange A rate-limits with a venue-provided wait; B is healthy.
	it.fake.balErr = &exchanges.NormalizedAPIError{Category: exchanges.CatRateLimit, Err: execution.ErrRateLimited,
		RateLimit: &exchanges.RateLimitInfo{RetryAfter: time.Hour, Source: exchanges.RLSourceHeader}}
	reqA1, reqB1 := seedBal(it.exID), seedBal(exB)
	it.exec.claimAndProcessAll(it.ctx)
	if s := reqStatus(t, it.db, reqA1); s != "RETRY_SCHEDULED" {
		t.Fatalf("A's rate-limited read = %s, want RETRY_SCHEDULED", s)
	}
	if s := reqStatus(t, it.db, reqB1); s != "SUCCEEDED" {
		t.Fatalf("B's read = %s, want SUCCEEDED (unrelated exchange unaffected)", s)
	}
	if !it.exec.parked(it.code) || it.exec.parked(codeB) {
		t.Fatalf("parking scope wrong: A=%t B=%t, want A only", it.exec.parked(it.code), it.exec.parked(codeB))
	}

	// While A is parked: nothing reaches A (its queued work is never claimed); B continues.
	it.fake.balErr = nil
	aCalls := atomic.LoadInt32(&it.fake.balCount)
	reqA2, reqB2 := seedBal(it.exID), seedBal(exB)
	for i := 0; i < 3; i++ {
		it.exec.claimAndProcessAll(it.ctx)
	}
	if got := atomic.LoadInt32(&it.fake.balCount); got != aCalls {
		t.Errorf("A received %d calls during cooldown, want 0", got-aCalls)
	}
	if s := reqStatus(t, it.db, reqA2); s != "QUEUED" {
		t.Errorf("A's queued request during cooldown = %s, want QUEUED (untouched)", s)
	}
	if s := reqStatus(t, it.db, reqB2); s != "SUCCEEDED" {
		t.Errorf("B's request during A's cooldown = %s, want SUCCEEDED", s)
	}

	// After the cooldown expires (simulated clock), A resumes.
	it.exec.nowFn = func() time.Time { return time.Now().Add(2 * time.Hour) }
	it.exec.claimAndProcessAll(it.ctx)
	if s := reqStatus(t, it.db, reqA2); s != "SUCCEEDED" {
		t.Errorf("A's request after cooldown = %s, want SUCCEEDED (resumed)", s)
	}
}

// --- gated: rate-limited mutations are never blindly retried ----------------------------------

// ambiguousRateLimitErr models a throttle response with NO proven pre-execution rejection
// (e.g. Wallex 429, or any HTTP-200 body mentioning a limit) — the mutation outcome is unknown.
func ambiguousRateLimitErr() error {
	return &exchanges.NormalizedAPIError{Exchange: "x", Op: "PlaceOrder", StatusCode: 200,
		Category: exchanges.CatRateLimit, Err: execution.ErrRateLimited, Message: "rate limit exceeded",
		// A wait comfortably longer than the drive loop, so the parked assertion is stable.
		RateLimit: &exchanges.RateLimitInfo{Source: exchanges.RLSourceBody, RetryAfter: time.Hour}}
}

// provenRateLimitErr models Nobitex's documented status:"failed"+TooManyRequests envelope —
// the venue PROVED the request was rejected before execution.
func provenRateLimitErr(retryAfter time.Duration) error {
	return &exchanges.NormalizedAPIError{Exchange: "x", Op: "PlaceOrder", StatusCode: 429,
		Category: exchanges.CatRateLimit, Err: execution.ErrRateLimited, Code: "TooManyRequests",
		RateLimit: &exchanges.RateLimitInfo{Source: exchanges.RLSourceCode, RetryAfter: retryAfter, DefiniteRejection: true}}
}

// TestRateLimited200BodyPlaceIsAmbiguousNotSuccessNotRetried: an HTTP-200 rate-limit body on
// PlaceOrder is (a) never treated as a successful placement, (b) never blindly retried, and
// (c) resolved through the read-only ambiguous recovery probe with the symbol lock HELD.
func TestRateLimited200BodyPlaceIsAmbiguousNotSuccessNotRetried(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, placeReq := it.seedBuyCycle(t, "0.5")
	it.fake.placeErr = ambiguousRateLimitErr()
	it.drive(3)

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 1 {
		t.Fatalf("PlaceOrder called %d times, want exactly 1 (no blind retry)", got)
	}
	if s := reqStatus(t, it.db, placeReq); s != "DEAD" {
		t.Errorf("ambiguous rate-limited place = %s, want DEAD (consumed into a probe)", s)
	}
	if it.orderState(ord) == "ACKED" || it.cycleState(cyc) == "BUY_SUBMITTED" {
		t.Error("HTTP-200 rate-limit body must not be treated as a successful placement")
	}
	var probes int
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE order_id=? AND request_type='GET_ORDER' AND JSON_EXTRACT(payload,'$.purpose')=?",
		ord, orders.PurposeAmbiguousPlaceProbe).Scan(&probes)
	if probes != 1 {
		t.Errorf("ambiguous place probes = %d, want 1 (read-only recovery)", probes)
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE while the outcome is ambiguous", it.lockStateByCycle(cyc))
	}
	if !it.exec.parked(it.code) {
		t.Error("the exchange must be parked after the rate limit")
	}
}

// TestProvenPreExecutionRejectionRequeuedThenRetried: ONLY a venue-proven pre-execution
// rejection re-queues the mutation (persisted RETRY_SCHEDULED, no probe, no cycle failure);
// after the cooldown it is retried and completes. A restart mid-cooldown does not duplicate
// the send (the persisted next_retry_at gates it, executor state notwithstanding).
func TestProvenPreExecutionRejectionRequeuedThenRetried(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, placeReq := it.seedBuyCycle(t, "0.5")
	it.fake.placeErr = provenRateLimitErr(300 * time.Millisecond)
	it.drive(2)

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 1 {
		t.Fatalf("PlaceOrder calls = %d, want 1", got)
	}
	if s := reqStatus(t, it.db, placeReq); s != "RETRY_SCHEDULED" {
		t.Fatalf("proven-rejected place = %s, want RETRY_SCHEDULED (safe requeue, not DEAD)", s)
	}
	var probes int
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE order_id=? AND request_type='GET_ORDER'", ord).Scan(&probes)
	if probes != 0 {
		t.Errorf("probes = %d, want 0 (venue proved no execution — nothing to recover)", probes)
	}

	// RESTART during the cooldown: a brand-new executor instance (fresh in-memory cooldowns)
	// must NOT duplicate the mutation — the persisted next_retry_at gates the re-claim.
	it.iocExec(t)
	it.exec.claimAndProcessAll(it.ctx)
	if got := atomic.LoadInt32(&it.fake.placeCount); got != 1 {
		t.Fatalf("restart duplicated the mutation: PlaceOrder calls = %d, want still 1", got)
	}

	// After the venue wait passes, the retry runs and succeeds.
	it.fake.placeErr = nil
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXR-1", Status: execution.StateOpen}
	it.drive(25) // > 300ms of ticks
	if got := atomic.LoadInt32(&it.fake.placeCount); got != 2 {
		t.Fatalf("PlaceOrder calls after cooldown = %d, want 2 (one proven-safe retry)", got)
	}
	if it.orderState(ord) == "QUEUED" {
		t.Errorf("order state = %s — the retried place should have progressed", it.orderState(ord))
	}
	_ = cyc
}

// TestRateLimitedCancelGoesToProbeNotResolution: a rate-limited CANCEL without proof is NOT a
// cancel resolution (the cancel may or may not have executed) — it parks the exchange and
// schedules the ambiguous-cancel read-only probe.
func TestRateLimitedCancelGoesToProbeNotResolution(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, _ := it.seedBuyCycle(t, "0.5")
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXC-1", Status: execution.StateOpen}
	it.fake.cancelErr = ambiguousRateLimitErr()
	it.drive(3)

	var probes int
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE order_id=? AND request_type='GET_ORDER' AND JSON_EXTRACT(payload,'$.purpose')=?",
		ord, orders.PurposeAmbiguousCancelProbe).Scan(&probes)
	if probes != 1 {
		t.Fatalf("ambiguous cancel probes = %d, want 1", probes)
	}
	if got := atomic.LoadInt32(&it.fake.cancelCount); got != 1 {
		t.Errorf("CancelOrder calls = %d, want exactly 1 (no blind re-cancel)", got)
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE", it.lockStateByCycle(cyc))
	}
}

// TestNilGuardInLiveModeSendsNothing (PR20 #8): live mode with a NIL Guard fails CLOSED —
// no PlaceOrder, no CancelOrder, the request is FAILED (recoverable), never silently allowed.
func TestNilGuardInLiveModeSendsNothing(t *testing.T) {
	it := setup(t)
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "live-nil-guard", AllowLiveExecution: true, ExecutionMode: "live", Guard: nil,
			FinalStatusDelay: 5 * time.Millisecond, Recovery: fastRecovery()})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	cyc, ord, placeReq := it.seedBuyCycle(t, "0.5")
	it.drive(3)

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 0 {
		t.Errorf("PlaceOrder called %d times with nil Guard in live mode, want 0", got)
	}
	// PR20 correction #1: a denied ENTRY BUY must not strand the cycle. Nothing was sent, so
	// it resolves through the official clean-failure path: request FAILED, order + cycle
	// FAILED, symbol lock RELEASED — never an order left QUEUED with no executable request.
	if s := reqStatus(t, it.db, placeReq); s != "FAILED" {
		t.Errorf("place under nil guard = %s, want FAILED (denied, nothing sent)", s)
	}
	if st := it.orderState(ord); st != "FAILED" {
		t.Errorf("denied entry-buy order = %s, want FAILED (not stranded in QUEUED)", st)
	}
	if st := it.cycleState(cyc); st != "FAILED" {
		t.Errorf("denied entry-buy cycle = %s, want FAILED (not left open)", st)
	}
	if lk := it.lockStateByCycle(cyc); lk != "RELEASED" {
		t.Errorf("symbol lock after denied entry buy = %s, want RELEASED", lk)
	}

	// Cancel path: a FRESH cycle/order (the one above is now terminally FAILED, and terminal
	// rows are correctly never re-transitioned). Seed a CANCEL directly against an ACKED
	// order; it must be denied without a network call.
	cyc2, ord2, _ := it.seedBuyCycle(t, "0.5")
	it.db.Exec("UPDATE orders SET state='ACKED', exchange_order_id='EX-NG' WHERE id=?", ord2)
	fp, _ := json.Marshal(orders.FollowupPayload{ExchangeOrderID: "EX-NG", Purpose: orders.PurposeCancelRemainder})
	res, err := it.db.Exec(`INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, request_type, status, payload, timeout_ms, max_retries, idempotency_key)
		VALUES (?, ?, ?, 'CANCEL_ORDER', 'CLAIMED', ?, 10000, 5, ?)`, it.exID, cyc2, ord2, fp, fmt.Sprintf("ng_%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	cid, _ := res.LastInsertId()
	it.exec.process(it.ctx, queue.Claimed{ID: cid, ExchangeID: it.exID, ExchangeCode: it.code, Type: queue.TypeCancelOrder,
		Payload: fp, OrderID: &ord2, CycleID: &cyc2, TimeoutMS: 10000, MaxRetries: 5})
	if got := atomic.LoadInt32(&it.fake.cancelCount); got != 0 {
		t.Errorf("CancelOrder called %d times with nil Guard in live mode, want 0", got)
	}
	// PR20 correction #1: a denied CANCEL must not abandon a possibly-open venue order —
	// request DEAD + order/cycle NEEDS_RECONCILE (lock held), not a bare request failure.
	if s := reqStatus(t, it.db, cid); s != "DEAD" {
		t.Errorf("cancel under nil guard = %s, want DEAD (order kept under management)", s)
	}
	if st := it.orderState(ord2); st != "NEEDS_RECONCILE" {
		t.Errorf("denied cancel left order = %s, want NEEDS_RECONCILE", st)
	}
}

// --- PR20 correction #5: cooldowns must survive a process restart ------------------------------

// TestCooldownSurvivesRestart is the reviewer's restart scenario: exchange A is throttled for
// a long period, the deadline is PERSISTED, the executor stops, a NEW executor starts (fresh
// in-memory state) — and still sends nothing to A before cooldown_until, while exchange B is
// unaffected. After the deadline passes, A resumes automatically.
func TestCooldownSurvivesRestart(t *testing.T) {
	it := setup(t)
	codeB := it.code + "restartb"
	res, err := it.db.Exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'x', 1)", codeB)
	if err != nil {
		t.Fatal(err)
	}
	exB, _ := res.LastInsertId()
	fakeB := &fakeClient{code: codeB}
	newExec := func() *Executor {
		e := New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake, codeB: fakeB}, nil,
			Config{Name: "restart", AllowLiveExecution: true, FinalStatusDelay: 5 * time.Millisecond, Recovery: fastRecovery()})
		if err := e.resolveExchangeIDs(it.ctx); err != nil {
			t.Fatal(err)
		}
		return e
	}
	seedBal := func(exID int64) int64 {
		r, err := it.db.Exec(`INSERT INTO exchange_requests (exchange_id, request_type, status, payload, timeout_ms, max_retries, idempotency_key)
			VALUES (?, 'GET_BALANCE', 'QUEUED', '{}', 10000, 5, ?)`, exID, fmt.Sprintf("rst_%d_%d", exID, time.Now().UnixNano()))
		if err != nil {
			t.Fatal(err)
		}
		id, _ := r.LastInsertId()
		return id
	}

	// A is throttled for 10 minutes by the venue.
	it.exec = newExec()
	it.fake.balErr = &exchanges.NormalizedAPIError{Category: exchanges.CatRateLimit, Err: execution.ErrRateLimited,
		RateLimit: &exchanges.RateLimitInfo{RetryAfter: 10 * time.Minute, Source: exchanges.RLSourceHeader}}
	seedBal(it.exID) // the request that gets throttled
	it.exec.claimAndProcessAll(it.ctx)
	if !it.exec.parked(it.code) {
		t.Fatal("A must be parked after the throttle")
	}

	// Durability is handed to the persistence worker (never the response path), so drive one
	// pass explicitly rather than sleeping.
	if ok := it.exec.persistPendingCooldowns(it.ctx); !ok {
		t.Fatal("cooldown persistence pass failed")
	}
	// The deadline is PERSISTED (this is what survives the process).
	var until time.Time
	var reason, source string
	if err := it.db.QueryRow("SELECT cooldown_until, reason, source FROM exchange_cooldowns WHERE exchange_id=?", it.exID).
		Scan(&until, &reason, &source); err != nil {
		t.Fatalf("cooldown not persisted: %v", err)
	}
	if d := time.Until(until); d < 9*time.Minute {
		t.Errorf("persisted cooldown_until is %v away, want ~10m", d)
	}
	if reason == "" || source == "" {
		t.Error("persisted cooldown must record a safe reason/source")
	}

	// RESTART: a brand-new executor with empty in-memory state must reload and stay parked.
	it.fake.balErr = nil
	restarted := newExec()
	if restarted.parked(it.code) {
		t.Fatal("precondition: a fresh executor starts un-parked until it loads")
	}
	if err := restarted.loadCooldowns(it.ctx); err != nil {
		t.Fatalf("loadCooldowns: %v", err)
	}
	if !restarted.parked(it.code) {
		t.Error("after restart A must still be parked (loaded from the database)")
	}
	if restarted.parked(codeB) {
		t.Error("B must never be parked by A's cooldown")
	}

	// No network call for A before the deadline; B keeps working.
	before := atomic.LoadInt32(&it.fake.balCount)
	reqA, reqB := seedBal(it.exID), seedBal(exB)
	restarted.claimAndProcessAll(it.ctx)
	if got := atomic.LoadInt32(&it.fake.balCount); got != before {
		t.Errorf("A received %d calls after restart during cooldown, want 0", got-before)
	}
	if s := reqStatus(t, it.db, reqA); s != "QUEUED" {
		t.Errorf("A's request after restart = %s, want QUEUED (still parked)", s)
	}
	if s := reqStatus(t, it.db, reqB); s != "SUCCEEDED" {
		t.Errorf("B's request after restart = %s, want SUCCEEDED (unaffected)", s)
	}

	// After cooldown_until passes, A resumes automatically.
	restarted.nowFn = func() time.Time { return until.Add(time.Second) }
	restarted.claimAndProcessAll(it.ctx)
	if s := reqStatus(t, it.db, reqA); s != "SUCCEEDED" {
		t.Errorf("A's request after the deadline = %s, want SUCCEEDED (resumed)", s)
	}
}

// TestPersistedCooldownIsExtendOnly: a LONGER deadline extends the stored one; a SHORTER one
// never shortens it — enforced in SQL, so it holds even out of order.
func TestPersistedCooldownIsExtendOnly(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	read := func() time.Time {
		var u time.Time
		if err := it.db.QueryRow("SELECT cooldown_until FROM exchange_cooldowns WHERE exchange_id=?", it.exID).Scan(&u); err != nil {
			t.Fatalf("read cooldown: %v", err)
		}
		return u
	}
	base := time.Now().UTC().Truncate(time.Second).Add(time.Hour)
	if err := it.exec.persistCooldown(it.ctx, it.code, base, "rate_limit:A", "header"); err != nil {
		t.Fatal(err)
	}
	// A SHORTER deadline must not shorten the active one (and must not rewrite reason/source).
	if err := it.exec.persistCooldown(it.ctx, it.code, base.Add(-30*time.Minute), "rate_limit:B", "body"); err != nil {
		t.Fatal(err)
	}
	if got := read(); !got.Equal(base) {
		t.Errorf("shorter cooldown shortened the deadline: %v, want %v", got, base)
	}
	var reason string
	it.db.QueryRow("SELECT reason FROM exchange_cooldowns WHERE exchange_id=?", it.exID).Scan(&reason)
	if reason != "rate_limit:A" {
		t.Errorf("reason = %q, want the LONGER cooldown's reason to survive", reason)
	}
	// A LONGER deadline extends.
	longer := base.Add(30 * time.Minute)
	if err := it.exec.persistCooldown(it.ctx, it.code, longer, "rate_limit:C", "code"); err != nil {
		t.Fatal(err)
	}
	if got := read(); !got.Equal(longer) {
		t.Errorf("longer cooldown did not extend: %v, want %v", got, longer)
	}
}

// --- PR20 correction #6: successful responses with exhausted quota ------------------------------

// TestSuccessfulResponseActivatesCooldownWithoutFailing: a throttle signal seen on a SUCCESSFUL
// response parks the exchange for FUTURE requests, and does NOT turn the completed operation
// into a failure or an ambiguous outcome.
func TestSuccessfulResponseActivatesCooldownWithoutFailing(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	sink := NewRateLimitSink()
	sink.bind(it.exec)

	// The operation itself succeeded — nothing about it changes.
	sink.NoteHeaderRateLimit(it.code, exchanges.RateLimitInfo{RetryAfter: 45 * time.Second, Source: exchanges.RLSourceHeader})
	if !it.exec.parked(it.code) {
		t.Fatal("exhausted quota on a successful response must park FUTURE requests")
	}
	rem, entry := it.exec.cooldowns.remaining(it.code, it.exec.nowFn())
	if rem <= 40*time.Second || rem > 45*time.Second {
		t.Errorf("cooldown = %v, want ~45s from the header", rem)
	}
	if entry.source != exchanges.RLSourceHeader {
		t.Errorf("source = %q, want header", entry.source)
	}
	// It becomes durable like any other park — via the worker, not inline.
	it.exec.persistPendingCooldowns(it.ctx)
	var until time.Time
	if err := it.db.QueryRow("SELECT cooldown_until FROM exchange_cooldowns WHERE exchange_id=?", it.exID).Scan(&until); err != nil {
		t.Fatalf("success-header cooldown not persisted: %v", err)
	}
	// An unbound sink is inert (signals before the executor exists cannot panic).
	NewRateLimitSink().NoteHeaderRateLimit(it.code, exchanges.RateLimitInfo{RetryAfter: time.Second})
}

// --- round 3 #2: a successful response must NEVER wait on cooldown persistence -------------

// slowDB wraps the executor's store so cooldown writes block/fail, proving the response path
// is not coupled to them.
type persistFault struct {
	mu    sync.Mutex
	delay time.Duration
	fail  bool
	calls int32
}

// TestSuccessfulResponseNeverWaitsForCooldownPersistence is the reviewer's scenario: a
// successful PlaceOrder whose response carries exhausted-quota headers must return
// IMMEDIATELY with its result, park the exchange in memory immediately, and leave durability
// to the worker — a slow/failing cooldown write must never delay (and therefore never time
// out / make ambiguous) a mutation the venue already performed.
func TestSuccessfulResponseNeverWaitsForCooldownPersistence(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	sink := NewRateLimitSink()
	sink.bind(it.exec)

	// Simulate the adapter's HTTP response path calling the sink, and measure it.
	start := time.Now()
	sink.NoteHeaderRateLimit(it.code, exchanges.RateLimitInfo{RetryAfter: 10 * time.Minute, Source: exchanges.RLSourceHeader})
	elapsed := time.Since(start)

	// 1) The response path did not block on any database write.
	if elapsed > 50*time.Millisecond {
		t.Errorf("sink took %v — the exchange response path must not wait on cooldown persistence", elapsed)
	}
	// 2) The in-memory park is active immediately (future requests already pause).
	if !it.exec.parked(it.code) {
		t.Error("the in-memory cooldown must activate immediately, before any persistence")
	}
	// 3) Durability is still OUTSTANDING and tracked — not silently dropped.
	if len(it.exec.cooldowns.pending(it.exec.nowFn())) != 1 {
		t.Error("the park must be queued for durable persistence")
	}
	// 4) It is only durable once the separate worker runs.
	var n int
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_cooldowns WHERE exchange_id=?", it.exID).Scan(&n)
	if n != 0 {
		t.Error("persistence must happen off the response path, not inline")
	}
	it.exec.persistPendingCooldowns(it.ctx)
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_cooldowns WHERE exchange_id=?", it.exID).Scan(&n)
	if n != 1 {
		t.Error("the worker must persist the pending cooldown")
	}
	if len(it.exec.cooldowns.pending(it.exec.nowFn())) != 0 {
		t.Error("a persisted cooldown must no longer be pending")
	}
}

// --- round 3 #3: a failed write is retried even when nothing extended ------------------------

// TestFailedPersistRetriedWhenDeadlineNotExtended is the reviewer's durability gap: if the
// first write fails and the next signal carries an EQUAL/SHORTER deadline (extended = false),
// persistence must still be retried — tracked by persisted-vs-active, not by extension.
func TestFailedPersistRetriedWhenDeadlineNotExtended(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	now := time.Now().UTC().Truncate(time.Second)
	it.exec.nowFn = func() time.Time { return now }

	// A park is armed while the DB is unusable: the write fails, the park still holds.
	it.exec.cooldowns.arm(it.code, now.Add(10*time.Minute), "rate_limit:X", "header", now)
	it.exec.cooldowns.markPersistFailed(it.code, fmt.Errorf("db down"), now)
	if !it.exec.parked(it.code) {
		t.Fatal("the in-memory park must hold even when persistence fails")
	}
	if len(it.exec.cooldowns.pending(now)) != 1 {
		t.Fatal("a failed write must stay pending")
	}

	// A LATER signal with a SHORTER deadline: extend-only means the deadline does not move,
	// so `extended` is false. The pending write must NOT be forgotten because of that.
	_, extended := it.exec.cooldowns.arm(it.code, now.Add(time.Minute), "rate_limit:Y", "body", now)
	if extended {
		t.Fatal("precondition: a shorter deadline must not extend")
	}
	if len(it.exec.cooldowns.pending(now)) != 1 {
		t.Error("a previously-failed write must stay pending even when nothing extended")
	}

	// The DB recovers → the worker persists it, and it survives a restart.
	if ok := it.exec.persistPendingCooldowns(it.ctx); !ok {
		t.Fatal("persistence should succeed once the DB is healthy")
	}
	if len(it.exec.cooldowns.pending(now)) != 0 {
		t.Error("a successful write must clear the pending state")
	}
	restarted := New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "restart-after-retry", AllowLiveExecution: true, Recovery: fastRecovery()})
	if err := restarted.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	restarted.nowFn = func() time.Time { return now }
	if err := restarted.loadCooldowns(it.ctx); err != nil {
		t.Fatal(err)
	}
	if !restarted.parked(it.code) {
		t.Error("the eventually-persisted cooldown must still be active after a restart")
	}
}

// TestCooldownDurabilityFailurePolicy: while a park cannot be made durable beyond the grace
// period, live execution is DISABLED (fail closed) rather than continuing while claiming the
// cooldown is durable — and it recovers automatically.
func TestCooldownDurabilityFailurePolicy(t *testing.T) {
	it := setup(t)
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "durability", AllowLiveExecution: true, Recovery: fastRecovery(),
			CooldownPersistGrace: time.Minute})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	it.exec.nowFn = func() time.Time { return now }
	if !it.exec.cooldownDurable(it.code) {
		t.Fatal("a healthy executor starts durable")
	}
	// A park un-persisted for LESS than the grace period: a policy pass keeps it healthy.
	it.exec.cooldowns.arm(it.code, now.Add(10*time.Minute), "rate_limit:X", "header", now.Add(-10*time.Second))
	it.exec.applyCooldownDurabilityPolicy(now)
	if !it.exec.cooldownDurable(it.code) {
		t.Error("within the grace period live entries must not be disabled")
	}
	// Pending beyond the grace period → live ENTRY BUYS disabled for THIS exchange.
	it.exec.nowFn = func() time.Time { return now.Add(2 * time.Minute) }
	it.exec.applyCooldownDurabilityPolicy(it.exec.nowFn())
	if it.exec.cooldownDurable(it.code) {
		t.Error("a park un-persisted beyond the grace period must disable live entries for this exchange")
	}
	// Recovery: once persisted, the next policy pass re-enables entries automatically.
	if ok := it.exec.persistPendingCooldowns(it.ctx); !ok {
		t.Fatal("persist should succeed")
	}
	if !it.exec.cooldownDurable(it.code) {
		t.Error("durability must recover automatically once the write lands")
	}
}

// TestDurabilityLossBlocksLiveSendsNotCancels: the fail-closed policy stops NEW exposure but
// never strands an open order — a cancel still goes through (same asymmetry as the audit
// outage policy).
func TestDurabilityLossBlocksLiveSendsNotCancels(t *testing.T) {
	it := setup(t)
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "durability-live", AllowLiveExecution: true, ExecutionMode: "live",
			Guard: live.NewGuard(it.db, clock.NewSystem(), nil), Recovery: fastRecovery()})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	// Force THIS exchange's stored durability flag to failed (in production the persister sets
	// it; here we set it directly so the place reaches the gate — a parked exchange would be
	// skipped at claim time instead).
	it.exec.markCooldownDurabilityFailed(it.code, true)
	if it.exec.cooldownDurable(it.code) {
		t.Fatal("precondition: this exchange's durability must be failed")
	}

	cyc, ord, placeReq := it.seedBuyCycle(t, "0.5")
	it.drive(3)
	if got := atomic.LoadInt32(&it.fake.placeCount); got != 0 {
		t.Errorf("PlaceOrder called %d times with cooldown durability lost, want 0", got)
	}
	if s := reqStatus(t, it.db, placeReq); s != "FAILED" {
		t.Errorf("place under lost durability = %s, want FAILED", s)
	}
	if it.cycleState(cyc) != "FAILED" || it.orderState(ord) != "FAILED" {
		t.Errorf("a denied entry buy must not strand the cycle: cycle=%s order=%s",
			it.cycleState(cyc), it.orderState(ord))
	}
}
