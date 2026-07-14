package executor

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/live"
	"v3TradeBot/internal/queue"
)

// PR20 round-4 executor tests: final pre-send guard (#2), exchange timeout at the network
// boundary (#3), per-exchange cooldown durability (#4), graceful-shutdown flush (#5), and the
// normalized client-order-id flow (#6).

// liveFakeExec wires the REAL live.Guard with the controllable fakeClient so tests can drive
// PlaceOrder counts, the client-id normalizer, and per-request timing.
func (it *intg) liveFakeExec(t *testing.T, cfg Config) {
	t.Helper()
	cfg.Name = "live-fake"
	cfg.AllowLiveExecution = true
	cfg.ExecutionMode = "live"
	cfg.Guard = live.NewGuard(it.store.DB(), clock.NewSystem(), nil)
	if cfg.Recovery == (RecoveryConfig{}) {
		cfg.Recovery = fastRecovery()
	}
	if cfg.FinalStatusDelay == 0 {
		cfg.FinalStatusDelay = 5 * time.Millisecond
	}
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil, cfg)
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	it.liveControls(0, true) // kill switch off, creds present
	it.db.Exec("UPDATE exchanges SET live_enabled=1 WHERE id=?", it.exID)
	it.db.Exec("UPDATE exchange_markets SET live_enabled=1 WHERE exchange_id=?", it.exID)
}

// enableAllLive flips live_enabled on every exchange + market (seedBuyCycle creates markets
// with live_enabled=0; the guard requires 1). Call AFTER seeding.
func (it *intg) enableAllLive() {
	it.db.Exec("UPDATE exchanges SET live_enabled=1")
	it.db.Exec("UPDATE exchange_markets SET live_enabled=1")
}

// claimOfCode is claimOf with an explicit exchange code (for multi-exchange tests).
func (it *intg) claimOfCode(t *testing.T, id int64, code string) queue.Claimed {
	c := it.claimOf(t, id)
	c.ExchangeCode = code
	return c
}

// burnPace consumes the exchange's first pacing slot so the NEXT send must wait ~one interval,
// giving a deterministic window to change state or observe the timeout.
func (it *intg) burnPace(perSec int) { it.exec.pacers.reserve(it.code, perSec, it.exec.nowFn()) }

// --- #2: the FINAL guard runs after pacing and catches state that went stale mid-pacing ------

// TestFinalGuardCatchesKillSwitchDuringPacing: the initial guard allows, then the kill switch
// engages while the request waits in the pacer; the final guard (after pacing, before
// MarkInFlight) must deny — zero PlaceOrder, and the buy is resolved with no stranded cycle.
func TestFinalGuardCatchesKillSwitchDuringPacing(t *testing.T) {
	it := setup(t)
	it.liveFakeExec(t, Config{ExchangeTuningFor: func(string) (int, time.Duration) { return 1, 0 }})
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXT-1", Status: execution.StateOpen}
	cyc, ord, _ := it.seedBuyCycle(t, "0.5")
	it.enableAllLive()
	it.burnPace(1) // next send waits ~1s

	done := make(chan struct{})
	go func() { defer close(done); it.exec.handlePlace(it.ctx, it.claimOf(t, it.placeReqID(t, ord)), it.fake) }()

	// Wait until the handler has passed the EARLY guard (client_order_id_sent persisted) and is
	// now in pacing, then engage the kill switch.
	it.waitForSentCID(t, ord)
	it.db.Exec("UPDATE live_controls SET kill_switch=1 WHERE id=1")
	<-done

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 0 {
		t.Errorf("PlaceOrder called %d times after the kill switch engaged during pacing, want 0", got)
	}
	if it.cycleState(cyc) != "FAILED" || it.orderState(ord) != "FAILED" {
		t.Errorf("a buy denied by the final guard must not strand the cycle: cycle=%s order=%s",
			it.cycleState(cyc), it.orderState(ord))
	}
	if it.lockStateByCycle(cyc) != "RELEASED" {
		t.Errorf("symbol lock = %s, want RELEASED", it.lockStateByCycle(cyc))
	}
}

// TestFinalGuardCatchesCycleStateChangeDuringPacing: a DIFFERENT condition (the order's cycle
// leaves a sendable state while pacing) is also caught by the final guard — proving it is a
// full re-check, not just a kill-switch check.
func TestFinalGuardCatchesCycleStateChangeDuringPacing(t *testing.T) {
	it := setup(t)
	it.liveFakeExec(t, Config{ExchangeTuningFor: func(string) (int, time.Duration) { return 1, 0 }})
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXT-2", Status: execution.StateOpen}
	_, ord, _ := it.seedBuyCycle(t, "0.5")
	it.enableAllLive()
	it.burnPace(1)

	done := make(chan struct{})
	go func() { defer close(done); it.exec.handlePlace(it.ctx, it.claimOf(t, it.placeReqID(t, ord)), it.fake) }()
	it.waitForSentCID(t, ord)
	// The order moves out of QUEUED (to ACKED, e.g. a recovery attached a real order) while
	// pacing → the final identity proof must deny AND, since exposure can no longer be
	// disproven, the lock must be HELD with order/cycle NEEDS_RECONCILE (PR20 correction #1).
	cyc := it.cycleOf(t, ord)
	it.db.Exec("UPDATE orders SET state='ACKED' WHERE id=?", ord)
	<-done

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 0 {
		t.Errorf("PlaceOrder called %d times after the order left QUEUED during pacing, want 0", got)
	}
	if it.orderState(ord) != "NEEDS_RECONCILE" || it.cycleState(cyc) != "NEEDS_RECONCILE" {
		t.Errorf("order=%s cycle=%s, want NEEDS_RECONCILE (exposure not disproven)", it.orderState(ord), it.cycleState(cyc))
	}
	if it.lockStateByCycle(cyc) != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (held — exposure could not be disproven)", it.lockStateByCycle(cyc))
	}
}

// cycleOf returns the cycle id of an order.
func (it *intg) cycleOf(t *testing.T, orderID int64) int64 {
	t.Helper()
	var c int64
	if err := it.db.QueryRow("SELECT cycle_id FROM orders WHERE id=?", orderID).Scan(&c); err != nil {
		t.Fatal(err)
	}
	return c
}

// --- #3: the exchange timeout starts AFTER pacing, at the network boundary -------------------

// TestExchangeTimeoutStartsAfterPacing: with a short request timeout and a long pacing wait,
// the request must NOT be marked ambiguous merely because pacing outlasted the timeout — the
// PlaceOrder call gets a FRESH, non-expired deadline created after pacing.
func TestExchangeTimeoutStartsAfterPacing(t *testing.T) {
	it := setup(t)
	it.liveFakeExec(t, Config{ExchangeTuningFor: func(string) (int, time.Duration) { return 1, 0 }})
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXT-3", Status: execution.StateOpen}
	ord, req := it.seedShortTimeoutBuy(t, "0.5", 50) // 50ms exchange timeout
	it.enableAllLive()
	it.burnPace(1) // pacing waits ~1s, far longer than 50ms

	it.exec.handlePlace(it.ctx, it.claimOf(t, req), it.fake)

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 1 {
		t.Fatalf("PlaceOrder called %d times, want 1 (pacing must not consume the exchange timeout)", got)
	}
	if it.fake.lastPlaceCtxErr != nil {
		t.Errorf("PlaceOrder received an already-expired context (%v) — the timeout started before pacing", it.fake.lastPlaceCtxErr)
	}
	if !it.fake.lastPlaceHasDeadline {
		t.Fatal("PlaceOrder should have a deadline")
	}
	if remaining := time.Until(it.fake.lastPlaceDeadline); remaining <= 0 || remaining > 60*time.Millisecond {
		t.Errorf("PlaceOrder deadline remaining = %v, want a fresh ~50ms window created after pacing", remaining)
	}
	if s := reqStatus(t, it.db, req); s == "DEAD" {
		t.Errorf("request = DEAD (ambiguous) — a request delayed only by pacing must not be made ambiguous")
	}
	if it.orderState(ord) == "NEEDS_RECONCILE" {
		t.Error("order pushed to NEEDS_RECONCILE by pacing delay — false ambiguity")
	}
}

// --- #4: cooldown durability failure is isolated per exchange --------------------------------

// TestCooldownDurabilityIsPerExchange: exchange A's durability failure denies A's entry buys
// but leaves exchange B fully operational; A resumes when its durability recovers.
func TestCooldownDurabilityIsPerExchange(t *testing.T) {
	it := setup(t)
	// Second exchange + fake client.
	codeB := it.code + "durb"
	res, err := it.db.Exec("INSERT INTO exchanges (code, name, enabled, live_enabled) VALUES (?, 'x', 1, 1)", codeB)
	if err != nil {
		t.Fatal(err)
	}
	exB, _ := res.LastInsertId()
	fakeB := &fakeClient{code: codeB, placeAck: execution.OrderAck{ExchangeOrderID: "B-1", Status: execution.StateOpen}}
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "A-1", Status: execution.StateOpen}
	guard := live.NewGuard(it.store.DB(), clock.NewSystem(), nil)
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake, codeB: fakeB}, nil,
		Config{Name: "per-ex-dur", AllowLiveExecution: true, ExecutionMode: "live", Guard: guard, Recovery: fastRecovery()})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	it.liveControls(0, true)
	it.db.Exec("UPDATE exchanges SET live_enabled=1")
	it.db.Exec("UPDATE exchange_markets SET live_enabled=1")
	it.db.Exec("INSERT INTO exchange_credentials (exchange_id, label, enabled, status) VALUES (?, 'd', 1, 'active')", exB)

	// Exchange A's cooldown durability is failed; B is healthy.
	it.exec.markCooldownDurabilityFailed(it.code, true)

	_, ordA, reqA := it.seedBuyCycle(t, "0.5")
	ordB, reqB := it.seedBuyCycleOn(t, exB, codeB, "0.5")
	it.enableAllLive()

	it.exec.handlePlace(it.ctx, it.claimOf(t, reqA), it.fake)
	it.exec.handlePlace(it.ctx, it.claimOfCode(t, reqB, codeB), fakeB)

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 0 {
		t.Errorf("exchange A PlaceOrder called %d times with durability failed, want 0", got)
	}
	if s := reqStatus(t, it.db, reqA); s != "FAILED" {
		t.Errorf("A's denied buy = %s, want FAILED", s)
	}
	_ = ordA
	if got := atomic.LoadInt32(&fakeB.placeCount); got != 1 {
		t.Errorf("exchange B PlaceOrder called %d times, want 1 (B must be unaffected by A's outage)", got)
	}
	if st := it.orderState(ordB); st == "QUEUED" || st == "FAILED" {
		t.Errorf("B's order = %s, want it to have progressed", st)
	}

	// A recovers → its next entry buy is allowed.
	it.exec.markCooldownDurabilityFailed(it.code, false)
	_, _, reqA2 := it.seedBuyCycle(t, "0.5")
	it.enableAllLive()
	it.exec.handlePlace(it.ctx, it.claimOf(t, reqA2), it.fake)
	if got := atomic.LoadInt32(&it.fake.placeCount); got != 1 {
		t.Errorf("exchange A PlaceOrder after recovery called %d times, want 1", got)
	}
}

// --- #5: graceful shutdown flushes pending cooldown persistence ------------------------------

// TestGracefulShutdownFlushesCooldowns: a cooldown armed in memory but not yet written by the
// worker is flushed during graceful shutdown, so a restart restores it.
func TestGracefulShutdownFlushesCooldowns(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	sink := NewRateLimitSink()
	sink.bind(it.exec)
	// Arm a park in memory; do NOT run the persister.
	sink.NoteHeaderRateLimit(it.code, exchanges.RateLimitInfo{RetryAfter: 10 * time.Minute, Source: exchanges.RLSourceHeader})
	var n int
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_cooldowns WHERE exchange_id=?", it.exID).Scan(&n)
	if n != 0 {
		t.Fatal("precondition: cooldown must not be persisted yet")
	}

	// Graceful shutdown flush writes it.
	it.exec.flushCooldownsOnShutdown()
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_cooldowns WHERE exchange_id=?", it.exID).Scan(&n)
	if n != 1 {
		t.Fatal("graceful shutdown must flush the pending cooldown")
	}

	// A fresh executor restores it.
	restarted := New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "restarted", AllowLiveExecution: true, Recovery: fastRecovery()})
	if err := restarted.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.loadCooldowns(it.ctx); err != nil {
		t.Fatal(err)
	}
	if !restarted.parked(it.code) {
		t.Error("the flushed cooldown must be active after a restart")
	}
}

// TestGracefulShutdownFlushIsBounded: if the DB is unavailable the flush must NOT hang — it
// returns within roughly the shutdown-flush timeout.
func TestGracefulShutdownFlushIsBounded(t *testing.T) {
	it := setup(t)
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "flush-bounded", Recovery: fastRecovery(), ShutdownFlushTimeout: 300 * time.Millisecond})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	it.exec.nowFn = func() time.Time { return now }
	it.exec.cooldowns.arm(it.code, now.Add(10*time.Minute), "rate_limit:X", "header", now)
	// Make writes fail by hiding the table.
	it.db.Exec("RENAME TABLE exchange_cooldowns TO exchange_cooldowns_hidden")
	defer it.db.Exec("RENAME TABLE exchange_cooldowns_hidden TO exchange_cooldowns")

	start := time.Now()
	it.exec.flushCooldownsOnShutdown()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("bounded shutdown flush took %v — it must not hang when the DB is unavailable", elapsed)
	}
}

// --- #6: the normalized client-order-id is validated, persisted, and sent unchanged ----------

// TestNormalizedClientOrderIDValidatedPersistedSent: when the adapter normalizer changes the
// client id, the FINAL guard sees the normalized value, the DB stores it, and exactly that
// value is sent — with no transformation after the guard.
func TestNormalizedClientOrderIDValidatedPersistedSent(t *testing.T) {
	it := setup(t)
	it.fake.normalizeCID = func(local string) string { return "NORM-" + local }
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EXT-N", Status: execution.StateOpen}
	it.liveFakeExec(t, Config{})
	_, ord, req := it.seedBuyCycle(t, "0.5")
	it.enableAllLive()
	var local string
	it.db.QueryRow("SELECT local_client_order_id FROM orders WHERE id=?", ord).Scan(&local)

	it.exec.handlePlace(it.ctx, it.claimOf(t, req), it.fake)

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 1 {
		t.Fatalf("PlaceOrder count = %d, want 1", got)
	}
	want := "NORM-" + local
	if it.fake.lastSentCID != want {
		t.Errorf("sent client id = %q, want the normalized %q", it.fake.lastSentCID, want)
	}
	var persisted string
	it.db.QueryRow("SELECT client_order_id_sent FROM orders WHERE id=?", ord).Scan(&persisted)
	if persisted != want {
		t.Errorf("persisted client_order_id_sent = %q, want %q", persisted, want)
	}
}

// TestNormalizerEmptyBlocksSend: a normalizer that returns empty must block the send (fail
// closed), with no stranded cycle.
func TestNormalizerEmptyBlocksSend(t *testing.T) {
	it := setup(t)
	it.fake.normalizeCID = func(string) string { return "" }
	it.liveFakeExec(t, Config{})
	cyc, ord, req := it.seedBuyCycle(t, "0.5")
	it.enableAllLive()

	it.exec.handlePlace(it.ctx, it.claimOf(t, req), it.fake)

	if got := atomic.LoadInt32(&it.fake.placeCount); got != 0 {
		t.Errorf("PlaceOrder called %d times with an empty normalized id, want 0", got)
	}
	if it.cycleState(cyc) != "FAILED" || it.orderState(ord) != "FAILED" {
		t.Errorf("an empty-normalized buy must resolve cleanly: cycle=%s order=%s", it.cycleState(cyc), it.orderState(ord))
	}
}

// --- helpers ---------------------------------------------------------------------------------

func (it *intg) placeReqID(t *testing.T, orderID int64) int64 {
	t.Helper()
	var id int64
	if err := it.db.QueryRow("SELECT id FROM exchange_requests WHERE order_id=? AND request_type='PLACE_ORDER' ORDER BY id DESC LIMIT 1", orderID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func (it *intg) waitForSentCID(t *testing.T, orderID int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var s string
		it.db.QueryRow("SELECT COALESCE(client_order_id_sent,'') FROM orders WHERE id=?", orderID).Scan(&s)
		if s != "" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("handler never persisted client_order_id_sent (never reached pacing)")
}

// seedShortTimeoutBuy seeds a buy PLACE_ORDER with a custom exchange timeout (ms).
func (it *intg) seedShortTimeoutBuy(t *testing.T, qty string, timeoutMs int) (orderID, reqID int64) {
	t.Helper()
	_, ord, req := it.seedBuyCycle(t, qty)
	it.db.Exec("UPDATE exchange_requests SET timeout_ms=? WHERE id=?", timeoutMs, req)
	return ord, req
}

// seedBuyCycleOn seeds a live buy cycle/order/request on a specific (already live-enabled)
// exchange, returning the order + request ids.
func (it *intg) seedBuyCycleOn(t *testing.T, exchangeID int64, code, qty string) (orderID, reqID int64) {
	t.Helper()
	seedSeq++
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, exchangeID, seedSeq) }
	last := func(r interface{ LastInsertId() (int64, error) }) int64 { id, _ := r.LastInsertId(); return id }
	ex := func(q string, a ...any) int64 {
		r, err := it.db.Exec(q, a...)
		if err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
		return last(r)
	}
	sym := u("M") + "/IRT"
	b := ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B"))
	qa := ex("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q"))
	m := ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", sym, b, qa)
	em := ex("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol, live_enabled) VALUES (?, ?, ?, ?, 1)", exchangeID, m, u("ES"), sym)
	cyc := ex("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run) VALUES (?, ?, ?, 'BUY_REQUEST_QUEUED', 0)", em, exchangeID, sym)
	ord := ex(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, order_type, limit_price, quantity)
		VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'QUEUED', 'limit', '100', ?)`, cyc, exchangeID, em, u("loc"), qty)
	ex("INSERT INTO symbol_locks (scope, canonical_symbol, cycle_id, expires_at) VALUES (?, ?, ?, NOW(6)+INTERVAL 1 HOUR)", code, sym, cyc)
	payload := mustJSON(map[string]any{"side": "buy", "order_type": "limit", "simulated_ioc": true,
		"intended_price": "100", "intended_quantity": qty, "local_client_order_id": u("loc")})
	req := ex(`INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, symbol, request_type, priority, status, payload, timeout_ms, max_retries, idempotency_key)
		VALUES (?, ?, ?, ?, 'PLACE_ORDER', 50, 'QUEUED', ?, 10000, 5, ?)`, exchangeID, cyc, ord, sym, payload, u("idem"))
	return ord, req
}

var _ = queue.TypePlaceOrder
