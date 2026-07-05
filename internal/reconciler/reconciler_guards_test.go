package reconciler

import (
	"fmt"
	"testing"
	"time"

	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/state"
)

// seedSell is seedCycleOrder for an exit_sell order (+ an ACTIVE lock).
func (f *rfix) seedSell(t *testing.T, cycleSt state.CycleState, orderSt state.OrderState, exoid string) (cyc, ord, lock int64) {
	t.Helper()
	rseq++
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), rseq) }
	last := func(r interface{ LastInsertId() (int64, error) }) int64 { id, _ := r.LastInsertId(); return id }
	ex := func(q string, a ...any) interface{ LastInsertId() (int64, error) } {
		r, err := f.db.Exec(q, a...)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	b := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("SB")))
	qa := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("SQ")))
	m := last(ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", u("SM"), b, qa))
	em := last(ex("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, 'X/Y')", f.exID, m, u("SES")))
	cyc = last(ex("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state) VALUES (?, ?, 'X/Y', ?)", em, f.exID, string(cycleSt)))
	var exoidArg any
	if exoid != "" {
		exoidArg = exoid
	}
	ord = last(ex(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, exchange_order_id, state, quantity)
		VALUES (?, ?, ?, 'sell', 'exit_sell', ?, ?, ?, '1')`, cyc, f.exID, em, u("sloc"), exoidArg, string(orderSt)))
	lock = f.seedLock(t, cyc)
	return
}

// TestRejectedNotTreatedAsCleanCancel (PR12 #2) — a buy order the exchange reports REJECTED must
// NOT be advanced to a terminal state that safe-closes the cycle / releases the lock: it is an
// execution anomaly → order+cycle NEEDS_RECONCILE, lock HELD.
func TestRejectedNotTreatedAsCleanCancel(t *testing.T) {
	f := setupR(t)
	cyc, ord := f.seedCycleOrder(t, state.CycleBuySubmitted, state.OrderSubmitted, "EXO-BR")
	lock := f.seedLock(t, cyc)
	f.result("EXO-BR", execution.StateRejected, "0", "EXO-BR", nil)

	rep, err := f.rec.ReconcileStartup(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ordSt(t, f.db, ord) != "NEEDS_RECONCILE" || cycState(t, f.db, cyc) != "NEEDS_RECONCILE" {
		t.Errorf("order/cycle=%s/%s, want NEEDS_RECONCILE (REJECTED is not a clean cancel)", ordSt(t, f.db, ord), cycState(t, f.db, cyc))
	}
	if lockState(t, f.db, lock) != "ACTIVE" {
		t.Errorf("lock=%s, want ACTIVE (REJECTED must not release the lock)", lockState(t, f.db, lock))
	}
	if rep.SafeClosed != 0 {
		t.Errorf("SafeClosed=%d, want 0 (REJECTED must not safe-close)", rep.SafeClosed)
	}
}

// TestSellRejectedKeepsLock (PR12 #2, sell side) — a rejected SELL keeps the lock (the buy leg
// may hold inventory) → order+cycle NEEDS_RECONCILE, lock ACTIVE.
func TestSellRejectedKeepsLock(t *testing.T) {
	f := setupR(t)
	cyc, ord, lock := f.seedSell(t, state.CycleSellSubmitted, state.OrderSubmitted, "EXO-SR")
	f.result("EXO-SR", execution.StateRejected, "0", "EXO-SR", nil)

	if _, err := f.rec.ReconcileStartup(f.ctx); err != nil {
		t.Fatal(err)
	}
	if ordSt(t, f.db, ord) != "NEEDS_RECONCILE" || cycState(t, f.db, cyc) != "NEEDS_RECONCILE" {
		t.Errorf("order/cycle=%s/%s, want NEEDS_RECONCILE", ordSt(t, f.db, ord), cycState(t, f.db, cyc))
	}
	if lockState(t, f.db, lock) != "ACTIVE" {
		t.Errorf("lock=%s, want ACTIVE (sell rejection must NOT release the lock)", lockState(t, f.db, lock))
	}
}

// TestSafeCloseRefusedWithActiveRequest (PR12 #3) — a cycle that looks zero-fill terminal but
// still has an ACTIVE exchange_request must NOT be safe-closed and its lock must NOT be
// released; it goes to NEEDS_RECONCILE.
func TestSafeCloseRefusedWithActiveRequest(t *testing.T) {
	f := setupR(t)
	// CANCELLED (terminal, zero fill) → would normally safe-close; CANCELLED is legal from
	// BUY_SUBMITTED, so the ONLY blocker is the active request.
	cyc, ord := f.seedCycleOrder(t, state.CycleBuySubmitted, state.OrderCancelled, "")
	lock := f.seedLock(t, cyc)
	if _, err := f.db.Exec(`INSERT INTO exchange_requests
		(exchange_id, cycle_id, order_id, request_type, status, payload, idempotency_key)
		VALUES (?, ?, ?, 'GET_ORDER', 'IN_FLIGHT', '{}', ?)`, f.exID, cyc, ord, fmt.Sprintf("act_%d", cyc)); err != nil {
		t.Fatal(err)
	}

	rep, err := f.rec.ReconcileStartup(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cycState(t, f.db, cyc) != "NEEDS_RECONCILE" {
		t.Errorf("cycle=%s, want NEEDS_RECONCILE (active request blocks safe-close)", cycState(t, f.db, cyc))
	}
	if lockState(t, f.db, lock) != "ACTIVE" {
		t.Errorf("lock=%s, want ACTIVE (must not release with an active request)", lockState(t, f.db, lock))
	}
	if rep.SafeClosed != 0 {
		t.Errorf("SafeClosed=%d, want 0 (active request must block safe-close)", rep.SafeClosed)
	}
}

// TestIllegalAdvanceGoesToReconcile (PR12 #4) — when the exchange-reported terminal is illegal
// from the DB state (SUBMITTED→CANCELLED is not allowed), the reconciler must NOT silently
// advance/close; it flags NEEDS_RECONCILE.
func TestIllegalAdvanceGoesToReconcile(t *testing.T) {
	f := setupR(t)
	cyc, ord := f.seedCycleOrder(t, state.CycleBuySubmitted, state.OrderSubmitted, "EXO-IC")
	lock := f.seedLock(t, cyc)
	f.result("EXO-IC", execution.StateCanceled, "0", "EXO-IC", nil) // SUBMITTED->CANCELLED is illegal

	rep, err := f.rec.ReconcileStartup(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ordSt(t, f.db, ord) != "NEEDS_RECONCILE" || cycState(t, f.db, cyc) != "NEEDS_RECONCILE" {
		t.Errorf("order/cycle=%s/%s, want NEEDS_RECONCILE (illegal advance not silently skipped)", ordSt(t, f.db, ord), cycState(t, f.db, cyc))
	}
	if lockState(t, f.db, lock) != "ACTIVE" {
		t.Errorf("lock=%s, want ACTIVE", lockState(t, f.db, lock))
	}
	if rep.SafeClosed != 0 {
		t.Errorf("SafeClosed=%d, want 0 (must not report a clean close for an illegal transition)", rep.SafeClosed)
	}
}

// TestApplyOrderOutcomeDivertsIllegalToReconcile (PR12 #4) — applyOrderOutcome must DIVERT an
// illegal intended advance to NEEDS_RECONCILE (returning diverted=true), never silently return
// nil as if it succeeded.
func TestApplyOrderOutcomeDivertsIllegalToReconcile(t *testing.T) {
	f := setupR(t)
	_, ord := f.seedCycleOrder(t, state.CycleBuySubmitted, state.OrderQueued, "")
	var ver int64
	f.db.QueryRow("SELECT version FROM orders WHERE id=?", ord).Scan(&ver)

	// QUEUED -> FILLED is illegal. Must divert to NEEDS_RECONCILE, not silently skip.
	diverted, err := f.rec.applyOrderOutcome(f.ctx, orderRow{ID: ord, State: state.OrderQueued, Version: ver},
		OrderOutcome{Decision: AdvanceTerminal, TargetState: state.OrderFilled, Reason: "test illegal advance"})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !diverted {
		t.Error("illegal advance must be reported as diverted (not a silent success)")
	}
	if ordSt(t, f.db, ord) != "NEEDS_RECONCILE" {
		t.Errorf("order=%s, want NEEDS_RECONCILE (diverted)", ordSt(t, f.db, ord))
	}
}
