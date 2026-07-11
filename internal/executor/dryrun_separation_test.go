package executor

import (
	"testing"
	"time"

	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/simexec"
)

// PR19 correction tests: strict dry-run/live queue separation + DB-backed simexec survives
// a restart / a different executor instance. Gated on V3_TEST_MYSQL_DSN (via setup()).

// simExecMode rebuilds the executor with a simexec client AND an explicit execution mode,
// so BOTH separation guards are active: the claim filter and the pre-send check.
func (it *intg) simExecMode(t *testing.T, sc simexec.Scenario, mode string) {
	t.Helper()
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: simexec.New(it.db, it.code, sc)}, nil,
		Config{Name: "exec-" + mode, AllowLiveExecution: true, ExecutionMode: mode, FinalStatusDelay: 10 * time.Millisecond, Recovery: fastRecovery()})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
}

func (it *intg) reqStatus(id int64) string {
	var s string
	it.db.QueryRow("SELECT status FROM exchange_requests WHERE id=?", id).Scan(&s)
	return s
}

func (it *intg) orderExoid(cyc int64) string {
	var s string
	it.db.QueryRow("SELECT COALESCE(exchange_order_id,'') FROM orders WHERE cycle_id=? AND role='entry_buy' ORDER BY id DESC LIMIT 1", cyc).Scan(&s)
	return s
}

// TestDryRunExecutorIgnoresLiveCycleRequest: a dry-run executor never claims/sends a request
// belonging to a real (dry_run=0) cycle — it stays QUEUED and the cycle/order are unchanged.
func TestDryRunExecutorIgnoresLiveCycleRequest(t *testing.T) {
	it := setup(t)
	it.simExecMode(t, simexec.FullFill, "dry_run")
	cyc, ord, reqID := it.seedBuyCycle(t, "0.5") // dry_run defaults to 0 (a LIVE cycle)

	it.drive(4)

	if st := it.reqStatus(reqID); st != "QUEUED" {
		t.Errorf("live request under a dry-run executor = %s, want QUEUED (untouched, not sent)", st)
	}
	if s := it.cycleState(cyc); s != "BUY_REQUEST_QUEUED" {
		t.Errorf("live cycle state = %s, want unchanged BUY_REQUEST_QUEUED", s)
	}
	if s := it.orderState(ord); s != "QUEUED" {
		t.Errorf("live order state = %s, want unchanged QUEUED", s)
	}
}

// TestLiveExecutorIgnoresDryRunCycleRequest: a live executor never claims/sends a dry-run
// cycle's request.
func TestLiveExecutorIgnoresDryRunCycleRequest(t *testing.T) {
	it := setup(t)
	it.simExecMode(t, simexec.FullFill, "live")
	cyc := it.dryCycle(t, "0.5") // dry_run=1
	var reqID int64
	it.db.QueryRow("SELECT id FROM exchange_requests WHERE cycle_id=? AND request_type='PLACE_ORDER' ORDER BY id DESC LIMIT 1", cyc).Scan(&reqID)

	it.drive(4)

	if st := it.reqStatus(reqID); st != "QUEUED" {
		t.Errorf("dry-run request under a live executor = %s, want QUEUED (untouched)", st)
	}
	if s := it.cycleState(cyc); s != "BUY_REQUEST_QUEUED" {
		t.Errorf("dry-run cycle state = %s, want unchanged BUY_REQUEST_QUEUED", s)
	}
}

// TestDryRunExecutorRestartContinues: after the buy is PLACED, a NEW executor instance with a
// FRESH simexec client (as a restart / second instance would build) continues the lifecycle —
// the follow-up GET_ORDER resolves via the persisted sim_exchange_orders, so the dry-run cycle
// reaches BUY_FILLED instead of a spurious NEEDS_RECONCILE (ErrOrderUnknown).
func TestDryRunExecutorRestartContinues(t *testing.T) {
	it := setup(t)
	it.simExecMode(t, simexec.FullFill, "dry_run")
	cyc := it.dryCycle(t, "0.5")

	// Drive until the buy has actually been placed (order has a SIM- exchange_order_id).
	it.drive(3)
	if exoid := it.orderExoid(cyc); exoid == "" {
		t.Fatalf("buy not placed after initial drive (no exchange_order_id); state=%s", it.cycleState(cyc))
	}

	// Simulate a RESTART / different instance: a brand-new executor + brand-new simexec client.
	it.simExecMode(t, simexec.FullFill, "dry_run")
	it.drive(6)

	if s := it.cycleState(cyc); s != "BUY_FILLED" {
		t.Errorf("after restart, dry-run cycle = %s, want BUY_FILLED (persistent sim state, no ErrOrderUnknown)", s)
	}
}
