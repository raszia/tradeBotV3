package executor

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/queue"
)

// PR20 correction round 11. A GET_ORDER recovery probe can become UNCLAIMABLE after it is created —
// the order's exchange is disabled, its credential removed, or a restart did not construct its
// client. The claim loop never picks it up and SweepStuck ignores QUEUED GET_ORDERs, so it would
// sit stuck forever. A startup + periodic sweep finalizes such probes conservatively (order-
// authoritative, mode-scoped), and the ambiguous-outcome paths re-check capability before creating
// a probe.

// seedRecoveryProbe inserts a QUEUED GET_ORDER recovery probe on the given exchange.
func (it *intg) seedRecoveryProbe(t *testing.T, cyc, ord, exchangeID int64, purpose, extID string) int64 {
	t.Helper()
	payload := fmt.Sprintf(`{"purpose":%q,"exchange_order_id":%q,"cycle_id":%d}`, purpose, extID, cyc)
	res, err := it.db.Exec(`INSERT INTO exchange_requests
		(exchange_id, cycle_id, order_id, symbol, request_type, priority, status, payload, timeout_ms, max_retries, idempotency_key)
		VALUES (?, ?, ?, 'X/IRT', 'GET_ORDER', 25, 'QUEUED', ?, 10000, 5, ?)`,
		exchangeID, cyc, ord, payload, fmt.Sprintf("probe_%d_%s", ord, time.Now().Format("150405.000000000")))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id
}

func (it *intg) reqStatusOf(t *testing.T, id int64) string { return reqStatus(t, it.db, id) }

// --- existing queued probe, exchange later disabled ------------------------------------------

func TestSweepUnclaimableProbeExchangeDisabled(t *testing.T) {
	for _, purpose := range []string{orders.PurposeAmbiguousPlaceProbe, orders.PurposeAmbiguousCancelProbe} {
		t.Run(purpose, func(t *testing.T) {
			it := setup(t)
			cyc, ord, _ := it.seedBuyCycle(t, "0.5")
			probe := it.seedRecoveryProbe(t, cyc, ord, it.exID, purpose, "EXT-1")
			// The exchange was usable when the probe was created; now it is disabled.
			if _, err := it.db.Exec("UPDATE exchanges SET enabled=0 WHERE id=?", it.exID); err != nil {
				t.Fatal(err)
			}

			ex := it.modeExec(t, "live")
			runSweeps(it.ctx, ex, it.q) // the production recovery sweep sequence

			if s := it.reqStatusOf(t, probe); s != "DEAD" {
				t.Errorf("unclaimable probe = %s, want DEAD (exchange disabled after it was queued)", s)
			}
			if st := it.orderState(ord); st != "NEEDS_RECONCILE" {
				t.Errorf("order = %s, want NEEDS_RECONCILE", st)
			}
			if st := it.cycleState(cyc); st != "NEEDS_RECONCILE" {
				t.Errorf("cycle = %s, want NEEDS_RECONCILE", st)
			}
			if st := it.lockStateByCycle(cyc); st != "ACTIVE" {
				t.Errorf("lock = %s, want ACTIVE (held)", st)
			}
		})
	}
}

// --- existing queued probe, restart without the client ---------------------------------------

// modeExecNoClient builds an executor with an EMPTY client map (as after a restart that did not
// construct the exchange's client) bound to an explicit mode.
func (it *intg) modeExecNoClient(t *testing.T, mode string) *Executor {
	t.Helper()
	ex := New(it.store, it.q, map[string]exchanges.PrivateClient{}, nil,
		Config{Name: "noclient-" + mode, AllowLiveExecution: true, ExecutionMode: mode,
			FinalStatusDelay: 10 * time.Millisecond, Recovery: fastRecovery()})
	if err := ex.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	return ex
}

func TestSweepUnclaimableProbeRestartWithoutClient(t *testing.T) {
	for _, purpose := range []string{orders.PurposeAmbiguousPlaceProbe, orders.PurposeAmbiguousCancelProbe} {
		t.Run(purpose, func(t *testing.T) {
			it := setup(t)
			cyc, ord, _ := it.seedBuyCycle(t, "0.5")
			probe := it.seedRecoveryProbe(t, cyc, ord, it.exID, purpose, "EXT-1") // exchange stays enabled

			// A new executor that never constructed this exchange's client.
			ex := it.modeExecNoClient(t, "live")
			ex.sweepUnclaimableRecoveryProbes(it.ctx, 200) // startup sweep

			if s := it.reqStatusOf(t, probe); s != "DEAD" {
				t.Errorf("probe = %s, want DEAD (no client constructed for its exchange after restart)", s)
			}
			if st := it.orderState(ord); st != "NEEDS_RECONCILE" {
				t.Errorf("order = %s, want NEEDS_RECONCILE", st)
			}
			if st := it.lockStateByCycle(cyc); st != "ACTIVE" {
				t.Errorf("lock = %s, want ACTIVE (held)", st)
			}
		})
	}
}

// A USABLE exchange's probe is left alone (it will be claimed normally).
func TestSweepUnclaimableProbeUsableExchangeUntouched(t *testing.T) {
	it := setup(t)
	cyc, ord, _ := it.seedBuyCycle(t, "0.5")
	probe := it.seedRecoveryProbe(t, cyc, ord, it.exID, orders.PurposeAmbiguousPlaceProbe, "EXT-1")

	ex := it.modeExec(t, "live") // wired client + enabled exchange
	ex.sweepUnclaimableRecoveryProbes(it.ctx, 200)

	if s := it.reqStatusOf(t, probe); s != "QUEUED" {
		t.Errorf("claimable probe = %s, want left QUEUED (usable exchange)", s)
	}
	if st := it.cycleState(cyc); st == "NEEDS_RECONCILE" {
		t.Error("a claimable probe's cycle was reconciled — must be left for the claim loop")
	}
}

// --- ambiguous place/cancel loses recovery capability before probe creation ------------------

func TestAmbiguousPlaceNoUnclaimableProbe(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, req := it.seedBuyCycle(t, "0.5")
	claim := it.claimOf(t, req) // claim while the exchange is enabled
	if _, err := it.db.Exec("UPDATE exchanges SET enabled=0 WHERE id=?", it.exID); err != nil {
		t.Fatal(err)
	}

	it.exec.recoverAmbiguousPlace(it.ctx, claim, "loc-cid", errors.New("ack timed out (ambiguous)"))

	if p := it.getOrderProbeCount(t, ord); p != 0 {
		t.Errorf("%d recovery probes created after losing recovery capability, want 0", p)
	}
	if s := it.reqStatusOf(t, req); s != "DEAD" {
		t.Errorf("request = %s, want DEAD", s)
	}
	if st := it.orderState(ord); st != "NEEDS_RECONCILE" {
		t.Errorf("order = %s, want NEEDS_RECONCILE", st)
	}
	if st := it.lockStateByCycle(cyc); st != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (held)", st)
	}
}

func TestAmbiguousCancelNoUnclaimableProbe(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, _ := it.seedBuyCycle(t, "0.5")
	cancelReq := it.seedCancelFor(t, cyc, ord, "EXT-9")
	claim := it.claimOf(t, cancelReq)
	if _, err := it.db.Exec("UPDATE exchanges SET enabled=0 WHERE id=?", it.exID); err != nil {
		t.Fatal(err)
	}

	it.exec.recoverAmbiguousCancel(it.ctx, claim, orders.FollowupPayload{ExchangeOrderID: "EXT-9"}, errors.New("cancel ack timed out (ambiguous)"))

	if p := it.getOrderProbeCount(t, ord); p != 0 {
		t.Errorf("%d recovery probes created after losing recovery capability, want 0", p)
	}
	if s := it.reqStatusOf(t, cancelReq); s != "DEAD" {
		t.Errorf("request = %s, want DEAD", s)
	}
	if st := it.orderState(ord); st != "NEEDS_RECONCILE" {
		t.Errorf("order = %s, want NEEDS_RECONCILE", st)
	}
	if st := it.lockStateByCycle(cyc); st != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (held)", st)
	}
}

// --- mode + ownership isolation for the unclaimable-probe sweep -------------------------------

// The probe's order is in a LIVE cycle A but the probe row falsely claims a DRY-RUN cycle B on a
// DIFFERENT (unwired) exchange. The dry-run executor must never touch it; the live executor
// finalizes it on the ORDER's cycle A; cycle/lock B stay unchanged.
func TestSweepUnclaimableProbeCrossModeIsolation(t *testing.T) {
	it := setup(t)
	other := it.seedExchange(t) // unwired in both executors
	ordA, _ := it.seedBuyCycleOn(t, other, "px", "0.5")
	cycA := it.cycleOf(t, ordA) // LIVE (seedBuyCycleOn makes a live cycle)
	cycB, _, _ := it.seedBuyCycle(t, "0.5")
	it.makeDryRun(t, cycB)
	// Probe: order in live cycle A (exchange `other`, unwired), but claims dry-run cycle B.
	probe := it.seedRecoveryProbe(t, cycB, ordA, other, orders.PurposeAmbiguousPlaceProbe, "EXT-1")

	dry := it.modeExec(t, "dry_run")
	dry.sweepUnclaimableRecoveryProbes(it.ctx, 200)
	if s := it.reqStatusOf(t, probe); s != "QUEUED" {
		t.Errorf("dry-run executor changed a live-order probe to %s; it must not touch it", s)
	}
	if st := it.cycleState(cycA); st == "NEEDS_RECONCILE" {
		t.Error("dry-run executor reconciled live cycle A — cross-mode violation")
	}

	live := it.modeExec(t, "live")
	live.sweepUnclaimableRecoveryProbes(it.ctx, 200)
	if s := it.reqStatusOf(t, probe); s != "DEAD" {
		t.Errorf("probe = %s, want DEAD (order's exchange unwired → unclaimable)", s)
	}
	if st := it.orderState(ordA); st != "NEEDS_RECONCILE" {
		t.Errorf("order A = %s, want NEEDS_RECONCILE", st)
	}
	if st := it.cycleState(cycA); st != "NEEDS_RECONCILE" {
		t.Errorf("cycle A = %s, want NEEDS_RECONCILE (authoritative from the order)", st)
	}
	if st := it.lockStateByCycle(cycA); st != "ACTIVE" {
		t.Errorf("lock A = %s, want ACTIVE (held)", st)
	}
	if st := it.cycleState(cycB); st != "BUY_REQUEST_QUEUED" {
		t.Errorf("claimed (untrusted) cycle B = %s, want UNCHANGED", st)
	}
	if st := it.lockStateByCycle(cycB); st != "ACTIVE" {
		t.Errorf("claimed lock B = %s, want unchanged ACTIVE", st)
	}
}

var _ = queue.TypeGetOrder
