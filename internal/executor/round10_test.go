package executor

import (
	"errors"
	"testing"
	"time"
)

// PR20 correction round 10. Production-path hardening of stale/malformed recovery:
//   #1 the execution-mode filter runs INSIDE SQL before LIMIT (no cross-mode starvation);
//   #2 conservative finalization after persistent orderRecoveryInfo failure uses the ownership
//      RETAINED from discovery — never the queue row's claimed cycle_id;
//   #3 no unclaimable GET_ORDER probe: without a usable recovery client the stale mutation is
//      finalized conservatively instead.

// --- #1: mode filter before LIMIT ------------------------------------------------------------

// TestMalformedSweepModeFilterBeforeLimit: 50 malformed rows of the OTHER mode (lower ids) plus one
// row of THIS executor's mode (highest id). With the production sweep limit of 50, one sweep must
// process this executor's row — the other mode's rows must not fill the LIMIT window and starve it.
func TestMalformedSweepModeFilterBeforeLimit(t *testing.T) {
	for _, tc := range []struct {
		name, mode string
		othersDry  bool // the 50 filler rows' cycle mode
		mineDry    bool // the starved row's cycle mode
	}{
		{"live_row_behind_50_dryrun", "live", true, false},
		{"dryrun_row_behind_50_live", "dry_run", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			it := setup(t)
			// Shared test DB: park malformed leftovers from OTHER tests so this subtest's
			// single-sweep window contains exactly the rows seeded below.
			if _, err := it.db.Exec(`UPDATE exchange_requests SET status='DEAD', last_error='test cleanup'
				WHERE request_type IN ('PLACE_ORDER','CANCEL_ORDER')
				  AND status IN ('QUEUED','RETRY_SCHEDULED','CLAIMED','IN_FLIGHT')
				  AND order_id IS NULL`); err != nil {
				t.Fatal(err)
			}
			otherCyc, _, _ := it.seedBuyCycle(t, "0.5")
			if tc.othersDry {
				it.makeDryRun(t, otherCyc)
			}
			var fillers []int64
			for i := 0; i < 50; i++ {
				fillers = append(fillers, it.seedRawMutation(t, "PLACE_ORDER", &otherCyc, nil, "QUEUED", 0))
			}
			// Park the fillers after this subtest so they cannot fill LATER tests' sweep windows.
			t.Cleanup(func() {
				for _, f := range fillers {
					_, _ = it.db.Exec("UPDATE exchange_requests SET status='DEAD', last_error='test cleanup' WHERE id=? AND status='QUEUED'", f)
				}
			})
			mineCyc, _, _ := it.seedBuyCycle(t, "0.5")
			if tc.mineDry {
				it.makeDryRun(t, mineCyc)
			}
			mine := it.seedRawMutation(t, "PLACE_ORDER", &mineCyc, nil, "QUEUED", 0) // HIGHEST id

			ex := it.modeExec(t, tc.mode)
			ex.sweepMalformedMutations(it.ctx, 50) // ONE sweep at the production Run-loop limit

			if s := reqStatus(t, it.db, mine); s != "DEAD" {
				t.Errorf("this mode's malformed row = %s after one sweep, want DEAD (LIMIT window was filled by the other mode → starvation)", s)
			}
			if st := it.cycleState(mineCyc); st != "NEEDS_RECONCILE" {
				t.Errorf("this mode's cycle = %s, want NEEDS_RECONCILE", st)
			}
			for _, f := range fillers {
				if s := reqStatus(t, it.db, f); s != "QUEUED" {
					t.Fatalf("other mode's row %d = %s, want untouched QUEUED", f, s)
				}
			}
			if st := it.cycleState(otherCyc); st != "BUY_REQUEST_QUEUED" {
				t.Errorf("other mode's cycle = %s, want unchanged", st)
			}
			if st := it.lockStateByCycle(otherCyc); st != "ACTIVE" {
				t.Errorf("other mode's lock = %s, want unchanged ACTIVE", st)
			}
		})
	}
}

// TestMalformedSweepBeforeLimitAuthoritativeOwnership: behind 50 other-mode filler rows sits ONE
// inconsistent row with a VALID persisted order (order in this executor's cycle A) whose queue
// metadata claims an UNRELATED cycle B. One production-limit sweep must (a) reach it despite the 50
// fillers (SQL mode filter before LIMIT) and (b) resolve it on the ORDER's authoritative cycle A,
// leaving the falsely-claimed cycle B untouched.
func TestMalformedSweepBeforeLimitAuthoritativeOwnership(t *testing.T) {
	it := setup(t)
	if _, err := it.db.Exec(`UPDATE exchange_requests SET status='DEAD', last_error='test cleanup'
		WHERE request_type IN ('PLACE_ORDER','CANCEL_ORDER')
		  AND status IN ('QUEUED','RETRY_SCHEDULED','CLAIMED','IN_FLIGHT') AND order_id IS NULL`); err != nil {
		t.Fatal(err)
	}
	dryCyc, _, _ := it.seedBuyCycle(t, "0.5")
	it.makeDryRun(t, dryCyc)
	var fillers []int64
	for i := 0; i < 50; i++ {
		fillers = append(fillers, it.seedRawMutation(t, "PLACE_ORDER", &dryCyc, nil, "QUEUED", 0))
	}
	t.Cleanup(func() {
		for _, f := range fillers {
			_, _ = it.db.Exec("UPDATE exchange_requests SET status='DEAD', last_error='test cleanup' WHERE id=? AND status='QUEUED'", f)
		}
	})
	cycA, ordA, _ := it.seedBuyCycle(t, "0.5") // LIVE — the order's real cycle
	cycB, _, _ := it.seedBuyCycle(t, "0.5")    // LIVE — unrelated, falsely claimed
	mine := it.seedRawMutation(t, "PLACE_ORDER", &cycB, &ordA, "QUEUED", 0)

	live := it.modeExec(t, "live")
	live.sweepMalformedMutations(it.ctx, 50) // ONE sweep at the production limit

	if s := reqStatus(t, it.db, mine); s != "DEAD" {
		t.Errorf("valid-order inconsistent row = %s after one limited sweep, want DEAD (reached despite 50 fillers)", s)
	}
	if st := it.orderState(ordA); st != "NEEDS_RECONCILE" {
		t.Errorf("order A = %s, want NEEDS_RECONCILE (authoritative from the order)", st)
	}
	if st := it.cycleState(cycA); st != "NEEDS_RECONCILE" {
		t.Errorf("cycle A = %s, want NEEDS_RECONCILE", st)
	}
	if st := it.lockStateByCycle(cycA); st != "ACTIVE" {
		t.Errorf("lock A = %s, want ACTIVE (held)", st)
	}
	if st := it.cycleState(cycB); st != "BUY_REQUEST_QUEUED" {
		t.Errorf("falsely-claimed cycle B = %s, want UNCHANGED", st)
	}
	for _, f := range fillers {
		if s := reqStatus(t, it.db, f); s != "QUEUED" {
			t.Fatalf("dry-run filler %d = %s, want untouched QUEUED", f, s)
		}
	}
}

// --- #2: persistent orderRecoveryInfo failure uses RETAINED authoritative ownership ----------

// TestStaleExpiredFailureUsesRetainedAuthoritativeCycle: the order belongs to cycle A, the queue
// row claims unrelated cycle B, and the order re-read fails persistently past the recovery window.
// The finalization must resolve the ACTUAL order + cycle A (retained at discovery) and leave the
// claimed cycle B and its lock untouched.
func TestStaleExpiredFailureUsesRetainedAuthoritativeCycle(t *testing.T) {
	for _, typ := range []string{"PLACE_ORDER", "CANCEL_ORDER"} {
		t.Run(typ, func(t *testing.T) {
			it := setup(t)
			cycA, ordA, _ := it.seedBuyCycle(t, "0.5")                              // the order's REAL cycle
			cycB, _, _ := it.seedBuyCycle(t, "0.5")                                 // unrelated, falsely claimed
			req := it.seedRawMutation(t, typ, &cycB, &ordA, "IN_FLIGHT", time.Hour) // expired (>5m floor)

			ex := it.modeExec(t, "live")
			ex.cfg.hookOrderRecoveryInfoErr = func(int64) error { return errors.New("orders table persistently unavailable") }

			ex.recoverStaleMutating(it.ctx, 5)

			if s := reqStatus(t, it.db, req); s != "DEAD" {
				t.Errorf("request = %s, want DEAD", s)
			}
			if st := it.orderState(ordA); st != "NEEDS_RECONCILE" {
				t.Errorf("ACTUAL order A = %s, want NEEDS_RECONCILE", st)
			}
			if st := it.cycleState(cycA); st != "NEEDS_RECONCILE" {
				t.Errorf("ACTUAL cycle A = %s, want NEEDS_RECONCILE (retained from discovery)", st)
			}
			if st := it.lockStateByCycle(cycA); st != "ACTIVE" {
				t.Errorf("lock A = %s, want ACTIVE (held)", st)
			}
			if st := it.cycleState(cycB); st != "BUY_REQUEST_QUEUED" {
				t.Errorf("claimed (untrusted) cycle B = %s, want UNCHANGED — it must never be mutated", st)
			}
			if st := it.lockStateByCycle(cycB); st != "ACTIVE" {
				t.Errorf("claimed lock B = %s, want unchanged ACTIVE", st)
			}
			if p := it.getOrderProbeCount(t, ordA); p != 0 {
				t.Errorf("%d probes created while the order was unreadable, want 0", p)
			}
		})
	}
}

// TestStaleExpiredFailureCrossModeIsolation: same persistent-failure scenario but the claimed cycle
// B is DRY-RUN while the order's cycle A is LIVE. The dry-run executor must never see or touch the
// row (discovery is scoped by the ORDER's cycle mode); the live executor finalizes it on A.
func TestStaleExpiredFailureCrossModeIsolation(t *testing.T) {
	it := setup(t)
	cycA, ordA, _ := it.seedBuyCycle(t, "0.5") // LIVE
	cycB, _, _ := it.seedBuyCycle(t, "0.5")
	it.makeDryRun(t, cycB) // DRY-RUN, falsely claimed
	req := it.seedRawMutation(t, "PLACE_ORDER", &cycB, &ordA, "IN_FLIGHT", time.Hour)
	fail := func(int64) error { return errors.New("orders table persistently unavailable") }

	dry := it.modeExec(t, "dry_run")
	dry.cfg.hookOrderRecoveryInfoErr = fail
	dry.recoverStaleMutating(it.ctx, 5)
	if s := reqStatus(t, it.db, req); s != "IN_FLIGHT" {
		t.Errorf("dry-run executor changed the row to %s; the ORDER's cycle is live — it must not touch it", s)
	}
	if st := it.cycleState(cycA); st == "NEEDS_RECONCILE" {
		t.Error("dry-run executor reconciled live cycle A — cross-mode violation")
	}

	live := it.modeExec(t, "live")
	live.cfg.hookOrderRecoveryInfoErr = fail
	live.recoverStaleMutating(it.ctx, 5)
	if s := reqStatus(t, it.db, req); s != "DEAD" {
		t.Errorf("request = %s, want DEAD (live executor owns the order's mode)", s)
	}
	if st := it.cycleState(cycA); st != "NEEDS_RECONCILE" {
		t.Errorf("cycle A = %s, want NEEDS_RECONCILE", st)
	}
	if st := it.lockStateByCycle(cycA); st != "ACTIVE" {
		t.Errorf("lock A = %s, want ACTIVE (held)", st)
	}
	if st := it.cycleState(cycB); st != "BUY_REQUEST_QUEUED" {
		t.Errorf("dry-run cycle B = %s, want unchanged", st)
	}
	if st := it.lockStateByCycle(cycB); st != "ACTIVE" {
		t.Errorf("dry-run lock B = %s, want unchanged ACTIVE", st)
	}
}

// --- #3: no unclaimable recovery probe -------------------------------------------------------

// TestStaleNoRecoveryClientNoUnclaimableProbe: the order's exchange has NO wired client in this
// executor. A GET_ORDER probe would never be claimed, so none may be created — the row is finalized
// conservatively instead.
func TestStaleNoRecoveryClientNoUnclaimableProbe(t *testing.T) {
	for _, typ := range []string{"PLACE_ORDER", "CANCEL_ORDER"} {
		t.Run(typ, func(t *testing.T) {
			it := setup(t)
			other := it.seedExchange(t) // enabled=1 but NOT in this executor's client map
			ord, _ := it.seedBuyCycleOn(t, other, "otherx", "0.5")
			cyc := it.cycleOf(t, ord)
			req := it.seedRawMutation(t, typ, &cyc, &ord, "IN_FLIGHT", time.Hour)
			it.setReqExchange(t, req, other) // fully CONSISTENT ownership — only the client is missing

			ex := it.modeExec(t, "live")
			ex.recoverStaleMutating(it.ctx, 5)

			if p := it.getOrderProbeCount(t, ord); p != 0 {
				t.Errorf("%d GET_ORDER probes created for an exchange with no recovery client — they would be unclaimable forever, want 0", p)
			}
			if s := reqStatus(t, it.db, req); s != "DEAD" {
				t.Errorf("request = %s, want DEAD", s)
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

// TestStaleDisabledExchangeNoUnclaimableProbe: the exchange HAS a wired client but is disabled
// (`exchanges.enabled=0`). Claim's SQL refuses disabled exchanges, so a probe would also be
// unclaimable — recovery must finalize conservatively, never leave a permanently queued probe.
// Covered for both PLACE_ORDER and CANCEL_ORDER stale recovery.
func TestStaleDisabledExchangeNoUnclaimableProbe(t *testing.T) {
	for _, typ := range []string{"PLACE_ORDER", "CANCEL_ORDER"} {
		t.Run(typ, func(t *testing.T) {
			it := setup(t)
			cyc, ord, _ := it.seedBuyCycle(t, "0.5")
			req := it.seedRawMutation(t, typ, &cyc, &ord, "IN_FLIGHT", time.Hour)
			if _, err := it.db.Exec("UPDATE exchanges SET enabled=0 WHERE id=?", it.exID); err != nil {
				t.Fatal(err)
			}

			ex := it.modeExec(t, "live")
			ex.recoverStaleMutating(it.ctx, 5)

			if p := it.getOrderProbeCount(t, ord); p != 0 {
				t.Errorf("%d GET_ORDER probes created on a DISABLED exchange — unclaimable forever, want 0", p)
			}
			if s := reqStatus(t, it.db, req); s != "DEAD" {
				t.Errorf("request = %s, want DEAD", s)
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
