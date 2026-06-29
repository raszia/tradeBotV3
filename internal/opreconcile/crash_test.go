package opreconcile

import (
	"errors"
	"testing"
)

// TestApplyCrashRollsBackAtomically (PR26 correction, scenario 6) proves an operator
// resolution apply is all-or-nothing: if it crashes/fails just before commit, NEITHER the
// state change NOR the audit row survives, and the symbol lock is not released. Preview
// already mutates nothing (TestPreviewDoesNotMutate). No exchange is contacted.
func TestApplyCrashRollsBackAtomically(t *testing.T) {
	f := setupR(t)
	f.seed("1", "0", false, "", "") // a NEEDS_RECONCILE cycle with zero exposure + an active lock

	// Inject a fault just before commit on an otherwise-valid resolution.
	f.r.faultBeforeCommit = func() error { return errors.New("injected: crash before reconcile commit") }

	_, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "would close", Operator: "op"})
	if err == nil {
		t.Fatal("apply must fail when the pre-commit fault fires")
	}

	// Nothing partially applied: cycle still NEEDS_RECONCILE, lock still ACTIVE, no audit row.
	if f.cycleState() != "NEEDS_RECONCILE" {
		t.Errorf("cycle = %s, want NEEDS_RECONCILE (rolled back)", f.cycleState())
	}
	if f.lockState() != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (rolled back)", f.lockState())
	}
	if f.auditCount() != 0 {
		t.Errorf("audit rows = %d, want 0 (rolled back — no partial audit)", f.auditCount())
	}

	// With the fault cleared, the same apply commits cleanly (state + audit together).
	f.r.faultBeforeCommit = nil
	if _, err := f.r.Apply(f.ctx, Request{CycleID: f.cycID, Action: ActionCancelZeroExposure, Reason: "close now", Operator: "op"}); err != nil {
		t.Fatal(err)
	}
	if f.cycleState() != "CANCELLED" || f.lockState() != "RELEASED" || f.auditCount() != 1 {
		t.Errorf("after clean apply: cycle=%s lock=%s audits=%d (want CANCELLED/RELEASED/1)", f.cycleState(), f.lockState(), f.auditCount())
	}
}
