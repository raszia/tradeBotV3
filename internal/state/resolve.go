package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Operator resolution is the ONLY exit from NEEDS_RECONCILE — explicit, audited, and
// never automatic. These targets are intentionally SEPARATE from the normal trading
// transition map (cycleTransitions / orderTransitions), so the engine/executor flow can
// never move a cycle/order out of NEEDS_RECONCILE; only ApplyCycleResolution /
// ApplyOrderResolution can, and only an authenticated operator path calls them.

// ErrNotReconcileResolution is returned when a resolution does not start from
// NEEDS_RECONCILE or targets a state that is not a permitted resolution outcome.
var ErrNotReconcileResolution = errors.New("state: not a valid NEEDS_RECONCILE resolution")

// cycleResolutionTargets are the cycle states a NEEDS_RECONCILE cycle may be resolved
// INTO by an operator. CANCELLED/FAILED/CLOSED are terminal exits; the *_FILLED targets
// hand the cycle back to the normal flow (e.g. a confirmed buy fill resumes sell
// management) — those keep the symbol lock because real inventory exists.
var cycleResolutionTargets = map[CycleState]bool{
	CycleBuyFilled:           true,
	CycleBuyPartiallyFilled:  true,
	CycleSellPartiallyFilled: true,
	CycleSellFilled:          true,
	CycleCancelled:           true,
	CycleFailed:              true,
	CycleClosed:              true,
}

// orderResolutionTargets are the order states an operator may resolve a NEEDS_RECONCILE
// order into.
var orderResolutionTargets = map[OrderState]bool{
	OrderFilled:          true,
	OrderPartiallyFilled: true,
	OrderCancelled:       true,
	OrderFailed:          true,
}

// IsCycleResolutionTarget / IsOrderResolutionTarget expose the whitelists for the
// resolution tool's validation + preview.
func IsCycleResolutionTarget(to CycleState) bool { return cycleResolutionTargets[to] }
func IsOrderResolutionTarget(to OrderState) bool { return orderResolutionTargets[to] }

// ApplyCycleResolution moves a cycle OUT of NEEDS_RECONCILE to a permitted resolution
// target, within the caller's tx, with the same version-guarded CAS + event-row write as
// a normal transition. From MUST be NEEDS_RECONCILE and To MUST be a resolution target —
// any other request is rejected (no ad-hoc state changes, no illegal transitions).
func ApplyCycleResolution(ctx context.Context, tx *sql.Tx, t CycleTransition) (Result, error) {
	if t.From != CycleNeedsReconcile {
		return Result{}, fmt.Errorf("%w: cycle resolution must start from NEEDS_RECONCILE, got %s", ErrNotReconcileResolution, t.From)
	}
	if !cycleResolutionTargets[t.To] {
		return Result{}, fmt.Errorf("%w: %s is not a resolvable cycle target", ErrNotReconcileResolution, t.To)
	}
	return applyTransition(ctx, tx, transitionSpec{
		table:          "cycles",
		idCol:          "id",
		eventTable:     "cycle_state_events",
		eventParentCol: "cycle_id",
		id:             t.CycleID,
		from:           string(t.From),
		to:             string(t.To),
		version:        t.Version,
		eventType:      defaultEventType(t.EventType, "operator_resolution"),
		message:        t.Reason,
		payload:        t.Payload,
	})
}

// ApplyOrderResolution is the order analogue of ApplyCycleResolution.
func ApplyOrderResolution(ctx context.Context, tx *sql.Tx, t OrderTransition) (Result, error) {
	if t.From != OrderNeedsReconcile {
		return Result{}, fmt.Errorf("%w: order resolution must start from NEEDS_RECONCILE, got %s", ErrNotReconcileResolution, t.From)
	}
	if !orderResolutionTargets[t.To] {
		return Result{}, fmt.Errorf("%w: %s is not a resolvable order target", ErrNotReconcileResolution, t.To)
	}
	return applyTransition(ctx, tx, transitionSpec{
		table:          "orders",
		idCol:          "id",
		eventTable:     "order_events",
		eventParentCol: "order_id",
		id:             t.OrderID,
		from:           string(t.From),
		to:             string(t.To),
		version:        t.Version,
		eventType:      defaultEventType(t.EventType, "operator_resolution"),
		message:        t.Reason,
		payload:        t.Payload,
	})
}
