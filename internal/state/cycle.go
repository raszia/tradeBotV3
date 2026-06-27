package state

// CycleState is the lifecycle state of a trading cycle. It is stored as a string
// in cycles.state. States change ONLY through the validated transitions in this
// package (see ApplyCycleTransition) — never via ad-hoc SQL elsewhere — so the
// lifecycle stays deterministic and auditable.
type CycleState string

const (
	CycleNew                 CycleState = "NEW"
	CycleSignalDetected      CycleState = "SIGNAL_DETECTED"
	CycleBuyRequestQueued    CycleState = "BUY_REQUEST_QUEUED"
	CycleBuySubmitted        CycleState = "BUY_SUBMITTED"
	CycleBuyPartiallyFilled  CycleState = "BUY_PARTIALLY_FILLED"
	CycleBuyFilled           CycleState = "BUY_FILLED"
	CycleSellRequestQueued   CycleState = "SELL_REQUEST_QUEUED"
	CycleSellSubmitted       CycleState = "SELL_SUBMITTED"
	CycleSellRepricePending  CycleState = "SELL_REPRICE_PENDING"
	CycleSellPartiallyFilled CycleState = "SELL_PARTIALLY_FILLED"
	CycleSellFilled          CycleState = "SELL_FILLED"
	CycleCancelPending       CycleState = "CANCEL_PENDING"
	CycleCancelled           CycleState = "CANCELLED"
	CycleFailed              CycleState = "FAILED"
	CycleNeedsReconcile      CycleState = "NEEDS_RECONCILE"
	CycleClosed              CycleState = "CLOSED"
)

// AllCycleStates lists every defined cycle state (used for validation and
// exhaustive tests).
var AllCycleStates = []CycleState{
	CycleNew, CycleSignalDetected, CycleBuyRequestQueued, CycleBuySubmitted,
	CycleBuyPartiallyFilled, CycleBuyFilled, CycleSellRequestQueued, CycleSellSubmitted,
	CycleSellRepricePending, CycleSellPartiallyFilled, CycleSellFilled, CycleCancelPending,
	CycleCancelled, CycleFailed, CycleNeedsReconcile, CycleClosed,
}

// terminalCycleStates are end states: a cycle in one of these never transitions
// again through the normal transition functions.
var terminalCycleStates = map[CycleState]bool{
	CycleClosed:    true,
	CycleCancelled: true,
	CycleFailed:    true,
}

// cycleTransitions is the authoritative map of allowed normal transitions
// (from -> set of allowed to). It intentionally contains NO self-loops, NO
// transitions out of terminal states, and NO transitions out of NEEDS_RECONCILE.
// Entry into NEEDS_RECONCILE is handled specially (see cycleCanTransition) and is
// allowed from any non-terminal state. Exit from NEEDS_RECONCILE is deliberately
// absent here — it is reserved for an explicit operator/reconciler resolution
// path added in a later PR, never an automatic transition.
var cycleTransitions = map[CycleState]map[CycleState]bool{
	CycleNew: {
		CycleSignalDetected: true,
		// CANCELLED is the clean "abandoned with no exposure" terminal (e.g. a
		// zero-fill simulated-IOC attempt); FAILED is reserved for real failures.
		CycleCancelled: true,
		CycleFailed:    true,
	},
	CycleSignalDetected: {
		CycleBuyRequestQueued: true,
		CycleCancelled:        true,
		CycleFailed:           true,
	},
	CycleBuyRequestQueued: {
		CycleBuySubmitted:  true,
		CycleCancelPending: true,
		// A queued buy abandoned before submission with no exposure → CANCELLED.
		CycleCancelled: true,
		CycleFailed:    true,
	},
	CycleBuySubmitted: {
		CycleBuyPartiallyFilled: true,
		CycleBuyFilled:          true,
		CycleCancelPending:      true,
		// A submitted buy that ends with zero fill and no exposure → CANCELLED
		// (clean no-fill), not FAILED.
		CycleCancelled: true,
		CycleFailed:    true,
	},
	CycleBuyPartiallyFilled: {
		CycleBuyFilled:     true,
		CycleCancelPending: true,
		CycleFailed:        true,
	},
	CycleBuyFilled: {
		CycleSellRequestQueued: true,
		CycleFailed:            true,
	},
	CycleSellRequestQueued: {
		CycleSellSubmitted: true,
		CycleCancelPending: true,
		CycleFailed:        true,
	},
	CycleSellSubmitted: {
		CycleSellPartiallyFilled: true,
		CycleSellFilled:          true,
		CycleSellRepricePending:  true,
		CycleCancelPending:       true,
		CycleFailed:              true,
	},
	CycleSellRepricePending: {
		CycleSellRequestQueued: true,
		CycleSellSubmitted:     true,
		CycleCancelPending:     true,
		CycleFailed:            true,
	},
	CycleSellPartiallyFilled: {
		CycleSellFilled:         true,
		CycleSellRepricePending: true,
		CycleCancelPending:      true,
		CycleFailed:             true,
	},
	CycleSellFilled: {
		CycleClosed: true,
		CycleFailed: true,
	},
	CycleCancelPending: {
		CycleCancelled: true,
		CycleFailed:    true,
	},
	// Terminal states (CLOSED, CANCELLED, FAILED) and NEEDS_RECONCILE intentionally
	// have no outgoing normal transitions.
}

// IsTerminalCycle reports whether s is a terminal cycle state. NEEDS_RECONCILE is
// NOT terminal — it is a holding state awaiting explicit resolution.
func IsTerminalCycle(s CycleState) bool { return terminalCycleStates[s] }

// IsKnownCycleState reports whether s is a defined cycle state.
func IsKnownCycleState(s CycleState) bool {
	for _, k := range AllCycleStates {
		if k == s {
			return true
		}
	}
	return false
}

// ValidCycleTargets returns the states reachable from `from` via a normal
// transition, including NEEDS_RECONCILE when allowed.
func ValidCycleTargets(from CycleState) []CycleState {
	var out []CycleState
	for _, to := range AllCycleStates {
		if cycleCanTransition(from, to) {
			out = append(out, to)
		}
	}
	return out
}

// cycleCanTransition is the single source of truth for whether from->to is a
// legal normal transition. NEEDS_RECONCILE is reachable from any non-terminal
// state; everything else comes from cycleTransitions.
func cycleCanTransition(from, to CycleState) bool {
	if to == CycleNeedsReconcile {
		return canEnterNeedsReconcileCycle(from)
	}
	return cycleTransitions[from][to]
}

// canEnterNeedsReconcileCycle: any non-terminal cycle (other than one already in
// NEEDS_RECONCILE) may be flagged for reconciliation.
func canEnterNeedsReconcileCycle(from CycleState) bool {
	return !IsTerminalCycle(from) && from != CycleNeedsReconcile
}
