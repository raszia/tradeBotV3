package state

// OrderState is the lifecycle state of a single order. Stored as a string in
// orders.state and changed ONLY through ApplyOrderTransition (never ad-hoc SQL).
type OrderState string

const (
	OrderNew             OrderState = "NEW"
	OrderRegistered      OrderState = "REGISTERED"
	OrderQueued          OrderState = "QUEUED"
	OrderSubmitted       OrderState = "SUBMITTED"
	OrderAcked           OrderState = "ACKED"
	OrderPartiallyFilled OrderState = "PARTIALLY_FILLED"
	OrderFilled          OrderState = "FILLED"
	OrderCancelPending   OrderState = "CANCEL_PENDING"
	OrderCancelled       OrderState = "CANCELLED"
	OrderRejected        OrderState = "REJECTED"
	OrderExpired         OrderState = "EXPIRED"
	OrderFailed          OrderState = "FAILED"
	OrderNeedsReconcile  OrderState = "NEEDS_RECONCILE"
)

// AllOrderStates lists every defined order state.
var AllOrderStates = []OrderState{
	OrderNew, OrderRegistered, OrderQueued, OrderSubmitted, OrderAcked,
	OrderPartiallyFilled, OrderFilled, OrderCancelPending, OrderCancelled,
	OrderRejected, OrderExpired, OrderFailed, OrderNeedsReconcile,
}

// terminalOrderStates are end states.
var terminalOrderStates = map[OrderState]bool{
	OrderFilled:    true,
	OrderCancelled: true,
	OrderRejected:  true,
	OrderExpired:   true,
	OrderFailed:    true,
}

// orderTransitions is the authoritative allowed-transition map for orders. As
// with cycles: no self-loops, no exits from terminal states, no exits from
// NEEDS_RECONCILE (operator-only, later PR). Entry into NEEDS_RECONCILE is
// special-cased and allowed from any non-terminal state.
//
// CANCEL_PENDING can still resolve to FILLED/PARTIALLY_FILLED because a cancel
// request may race with a fill on the exchange — we must accept the exchange's
// truth rather than assume the cancel won.
var orderTransitions = map[OrderState]map[OrderState]bool{
	OrderNew: {
		OrderRegistered: true,
		OrderFailed:     true,
	},
	OrderRegistered: {
		OrderQueued: true,
		OrderFailed: true,
	},
	OrderQueued: {
		OrderSubmitted:     true,
		OrderCancelPending: true,
		OrderFailed:        true,
	},
	OrderSubmitted: {
		OrderAcked:           true,
		OrderPartiallyFilled: true,
		OrderFilled:          true,
		OrderRejected:        true,
		OrderExpired:         true,
		OrderCancelPending:   true,
		OrderFailed:          true,
	},
	OrderAcked: {
		OrderPartiallyFilled: true,
		OrderFilled:          true,
		OrderCancelPending:   true,
		OrderExpired:         true,
		OrderFailed:          true,
	},
	OrderPartiallyFilled: {
		OrderFilled:        true,
		OrderCancelPending: true,
		OrderExpired:       true,
		OrderFailed:        true,
	},
	OrderCancelPending: {
		OrderCancelled:       true,
		OrderPartiallyFilled: true, // cancel raced a fill
		OrderFilled:          true, // cancel raced a full fill
		OrderFailed:          true,
	},
	// Terminal states and NEEDS_RECONCILE have no outgoing normal transitions.
}

// IsTerminalOrder reports whether s is a terminal order state. NEEDS_RECONCILE is
// NOT terminal.
func IsTerminalOrder(s OrderState) bool { return terminalOrderStates[s] }

// IsKnownOrderState reports whether s is a defined order state.
func IsKnownOrderState(s OrderState) bool {
	for _, k := range AllOrderStates {
		if k == s {
			return true
		}
	}
	return false
}

// ValidOrderTargets returns the states reachable from `from`.
func ValidOrderTargets(from OrderState) []OrderState {
	var out []OrderState
	for _, to := range AllOrderStates {
		if orderCanTransition(from, to) {
			out = append(out, to)
		}
	}
	return out
}

func orderCanTransition(from, to OrderState) bool {
	if to == OrderNeedsReconcile {
		return canEnterNeedsReconcileOrder(from)
	}
	return orderTransitions[from][to]
}

func canEnterNeedsReconcileOrder(from OrderState) bool {
	return !IsTerminalOrder(from) && from != OrderNeedsReconcile
}
