package reconciler

import (
	"errors"

	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/state"
)

// Decision is a reconciliation outcome for one order.
type Decision string

const (
	// NoAction: nothing to do (already terminal / no client to verify with).
	NoAction Decision = "no_action"
	// Continue: the order is legitimately still active on the exchange; resume.
	Continue Decision = "continue"
	// AdvanceTerminal: the exchange clearly reports a terminal state; advance the
	// DB order to TargetState.
	AdvanceTerminal Decision = "advance_terminal"
	// NeedsReconcile: ambiguous/contradictory/unverifiable — flag for the operator.
	NeedsReconcile Decision = "needs_reconcile"
)

// OrderOutcome is the decision for one order plus any data to persist.
type OrderOutcome struct {
	Decision    Decision
	TargetState state.OrderState // for AdvanceTerminal
	Reason      string
	// AttachExchangeOrderID, if set, is the exchange order id positively
	// identified for an order that previously had only a local client order id.
	AttachExchangeOrderID string
}

// decideKnownOrder maps a GetOrder result (for an order whose exchange_order_id
// is known) into a reconciliation outcome. It is PURE — no DB/IO — so the whole
// decision matrix is unit-testable.
//
// SAFETY: "exchange reports order unknown" is NOT treated as proof the order
// never filled (it could have filled and aged out of the venue's lookup window),
// so it goes to NeedsReconcile — never to a clean close. Likewise an order merely
// missing from open-orders is not proof of no fill (handled by the caller).
func decideKnownOrder(dbState state.OrderState, st execution.OrderStatus, statusErr error) OrderOutcome {
	if statusErr != nil {
		if errors.Is(statusErr, execution.ErrOrderUnknown) {
			return OrderOutcome{Decision: NeedsReconcile,
				Reason: "exchange reports order unknown — NOT proof of no fill; needs reconcile"}
		}
		return OrderOutcome{Decision: NeedsReconcile,
			Reason: "exchange status unavailable: " + statusErr.Error()}
	}

	switch st.Status {
	case execution.StateFilled:
		return advanceOrNoop(dbState, state.OrderFilled, "exchange reports FILLED")
	case execution.StateCanceled:
		if st.FilledQty.IsPositive() {
			// A cancel that left a fill behind = inventory/exposure → accounting (PR10).
			return OrderOutcome{Decision: NeedsReconcile,
				Reason: "canceled but a fill exists — exposure pending fill accounting (PR10)"}
		}
		return advanceOrNoop(dbState, state.OrderCancelled, "exchange reports CANCELED, zero fill")
	case execution.StatePartiallyCanceled:
		return OrderOutcome{Decision: NeedsReconcile,
			Reason: "partially canceled — filled inventory pending accounting (PR10)"}
	case execution.StateRejected:
		// REJECTED is NOT a clean zero-fill cancel — the venue REFUSED the order, an execution
		// anomaly. It must NOT advance to a terminal state that could trigger a safe-close +
		// lock release: for a sell the buy leg may already hold inventory (lock must stay
		// held), and for a buy silently closing it would hide the rejection. Always
		// NeedsReconcile — reconciliation/operator decides the side-appropriate next action.
		return OrderOutcome{Decision: NeedsReconcile,
			Reason: "exchange reports REJECTED — execution anomaly, never a clean cancel; needs reconcile"}
	case execution.StateExpired:
		if st.FilledQty.IsPositive() {
			return OrderOutcome{Decision: NeedsReconcile,
				Reason: "expired with a partial fill — pending accounting (PR10)"}
		}
		return advanceOrNoop(dbState, state.OrderExpired, "exchange reports EXPIRED, zero fill")
	case execution.StateOpen, execution.StateNew:
		return OrderOutcome{Decision: Continue, Reason: "still working on the exchange"}
	case execution.StatePartiallyFilled:
		return OrderOutcome{Decision: Continue, Reason: "partially filled, still open — resume; accounting in PR10"}
	default:
		return OrderOutcome{Decision: NeedsReconcile, Reason: "exchange status unclear"}
	}
}

// advanceOrNoop returns AdvanceTerminal to target if that is a legal transition
// from dbState; NoAction if already there/terminal; NeedsReconcile if the
// transition would be illegal (conservative — never force an illegal state).
func advanceOrNoop(dbState, target state.OrderState, reason string) OrderOutcome {
	if dbState == target || state.IsTerminalOrder(dbState) {
		return OrderOutcome{Decision: NoAction, Reason: "already terminal/at target"}
	}
	if state.ValidateOrderTransition(dbState, target) != nil {
		return OrderOutcome{Decision: NeedsReconcile,
			Reason: "cannot advance " + string(dbState) + "->" + string(target) + " safely"}
	}
	return OrderOutcome{Decision: AdvanceTerminal, TargetState: target, Reason: reason}
}
