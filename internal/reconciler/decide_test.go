package reconciler

import (
	"errors"
	"testing"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/state"
)

func st(status execution.NormalizedOrderState, filled string) execution.OrderStatus {
	return execution.OrderStatus{Status: status, FilledQty: decimal.RequireFromString(filled)}
}

func TestDecideKnownOrderClearTerminals(t *testing.T) {
	cases := []struct {
		name   string
		from   state.OrderState
		status execution.OrderStatus
		want   Decision
		target state.OrderState
	}{
		{"filled", state.OrderSubmitted, st(execution.StateFilled, "1"), AdvanceTerminal, state.OrderFilled},
		// CANCELLED is only legal from CANCEL_PENDING (the cancel we requested).
		{"canceled zero-fill", state.OrderCancelPending, st(execution.StateCanceled, "0"), AdvanceTerminal, state.OrderCancelled},
		{"rejected", state.OrderSubmitted, st(execution.StateRejected, "0"), AdvanceTerminal, state.OrderRejected},
		{"expired zero-fill", state.OrderSubmitted, st(execution.StateExpired, "0"), AdvanceTerminal, state.OrderExpired},
	}
	for _, c := range cases {
		out := decideKnownOrder(c.from, c.status, nil)
		if out.Decision != c.want || out.TargetState != c.target {
			t.Errorf("%s: got %s->%s, want %s->%s", c.name, out.Decision, out.TargetState, c.want, c.target)
		}
	}
}

func TestDecideKnownOrderAmbiguousAndExposure(t *testing.T) {
	// canceled WITH a fill = exposure -> needs reconcile (not a clean cancel).
	if out := decideKnownOrder(state.OrderSubmitted, st(execution.StateCanceled, "0.5"), nil); out.Decision != NeedsReconcile {
		t.Errorf("canceled-with-fill = %s, want needs_reconcile", out.Decision)
	}
	if out := decideKnownOrder(state.OrderSubmitted, st(execution.StatePartiallyCanceled, "0.5"), nil); out.Decision != NeedsReconcile {
		t.Errorf("partially_canceled = %s, want needs_reconcile", out.Decision)
	}
	if out := decideKnownOrder(state.OrderSubmitted, st(execution.StateExpired, "0.3"), nil); out.Decision != NeedsReconcile {
		t.Errorf("expired-with-fill = %s, want needs_reconcile", out.Decision)
	}
	if out := decideKnownOrder(state.OrderSubmitted, st(execution.StateUnknown, "0"), nil); out.Decision != NeedsReconcile {
		t.Errorf("unknown = %s, want needs_reconcile", out.Decision)
	}
}

func TestDecideKnownOrderStillWorking(t *testing.T) {
	for _, s := range []execution.NormalizedOrderState{execution.StateOpen, execution.StateNew, execution.StatePartiallyFilled} {
		if out := decideKnownOrder(state.OrderSubmitted, st(s, "0"), nil); out.Decision != Continue {
			t.Errorf("%s = %s, want continue", s, out.Decision)
		}
	}
}

func TestDecideKnownOrderUnknownIsNotProofOfNoFill(t *testing.T) {
	// The exchange not knowing the order is NOT proof it never filled -> reconcile.
	out := decideKnownOrder(state.OrderSubmitted, execution.OrderStatus{}, execution.ErrOrderUnknown)
	if out.Decision != NeedsReconcile {
		t.Fatalf("ErrOrderUnknown -> %s, want needs_reconcile (not a close)", out.Decision)
	}
	// A generic API error is also conservative.
	if out := decideKnownOrder(state.OrderSubmitted, execution.OrderStatus{}, errors.New("503")); out.Decision != NeedsReconcile {
		t.Errorf("api error -> %s, want needs_reconcile", out.Decision)
	}
}

func TestAdvanceOrNoop(t *testing.T) {
	// already terminal -> no action
	if out := advanceOrNoop(state.OrderFilled, state.OrderFilled, "x"); out.Decision != NoAction {
		t.Errorf("already terminal = %s, want no_action", out.Decision)
	}
	// illegal transition -> needs reconcile (never force)
	if out := advanceOrNoop(state.OrderNew, state.OrderFilled, "x"); out.Decision != NeedsReconcile {
		t.Errorf("illegal NEW->FILLED = %s, want needs_reconcile", out.Decision)
	}
}
