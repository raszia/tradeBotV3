package state

import (
	"errors"
	"testing"
)

// ---- cycle transitions ----

func TestCycleLegalTransitionsFromMapAllValidate(t *testing.T) {
	for from, tos := range cycleTransitions {
		for to := range tos {
			if err := ValidateCycleTransition(from, to); err != nil {
				t.Errorf("declared legal transition %s->%s rejected: %v", from, to, err)
			}
		}
	}
}

func TestCycleNoSelfLoops(t *testing.T) {
	for _, s := range AllCycleStates {
		if err := ValidateCycleTransition(s, s); err == nil {
			t.Errorf("self-loop %s->%s should be illegal", s, s)
		}
	}
}

func TestCycleTerminalStatesHaveNoExit(t *testing.T) {
	terminals := []CycleState{CycleClosed, CycleCancelled, CycleFailed}
	for _, term := range terminals {
		if !IsTerminalCycle(term) {
			t.Errorf("%s should be terminal", term)
		}
		for _, to := range AllCycleStates {
			if err := ValidateCycleTransition(term, to); err == nil {
				t.Errorf("terminal %s must not transition to %s", term, to)
			}
		}
	}
}

func TestCycleNeedsReconcileEntryFromNonTerminal(t *testing.T) {
	for _, from := range AllCycleStates {
		err := ValidateCycleTransition(from, CycleNeedsReconcile)
		nonTerminalSource := !IsTerminalCycle(from) && from != CycleNeedsReconcile
		if nonTerminalSource && err != nil {
			t.Errorf("non-terminal %s should be able to enter NEEDS_RECONCILE: %v", from, err)
		}
		if !nonTerminalSource && err == nil {
			t.Errorf("%s must NOT enter NEEDS_RECONCILE", from)
		}
	}
}

func TestCycleNeedsReconcileHasNoAutomaticExit(t *testing.T) {
	for _, to := range AllCycleStates {
		if err := ValidateCycleTransition(CycleNeedsReconcile, to); err == nil {
			t.Errorf("NEEDS_RECONCILE must not transition to %s via normal transitions", to)
		}
	}
}

func TestCycleSpecificIllegalTransitions(t *testing.T) {
	illegal := [][2]CycleState{
		{CycleNew, CycleBuyFilled}, // skips steps
		{CycleNew, CycleClosed},    // skips entire lifecycle
		{CycleSignalDetected, CycleSellFilled},
		{CycleBuySubmitted, CycleClosed},
		{CycleSellFilled, CycleNew},         // backwards
		{CycleClosed, CycleSignalDetected},  // out of terminal
		{CycleBuyFilled, CycleBuySubmitted}, // backwards
	}
	for _, p := range illegal {
		if err := ValidateCycleTransition(p[0], p[1]); err == nil {
			t.Errorf("expected %s->%s to be illegal", p[0], p[1])
		} else if !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("%s->%s: want ErrInvalidTransition, got %v", p[0], p[1], err)
		}
	}
}

func TestCycleUnknownStateRejected(t *testing.T) {
	if err := ValidateCycleTransition("BOGUS", CycleNew); err == nil {
		t.Error("unknown from-state should be rejected")
	}
	if err := ValidateCycleTransition(CycleNew, "BOGUS"); err == nil {
		t.Error("unknown to-state should be rejected")
	}
}

func TestCycleHappyPathChainIsLegal(t *testing.T) {
	chain := []CycleState{
		CycleNew, CycleSignalDetected, CycleBuyRequestQueued, CycleBuySubmitted,
		CycleBuyFilled, CycleSellRequestQueued, CycleSellSubmitted, CycleSellFilled, CycleClosed,
	}
	for i := 0; i+1 < len(chain); i++ {
		if err := ValidateCycleTransition(chain[i], chain[i+1]); err != nil {
			t.Errorf("happy-path %s->%s should be legal: %v", chain[i], chain[i+1], err)
		}
	}
}

// ---- order transitions ----

func TestOrderLegalTransitionsFromMapAllValidate(t *testing.T) {
	for from, tos := range orderTransitions {
		for to := range tos {
			if err := ValidateOrderTransition(from, to); err != nil {
				t.Errorf("declared legal transition %s->%s rejected: %v", from, to, err)
			}
		}
	}
}

func TestOrderNoSelfLoops(t *testing.T) {
	for _, s := range AllOrderStates {
		if err := ValidateOrderTransition(s, s); err == nil {
			t.Errorf("self-loop %s->%s should be illegal", s, s)
		}
	}
}

func TestOrderTerminalStatesHaveNoExit(t *testing.T) {
	for _, term := range []OrderState{OrderFilled, OrderCancelled, OrderRejected, OrderExpired, OrderFailed} {
		if !IsTerminalOrder(term) {
			t.Errorf("%s should be terminal", term)
		}
		for _, to := range AllOrderStates {
			if err := ValidateOrderTransition(term, to); err == nil {
				t.Errorf("terminal %s must not transition to %s", term, to)
			}
		}
	}
}

func TestOrderNeedsReconcileEntryFromNonTerminal(t *testing.T) {
	for _, from := range AllOrderStates {
		err := ValidateOrderTransition(from, OrderNeedsReconcile)
		nonTerminalSource := !IsTerminalOrder(from) && from != OrderNeedsReconcile
		if nonTerminalSource && err != nil {
			t.Errorf("non-terminal %s should enter NEEDS_RECONCILE: %v", from, err)
		}
		if !nonTerminalSource && err == nil {
			t.Errorf("%s must NOT enter NEEDS_RECONCILE", from)
		}
	}
}

func TestOrderNeedsReconcileHasNoAutomaticExit(t *testing.T) {
	for _, to := range AllOrderStates {
		if err := ValidateOrderTransition(OrderNeedsReconcile, to); err == nil {
			t.Errorf("order NEEDS_RECONCILE must not transition to %s", to)
		}
	}
}

func TestOrderCancelPendingCanRaceFill(t *testing.T) {
	// A cancel may lose the race to a fill, so these must be legal.
	for _, to := range []OrderState{OrderCancelled, OrderFilled, OrderPartiallyFilled} {
		if err := ValidateOrderTransition(OrderCancelPending, to); err != nil {
			t.Errorf("CANCEL_PENDING->%s should be legal: %v", to, err)
		}
	}
}

func TestOrderSpecificIllegalTransitions(t *testing.T) {
	illegal := [][2]OrderState{
		{OrderNew, OrderFilled},
		{OrderRegistered, OrderSubmitted}, // must be QUEUED first
		{OrderFilled, OrderNew},
		{OrderCancelled, OrderSubmitted},
		{OrderQueued, OrderFilled}, // must be SUBMITTED first
	}
	for _, p := range illegal {
		if err := ValidateOrderTransition(p[0], p[1]); err == nil {
			t.Errorf("expected %s->%s to be illegal", p[0], p[1])
		}
	}
}

// ---- request status consistency ----

func TestRequestStatusVocabularyMatchesSchema(t *testing.T) {
	want := []RequestStatus{
		RequestQueued, RequestClaimed, RequestInFlight, RequestSucceeded,
		RequestFailed, RequestRetryScheduled, RequestDead,
	}
	if len(AllRequestStatuses) != len(want) {
		t.Fatalf("AllRequestStatuses len = %d, want %d", len(AllRequestStatuses), len(want))
	}
	for i := range want {
		if AllRequestStatuses[i] != want[i] {
			t.Errorf("AllRequestStatuses[%d] = %q, want %q", i, AllRequestStatuses[i], want[i])
		}
	}
	// Exact string values must match the DB ENUM in migration 005.
	if string(RequestInFlight) != "IN_FLIGHT" || string(RequestRetryScheduled) != "RETRY_SCHEDULED" {
		t.Error("request status string values drifted from the schema enum")
	}
	if !IsTerminalRequest(RequestSucceeded) || !IsTerminalRequest(RequestDead) || IsTerminalRequest(RequestQueued) {
		t.Error("IsTerminalRequest classification wrong")
	}
}
