package orders

import (
	"errors"
	"testing"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/execution"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func st(status execution.NormalizedOrderState, intended, filled, remaining, avg string) execution.OrderStatus {
	return execution.OrderStatus{
		Status: status, IntendedQty: d(intended), FilledQty: d(filled), RemainingQty: d(remaining), AvgPrice: d(avg),
	}
}

func TestClassifyMatrix(t *testing.T) {
	cases := []struct {
		name string
		st   execution.OrderStatus
		err  error
		want Classification
	}{
		{"full", st(execution.StateFilled, "10", "10", "0", "100"), nil, ClassFull},
		{"filled-but-remaining", st(execution.StateFilled, "10", "8", "2", "100"), nil, ClassAmbiguous},
		{"filled-but-zero-qty", st(execution.StateFilled, "10", "0", "0", "0"), nil, ClassAmbiguous},
		{"canceled-zero", st(execution.StateCanceled, "10", "0", "10", "0"), nil, ClassZero},
		{"canceled-partial", st(execution.StateCanceled, "10", "4", "6", "100"), nil, ClassPartial},
		{"canceled-partial-no-price", st(execution.StateCanceled, "10", "4", "6", "0"), nil, ClassAmbiguous},
		{"expired-zero", st(execution.StateExpired, "10", "0", "10", "0"), nil, ClassZero},
		{"expired-partial", st(execution.StateExpired, "10", "3", "7", "100"), nil, ClassPartial},
		{"partially-canceled-partial", st(execution.StatePartiallyCanceled, "10", "5", "5", "100"), nil, ClassPartial},
		{"partially-canceled-zero", st(execution.StatePartiallyCanceled, "10", "0", "10", "0"), nil, ClassZero},
		{"rejected-final-status-is-ambiguous", st(execution.StateRejected, "10", "0", "10", "0"), nil, ClassAmbiguous},
		{"still-partially-filled", st(execution.StatePartiallyFilled, "10", "5", "5", "100"), nil, ClassAmbiguous},
		{"still-open", st(execution.StateOpen, "10", "0", "10", "0"), nil, ClassAmbiguous},
		{"new", st(execution.StateNew, "10", "0", "10", "0"), nil, ClassAmbiguous},
		{"unknown-state", st(execution.StateUnknown, "10", "0", "10", "0"), nil, ClassAmbiguous},
		// A fetch error — even ErrOrderUnknown / "missing" — is never proof of zero fill.
		{"order-unknown-not-zero", st(execution.StateUnknown, "10", "0", "10", "0"), execution.ErrOrderUnknown, ClassAmbiguous},
		{"err-overrides-filled", st(execution.StateFilled, "10", "10", "0", "100"), errors.New("boom"), ClassAmbiguous},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Classify(c.st, c.err); got != c.want {
				t.Errorf("Classify(%s) = %s, want %s", c.name, got, c.want)
			}
		})
	}
}

func TestReleasesLock(t *testing.T) {
	// Only a proven zero-fill releases the lock.
	if !ClassZero.ReleasesLock() {
		t.Error("zero-fill should release the lock")
	}
	for _, c := range []Classification{ClassFull, ClassPartial, ClassAmbiguous} {
		if c.ReleasesLock() {
			t.Errorf("%s must NOT release the lock (exposure or unknown)", c)
		}
	}
}

func TestActualExecutionMode(t *testing.T) {
	for in, want := range map[string]string{"maker": "MAKER", "taker": "TAKER", "": "UNKNOWN", "weird": "UNKNOWN"} {
		if got := actualExecutionMode(in); got != want {
			t.Errorf("actualExecutionMode(%q) = %q, want %q", in, got, want)
		}
	}
}
