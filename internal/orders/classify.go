package orders

import (
	"github.com/shopspring/decimal"

	"v3TradeBot/internal/execution"
)

// usableAvgPrice returns a usable average fill price (the cost basis) and whether one exists:
// the venue's reported AvgPrice if positive, else derived as ExecutedQuote / FilledQty when
// both are positive. A full/partial fill with NO usable price AND no usable executed quote has
// no cost basis — it must be treated as ambiguous, never recorded as a clean fill (PR10 #7).
func usableAvgPrice(st execution.OrderStatus) (decimal.Decimal, bool) {
	if st.AvgPrice.IsPositive() {
		return st.AvgPrice, true
	}
	if st.ExecutedQuote.IsPositive() && st.FilledQty.IsPositive() {
		return st.ExecutedQuote.Div(st.FilledQty), true
	}
	return decimal.Zero, false
}

// Classification is the conservative verdict on a buy order's final status. It is
// pure (no DB) so the decision matrix is unit-tested without a database.
type Classification string

const (
	ClassFull      Classification = "full"      // fully filled
	ClassPartial   Classification = "partial"   // partially filled, remainder settled (cancelled/expired)
	ClassZero      Classification = "zero"      // POSITIVELY proven zero fill
	ClassAmbiguous Classification = "ambiguous" // unknown / contradictory / still-open -> NEEDS_RECONCILE
)

// ReasonZeroFill is recorded on the cycle when a simulated-IOC buy provably did not
// fill (a clean no-fill — CANCELLED, never FAILED).
const ReasonZeroFill = "SIMULATED_IOC_ZERO_FILL"

// Classify maps an exchange-reported final OrderStatus (and any fetch error) to a
// conservative classification. The cardinal rules (PROJECT_ARCHITECTURE.md §2a/§17):
//   - a status fetch error — INCLUDING ErrOrderUnknown / a missing order — is NEVER
//     proof of zero fill; it is Ambiguous → NEEDS_RECONCILE;
//   - "filled" with leftover/zero quantity is contradictory → Ambiguous;
//   - a partial fill with no usable price is incomplete → Ambiguous;
//   - anything still open / new / unknown is unresolved → Ambiguous.
func Classify(st execution.OrderStatus, statusErr error) Classification {
	if statusErr != nil {
		return ClassAmbiguous // missing/unknown/error — not proof of zero fill
	}
	switch st.Status {
	case execution.StateFilled:
		if st.RemainingQty.IsPositive() || !st.FilledQty.IsPositive() {
			return ClassAmbiguous // "filled" but qty doesn't agree
		}
		if _, ok := usableAvgPrice(st); !ok {
			return ClassAmbiguous // full fill but NO usable cost basis (avg price / executed quote)
		}
		return ClassFull

	case execution.StateCanceled, execution.StateExpired, execution.StatePartiallyCanceled:
		if st.FilledQty.IsZero() {
			return ClassZero // settled with zero fill
		}
		if _, ok := usableAvgPrice(st); st.FilledQty.IsPositive() && ok {
			return ClassPartial // settled with a usable partial fill (price reported or derived)
		}
		return ClassAmbiguous // some fill but incomplete detail (no usable cost basis)

	case execution.StatePartiallyFilled, execution.StateOpen, execution.StateNew, execution.StateUnknown:
		return ClassAmbiguous // not resolved after the cancel attempt

	case execution.StateRejected:
		// A "rejected" status AFTER a successful place+ack+cancel is contradictory
		// (rejections are detected at place time). Treat as ambiguous, never as proof
		// of no exposure. The place-time rejection path is handled separately
		// (OnPlaceRejected), where there really is no exposure.
		return ClassAmbiguous

	default:
		return ClassAmbiguous
	}
}

// ReleasesLock reports whether a final-status classification means there is
// positively NO exchange exposure, so the symbol lock can be safely released.
// Full/Partial keep the lock (inventory to sell); Ambiguous keeps it (unknown).
// Only a proven zero-fill releases it.
func (c Classification) ReleasesLock() bool { return c == ClassZero }

// actualExecutionMode maps the venue's liquidity flag to MAKER/TAKER/UNKNOWN. When
// the venue does not report it, the mode is UNKNOWN (never guessed).
func actualExecutionMode(liquidity string) string {
	switch liquidity {
	case "maker":
		return "MAKER"
	case "taker":
		return "TAKER"
	default:
		return "UNKNOWN"
	}
}
