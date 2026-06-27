package sellflow

import (
	"testing"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func TestSellPrice(t *testing.T) {
	// Binance ref 100, offset 30 bps -> 99.7, floored to tick 0.01 -> 99.70.
	if got := SellPrice(d("100"), 30, d("0.01")); !got.Equal(d("99.7")) {
		t.Errorf("SellPrice = %s, want 99.7", got)
	}
	// Floors to tick: 100 * (1 - 0.0033) = 99.67 -> floor to 0.1 -> 99.6.
	if got := SellPrice(d("100"), 33, d("0.1")); !got.Equal(d("99.6")) {
		t.Errorf("SellPrice tick-floor = %s, want 99.6", got)
	}
	// No tick -> exact.
	if got := SellPrice(d("100"), 100, decimal.Zero); !got.Equal(d("99")) {
		t.Errorf("SellPrice no-tick = %s, want 99", got)
	}
	// Negative offset clamps to 0.
	if got := SellPrice(d("100"), -50, decimal.Zero); !got.Equal(d("100")) {
		t.Errorf("SellPrice neg-offset = %s, want 100", got)
	}
}

func TestSnapDownToTick(t *testing.T) {
	if got := SnapDownToTick(d("99.678"), d("0.01")); !got.Equal(d("99.67")) {
		t.Errorf("snap = %s, want 99.67", got)
	}
	if got := SnapDownToTick(d("99.678"), decimal.Zero); !got.Equal(d("99.678")) {
		t.Errorf("no-tick snap = %s, want unchanged", got)
	}
}

func TestSnapQtyToStep(t *testing.T) {
	// 0.4567 floored to step 0.001 -> 0.456 (never rounds up past inventory).
	if got := SnapQtyToStep(d("0.4567"), d("0.001")); !got.Equal(d("0.456")) {
		t.Errorf("step snap = %s, want 0.456", got)
	}
	if got := SnapQtyToStep(d("0.4567"), decimal.Zero); !got.Equal(d("0.4567")) {
		t.Errorf("no-step snap = %s, want unchanged", got)
	}
}

func TestMeetsMinimums(t *testing.T) {
	// qty 0.5 @ price 100 = 50 notional.
	if !MeetsMinimums(d("0.5"), d("100"), d("0.1"), d("10")) {
		t.Error("0.5@100 should meet min qty 0.1 / min notional 10")
	}
	if MeetsMinimums(d("0.05"), d("100"), d("0.1"), d("0")) {
		t.Error("0.05 < min qty 0.1 should fail")
	}
	if MeetsMinimums(d("0.05"), d("100"), d("0"), d("10")) {
		t.Error("notional 5 < min 10 should fail")
	}
	if MeetsMinimums(d("0"), d("100"), d("0"), d("0")) {
		t.Error("zero qty must fail")
	}
	// No minimums configured -> any positive qty/price qualifies.
	if !MeetsMinimums(d("0.0001"), d("0.01"), d("0"), d("0")) {
		t.Error("no minimums -> should qualify")
	}
}
