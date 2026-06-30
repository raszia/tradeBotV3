package configstore

import (
	"testing"

	"github.com/shopspring/decimal"
)

// TestFeeForScopingAndPriority (PR6 correction) verifies that default fees are scoped per
// exchange (two exchanges' defaults coexist without overwriting each other), that FeeFor
// resolves the correct exchange's default, that a market-specific override wins, and that
// no exchange ever sees another exchange's default.
func TestFeeForScopingAndPriority(t *testing.T) {
	dec := decimal.RequireFromString
	snap := &Snapshot{
		FeesByMarketID: map[int64]FeeConfig{
			100: {ExchangeID: 1, ExchangeMarketID: 100, MakerFee: dec("0.005"), TakerFee: dec("0.006")},
		},
		DefaultFeesByExchangeID: map[int64]FeeConfig{
			1: {ExchangeID: 1, MakerFee: dec("0.001"), TakerFee: dec("0.002")},
			2: {ExchangeID: 2, MakerFee: dec("0.003"), TakerFee: dec("0.004")},
		},
	}

	// Both exchange defaults survive — no shared "key 0" collision.
	if !snap.DefaultFeesByExchangeID[1].MakerFee.Equal(dec("0.001")) {
		t.Error("exchange 1 default fee was lost")
	}
	if !snap.DefaultFeesByExchangeID[2].MakerFee.Equal(dec("0.003")) {
		t.Error("exchange 2 default fee was lost / overwritten")
	}

	// FeeFor returns the DEFAULT fee for the correct exchange when there is no override.
	if f, ok := snap.FeeFor(1, 0); !ok || !f.MakerFee.Equal(dec("0.001")) {
		t.Errorf("FeeFor(1, default) = %v ok=%v, want 0.001", f.MakerFee, ok)
	}
	if f, ok := snap.FeeFor(2, 0); !ok || !f.MakerFee.Equal(dec("0.003")) {
		t.Errorf("FeeFor(2, default) = %v ok=%v, want 0.003", f.MakerFee, ok)
	}
	// Never use another exchange's default.
	f1, _ := snap.FeeFor(1, 0)
	f2, _ := snap.FeeFor(2, 0)
	if f1.MakerFee.Equal(f2.MakerFee) {
		t.Error("two exchanges resolved to the SAME default fee (cross-exchange leak)")
	}

	// A market-specific fee takes priority over the exchange default.
	if f, ok := snap.FeeFor(1, 100); !ok || !f.MakerFee.Equal(dec("0.005")) {
		t.Errorf("FeeFor(1, 100) = %v, want the market override 0.005", f.MakerFee)
	}
	// An override MISS falls back to the same exchange's default (not another exchange's).
	if f, ok := snap.FeeFor(1, 999); !ok || !f.MakerFee.Equal(dec("0.001")) {
		t.Errorf("FeeFor(1, missing override) = %v, want fallback to exchange-1 default 0.001", f.MakerFee)
	}
	// Unknown exchange with no override / default -> not found.
	if _, ok := snap.FeeFor(99, 0); ok {
		t.Error("FeeFor(unknown exchange) should be (·, false)")
	}
	// nil snapshot is safe.
	var nilSnap *Snapshot
	if _, ok := nilSnap.FeeFor(1, 0); ok {
		t.Error("nil snapshot FeeFor should be (·, false)")
	}
}
