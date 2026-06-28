package configstore

import "testing"

func ip(n int) *int       { return &n }
func sp(s string) *string { return &s }
func bp(b bool) *bool     { return &b }

func TestSymbolConfigValidate(t *testing.T) {
	// A fully valid update passes.
	ok := SymbolConfigUpdate{
		MinSpreadBps: ip(40), BuySize: sp("0.5"), BuySizeUnit: sp("base"), SellOffsetBps: ip(20),
		RepriceIntervalSeconds: ip(5), OrderTimeoutMs: ip(3000), MaxRetries: ip(3), RetryBackoffMs: ip(500),
		MakerFirstEnabled: bp(true), MakerAttemptsBeforeTaker: ip(2), MakerSignalWindowSeconds: ip(60),
		MakerWaitBeforeCancelMs: ip(2000), MakerPriceOffsetBps: ip(5), TakerPriceMode: sp("ASK"), MaxTakerSlippageBps: ip(40),
	}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid update rejected: %v", err)
	}

	bad := []struct {
		name string
		u    SymbolConfigUpdate
	}{
		{"min_spread<0", SymbolConfigUpdate{MinSpreadBps: ip(-1)}},
		{"buy_size=0", SymbolConfigUpdate{BuySize: sp("0")}},
		{"buy_size non-numeric", SymbolConfigUpdate{BuySize: sp("abc")}},
		{"bad unit", SymbolConfigUpdate{BuySizeUnit: sp("xyz")}},
		{"sell_offset<0", SymbolConfigUpdate{SellOffsetBps: ip(-1)}},
		{"reprice<0", SymbolConfigUpdate{RepriceIntervalSeconds: ip(-1)}},
		{"timeout=0", SymbolConfigUpdate{OrderTimeoutMs: ip(0)}},
		{"max_retries<0", SymbolConfigUpdate{MaxRetries: ip(-1)}},
		{"maker_attempts<0", SymbolConfigUpdate{MakerAttemptsBeforeTaker: ip(-1)}},
		{"maker_window=0", SymbolConfigUpdate{MakerSignalWindowSeconds: ip(0)}},
		{"maker_wait<0", SymbolConfigUpdate{MakerWaitBeforeCancelMs: ip(-1)}},
		{"maker_offset<0", SymbolConfigUpdate{MakerPriceOffsetBps: ip(-1)}},
		{"slippage<0", SymbolConfigUpdate{MaxTakerSlippageBps: ip(-1)}},
		{"bad taker mode", SymbolConfigUpdate{TakerPriceMode: sp("BID")}},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			if err := c.u.Validate(); err == nil {
				t.Errorf("%s should be rejected", c.name)
			} else if !IsValidation(err) {
				t.Errorf("%s should be a ValidationError, got %T", c.name, err)
			}
		})
	}
}

func TestExchangeConfigValidate(t *testing.T) {
	if err := (ExchangeConfigUpdate{MaxConcurrentRequests: ip(2), RequestTimeoutMs: ip(5000)}).Validate(); err != nil {
		t.Fatalf("valid exchange config rejected: %v", err)
	}
	for _, u := range []ExchangeConfigUpdate{
		{MaxConcurrentRequests: ip(0)},
		{RequestTimeoutMs: ip(0)},
		{MaxRetries: ip(-1)},
		{RetryBackoffMs: ip(-1)},
		{RateLimitPerSec: ip(-1)},
	} {
		if err := u.Validate(); !IsValidation(err) {
			t.Errorf("invalid exchange config %+v should be a ValidationError, got %v", u, err)
		}
	}
}
