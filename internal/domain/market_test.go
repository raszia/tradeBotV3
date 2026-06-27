package domain

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func TestNormalizeSymbol(t *testing.T) {
	cases := map[string]string{
		"btc/usdt":  "BTC/USDT",
		" ETH/IRT ": "ETH/IRT",
		"eth/irr":   "ETH/IRT", // IRR normalized to IRT
		"USDT/IRT":  "USDT/IRT",
	}
	for in, want := range cases {
		got, err := NormalizeSymbol(in)
		if err != nil {
			t.Errorf("NormalizeSymbol(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("NormalizeSymbol(%q) = %q, want %q", in, got, want)
		}
	}
	for _, bad := range []string{"", "BTC", "BTC/", "/USDT", "A/B/C"} {
		if _, err := NormalizeSymbol(bad); err == nil {
			t.Errorf("NormalizeSymbol(%q) should error", bad)
		}
	}
}

func TestQuoteAssetType(t *testing.T) {
	cases := map[string]string{
		"BTC/USDT": "USDT",
		"BTC/IRT":  "IRT",
		"BTC/IRR":  "IRT",
		"BTC/EUR":  "OTHER",
		"garbage":  "OTHER",
	}
	for in, want := range cases {
		if got := QuoteAssetType(in); got != want {
			t.Errorf("QuoteAssetType(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSymbolRulesRounding(t *testing.T) {
	r := SymbolRules{QuantityStep: d("0.001"), PriceTick: d("0.5")}
	if got := r.RoundQuantityDown(d("1.23456")); !got.Equal(d("1.234")) {
		t.Errorf("RoundQuantityDown = %s, want 1.234", got)
	}
	if got := r.RoundPriceDown(d("100.7")); !got.Equal(d("100.5")) {
		t.Errorf("RoundPriceDown = %s, want 100.5", got)
	}
	if got := r.RoundPriceUp(d("100.1")); !got.Equal(d("100.5")) {
		t.Errorf("RoundPriceUp = %s, want 100.5", got)
	}
	// Zero step/tick is a no-op.
	z := SymbolRules{}
	if got := z.RoundQuantityDown(d("1.23456")); !got.Equal(d("1.23456")) {
		t.Errorf("zero step should be no-op, got %s", got)
	}
}

func TestSymbolRulesValidate(t *testing.T) {
	r := SymbolRules{MinQuantity: d("0.01"), MaxQuantity: d("10"), MinNotional: d("50")}
	if err := r.Validate(d("0.5"), d("100")); err != nil {
		t.Errorf("valid order rejected: %v", err)
	}
	if err := r.Validate(d("0.001"), d("100")); err == nil {
		t.Error("below-min qty should fail")
	}
	if err := r.Validate(d("20"), d("100")); err == nil {
		t.Error("above-max qty should fail")
	}
	if err := r.Validate(d("0.5"), d("10")); err == nil {
		t.Error("below-min notional should fail")
	}
}

func TestOrderBookHelpers(t *testing.T) {
	now := time.Now()
	b := OrderBook{
		UpdatedAt: now,
		Bids:      []Level{{Price: d("100"), Quantity: d("1")}},
		Asks:      []Level{{Price: d("101"), Quantity: d("2")}},
	}
	if bid, ok := b.BestBid(); !ok || !bid.Price.Equal(d("100")) {
		t.Errorf("BestBid = %v ok=%v", bid, ok)
	}
	if ask, ok := b.BestAsk(); !ok || !ask.Price.Equal(d("101")) {
		t.Errorf("BestAsk = %v ok=%v", ask, ok)
	}
	if !b.Fresh(time.Minute, now.Add(time.Second)) {
		t.Error("book should be fresh")
	}
	if b.Fresh(time.Second, now.Add(time.Minute)) {
		t.Error("book should be stale")
	}
	var empty OrderBook
	if _, ok := empty.BestBid(); ok {
		t.Error("empty book has no best bid")
	}
}
