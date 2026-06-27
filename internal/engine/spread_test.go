package engine

import (
	"reflect"
	"testing"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/events"
)

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func TestComputeSpreadBasic(t *testing.T) {
	// Iranian ask 100, Binance bid 101 -> raw 100 bps; fees 10+10 -> adjusted 80.
	r := ComputeSpread(SpreadInputs{
		IranianAsk: dec("100"), BinanceRef: dec("101"),
		BuyFeeBps: dec("10"), SellFeeBps: dec("10"),
	})
	if !r.OK {
		t.Fatal("expected OK")
	}
	if !r.SpreadBps.Equal(dec("100")) {
		t.Errorf("spread = %s, want 100", r.SpreadBps)
	}
	if !r.FeeAdjustedBps.Equal(dec("80")) {
		t.Errorf("fee-adjusted = %s, want 80", r.FeeAdjustedBps)
	}
}

func TestComputeSpreadNoEdgeIsNegative(t *testing.T) {
	// Iranian ask above Binance bid -> negative spread (no edge).
	r := ComputeSpread(SpreadInputs{IranianAsk: dec("102"), BinanceRef: dec("101")})
	if !r.OK || !r.SpreadBps.IsNegative() {
		t.Errorf("expected negative spread, got %s ok=%v", r.SpreadBps, r.OK)
	}
}

func TestComputeSpreadInvalidAsk(t *testing.T) {
	for _, ask := range []string{"0", "-1"} {
		if r := ComputeSpread(SpreadInputs{IranianAsk: dec(ask), BinanceRef: dec("101")}); r.OK {
			t.Errorf("ask %s should be invalid", ask)
		}
	}
}

func TestFeeFractionToBps(t *testing.T) {
	if got := feeFractionToBps(dec("0.001")); !got.Equal(dec("10")) {
		t.Errorf("0.001 -> %s bps, want 10", got)
	}
	if got := feeFractionToBps(dec("0")); !got.Equal(dec("0")) {
		t.Errorf("0 -> %s bps, want 0", got)
	}
}

func TestRoundToInt(t *testing.T) {
	cases := map[string]int{"99.4": 99, "99.6": 100, "-12.5": -13, "100": 100}
	for in, want := range cases {
		if got := roundToInt(dec(in)); got != want {
			t.Errorf("roundToInt(%s) = %d, want %d", in, got, want)
		}
	}
}

func TestSymbolSplit(t *testing.T) {
	if baseOf("BTC/IRT") != "BTC" || quoteOf("BTC/IRT") != "IRT" {
		t.Error("BTC/IRT split wrong")
	}
	if baseOf("ETH/USDT") != "ETH" || quoteOf("ETH/USDT") != "USDT" {
		t.Error("ETH/USDT split wrong")
	}
	if baseOf("NOSLASH") != "NOSLASH" || quoteOf("NOSLASH") != "" {
		t.Error("no-slash handling wrong")
	}
}

func TestIsUSDTQuote(t *testing.T) {
	for _, q := range []string{"USDT", "usdt", "USD", "USDC"} {
		if !isUSDTQuote(q) {
			t.Errorf("%s should be a USDT-like quote", q)
		}
	}
	for _, q := range []string{"IRT", "IRR", "irt", ""} {
		if isUSDTQuote(q) {
			t.Errorf("%s should NOT be a USDT-like quote", q)
		}
	}
}

// TestTargetsRespectVersionAndFlags covers the event-fan-out logic offline: an
// unconfigured (v0) system yields nothing, and a reference-venue event re-evaluates
// only the signal-enabled Iranian markets on the same base asset.
func TestTargetsRespectVersionAndFlags(t *testing.T) {
	e := New(nil, nil, nil, nil, nil, Config{})

	// Version 0 => no signal at all, regardless of seeded markets.
	v0 := &configstore.Snapshot{Version: 0}
	if got := e.targets(v0, events.MarketEvent{Exchange: "binance", Symbol: "BTC/USDT"}); got != nil {
		t.Errorf("v0 must yield no targets, got %d", len(got))
	}

	markets := map[int64]configstore.MarketConfig{
		1: {ExchangeMarketID: 1, ExchangeCode: "nobitex", CanonicalSymbol: "BTC/IRT", EnabledForSignal: true},
		2: {ExchangeMarketID: 2, ExchangeCode: "nobitex", CanonicalSymbol: "ETH/IRT", EnabledForSignal: true},
		3: {ExchangeMarketID: 3, ExchangeCode: "wallex", CanonicalSymbol: "BTC/IRT", EnabledForSignal: false},  // disabled
		4: {ExchangeMarketID: 4, ExchangeCode: "binance", CanonicalSymbol: "BTC/USDT", EnabledForSignal: true}, // the reference itself
	}
	bySym := map[string][]configstore.MarketConfig{}
	for _, m := range markets {
		bySym[m.CanonicalSymbol] = append(bySym[m.CanonicalSymbol], m)
	}
	snap := &configstore.Snapshot{Version: 1, MarketsByID: markets, MarketsBySymbol: bySym}

	// A Binance (reference) BTC tick re-evaluates only signal-enabled Iranian BTC
	// markets: market 1 (yes), not 2 (ETH), not 3 (disabled), not 4 (the reference).
	got := e.targets(snap, events.MarketEvent{Exchange: "binance", Symbol: "BTC/USDT"})
	if len(got) != 1 || got[0].ExchangeMarketID != 1 {
		t.Errorf("binance BTC tick targets = %+v, want only market 1", got)
	}

	// A direct Iranian tick evaluates just that market (if signal-enabled).
	got = e.targets(snap, events.MarketEvent{Exchange: "nobitex", Symbol: "ETH/IRT"})
	if len(got) != 1 || got[0].ExchangeMarketID != 2 {
		t.Errorf("nobitex ETH tick targets = %+v, want only market 2", got)
	}
	// A disabled Iranian market never targets.
	got = e.targets(snap, events.MarketEvent{Exchange: "wallex", Symbol: "BTC/IRT"})
	if len(got) != 0 {
		t.Errorf("disabled market must not target, got %d", len(got))
	}
}

// TestEngineHoldsNoExchangeClient is a structural guard for the PR8 boundary: the
// engine must never be able to send orders. None of its fields may satisfy an
// order-mutating interface (no PlaceOrder/CancelOrder reachable by construction).
func TestEngineHoldsNoExchangeClient(t *testing.T) {
	type orderSender interface {
		PlaceOrder(any) any
	}
	type orderCanceller interface {
		CancelOrder(any) any
	}
	et := reflect.TypeOf(Engine{})
	sender := reflect.TypeOf((*orderSender)(nil)).Elem()
	canceller := reflect.TypeOf((*orderCanceller)(nil)).Elem()
	for i := 0; i < et.NumField(); i++ {
		ft := et.Field(i).Type
		if ft.Implements(sender) || ft.Implements(canceller) {
			t.Errorf("engine field %q can place/cancel orders — PR8 forbids it", et.Field(i).Name)
		}
	}
}
