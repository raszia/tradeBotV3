package regime

import (
	"errors"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }
func sym(n string, w int) SymbolWeight {
	return SymbolWeight{Symbol: n, Weight: decimal.NewFromInt(int64(w))}
}
func tf(l string, sec, w int) Timeframe {
	return Timeframe{Label: l, Seconds: sec, Weight: decimal.NewFromInt(int64(w))}
}

// scoreSeries builds a 2-sample series (now + 1h ago) yielding the given bps score
// over a 1h timeframe: current = 100*(1 + score/10000), reference (1h ago) = 100.
func scoreSeries(scoreBps int) []Sample {
	ref := dec("100")
	cur := ref.Mul(bps.Add(decimal.NewFromInt(int64(scoreBps)))).Div(bps)
	return []Sample{{Age: 0, Price: cur}, {Age: time.Hour, Price: ref}}
}

func basket1(neutral, moderate, strong int) Basket {
	return Basket{
		ID: 1, Name: "b", Symbols: []SymbolWeight{sym("BTC/USDT", 1)}, Timeframes: []Timeframe{tf("1h", 3600, 1)},
		NeutralBandBps: neutral, ModerateBps: moderate, StrongBps: strong, ConfigVersion: 7,
	}
}

// TestBasketValidate is the offline config-rejection matrix (reviewer's required cases).
// Only genuinely invalid values are rejected; a valid basket passes.
func TestBasketValidate(t *testing.T) {
	valid := basket1(5, 30, 100) // neutral<=moderate<=strong, positive symbol/tf weights

	// negative weight (decimal) helper.
	negW := func(n string, w string) SymbolWeight { return SymbolWeight{Symbol: n, Weight: dec(w)} }

	cases := []struct {
		name    string
		mutate  func(b *Basket)
		wantErr bool
	}{
		{"valid", func(*Basket) {}, false},
		// 1. zero / negative symbol weight.
		{"zero-symbol-weight", func(b *Basket) { b.Symbols = []SymbolWeight{negW("BTC/USDT", "0")} }, true},
		{"negative-symbol-weight", func(b *Basket) { b.Symbols = []SymbolWeight{negW("BTC/USDT", "-1")} }, true},
		// 2. zero / negative timeframe seconds; and zero timeframe weight.
		{"zero-tf-seconds", func(b *Basket) { b.Timeframes = []Timeframe{tf("1h", 0, 1)} }, true},
		{"negative-tf-seconds", func(b *Basket) { b.Timeframes = []Timeframe{tf("1h", -60, 1)} }, true},
		{"zero-tf-weight", func(b *Basket) { b.Timeframes = []Timeframe{tf("1h", 3600, 0)} }, true},
		// 3. negative neutral band.
		{"negative-neutral", func(b *Basket) { b.NeutralBandBps = -1 }, true},
		// 4. moderate < neutral.
		{"moderate-below-neutral", func(b *Basket) { b.NeutralBandBps, b.ModerateBps = 30, 10 }, true},
		// 5. strong < moderate.
		{"strong-below-moderate", func(b *Basket) { b.ModerateBps, b.StrongBps = 50, 20 }, true},
		// negative update interval.
		{"negative-interval", func(b *Basket) { b.UpdateIntervalSeconds = -1 }, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := valid
			b.Symbols = append([]SymbolWeight(nil), valid.Symbols...)
			b.Timeframes = append([]Timeframe(nil), valid.Timeframes...)
			c.mutate(&b)
			err := b.Validate()
			if c.wantErr && err == nil {
				t.Errorf("%s: Validate() = nil, want error", c.name)
			}
			if !c.wantErr && err != nil {
				t.Errorf("%s: Validate() = %v, want nil", c.name, err)
			}
			if c.wantErr && err != nil && !errors.Is(err, ErrInvalidBasketConfig) {
				t.Errorf("%s: error %v does not wrap ErrInvalidBasketConfig", c.name, err)
			}
		})
	}
}

func TestCalculateBullishStrong(t *testing.T) {
	b := basket1(5, 30, 100)
	r := Calculate(b, map[string][]Sample{"BTC/USDT": scoreSeries(1000)}, time.Minute)
	if r.Direction != DirBullish || r.Level != LvlStrong {
		t.Errorf("direction/level = %s/%s, want BULLISH/STRONG", r.Direction, r.Level)
	}
	if !r.ScoreBps.Round(2).Equal(dec("1000")) {
		t.Errorf("score = %s, want 1000 bps", r.ScoreBps)
	}
	if !r.Confidence.Equal(dec("1")) {
		t.Errorf("confidence = %s, want 1", r.Confidence)
	}
	if _, ok := r.TimeframeScores["1h"]; !ok {
		t.Error("missing 1h timeframe score")
	}
	if r.StaleReason != "" {
		t.Errorf("unexpected stale reason: %s", r.StaleReason)
	}
}

func TestThresholdMapping(t *testing.T) {
	b := basket1(5, 30, 100)
	cases := []struct {
		score   int
		wantDir string
		wantLvl string
	}{
		{3, DirNeutral, LvlFlat},
		{-3, DirNeutral, LvlFlat},
		{20, DirBullish, LvlWeak},
		{-20, DirBearish, LvlWeak},
		{50, DirBullish, LvlModerate},
		{-50, DirBearish, LvlModerate},
		{150, DirBullish, LvlStrong},
		{-150, DirBearish, LvlStrong},
	}
	for _, c := range cases {
		r := Calculate(b, map[string][]Sample{"BTC/USDT": scoreSeries(c.score)}, time.Minute)
		if r.Direction != c.wantDir || r.Level != c.wantLvl {
			t.Errorf("score %d -> %s/%s, want %s/%s", c.score, r.Direction, r.Level, c.wantDir, c.wantLvl)
		}
	}
}

func TestWeightedSymbolContribution(t *testing.T) {
	// BTC weight 3 @ +100 bps, ETH weight 1 @ -100 bps -> tf score (3*100-1*100)/4 = +50.
	b := Basket{
		ID: 1, Symbols: []SymbolWeight{sym("BTC/USDT", 3), sym("ETH/USDT", 1)},
		Timeframes: []Timeframe{tf("1h", 3600, 1)}, NeutralBandBps: 5, ModerateBps: 30, StrongBps: 100,
	}
	r := Calculate(b, map[string][]Sample{"BTC/USDT": scoreSeries(100), "ETH/USDT": scoreSeries(-100)}, time.Minute)
	if !r.ScoreBps.Round(2).Equal(dec("50")) {
		t.Errorf("weighted score = %s, want 50", r.ScoreBps)
	}
	if r.Direction != DirBullish || r.Level != LvlModerate {
		t.Errorf("dir/lvl = %s/%s, want BULLISH/MODERATE", r.Direction, r.Level)
	}
	if !r.SymbolContributions["BTC/USDT"].Round(2).Equal(dec("100")) || !r.SymbolContributions["ETH/USDT"].Round(2).Equal(dec("-100")) {
		t.Errorf("contributions = %v", r.SymbolContributions)
	}
}

func TestTimeframeWeighting(t *testing.T) {
	// tf A (weight 3) score +100, tf B (weight 1) score -100 -> overall (3*100-1*100)/4 = 50.
	b := Basket{
		ID: 1, Symbols: []SymbolWeight{sym("BTC/USDT", 1)},
		Timeframes: []Timeframe{tf("A", 3600, 3), tf("B", 7200, 1)}, NeutralBandBps: 5, ModerateBps: 30, StrongBps: 100,
	}
	// current 0, ref@1h=100 (-> +100 over A), ref@2h such that score = -100 over B.
	cur := dec("101")
	refA := dec("100")                             // (101-100)/100 = +100 bps
	refB := cur.Mul(bps).Div(bps.Sub(dec("-100"))) // current/(1 - 100/10000) ... solve (cur-ref)/ref=-100/10000
	_ = refB
	// Easier: build explicit refs. score_B = -100 => ref = cur/(1 + (-100)/10000) = cur/0.99.
	refBcalc := cur.Div(dec("0.99"))
	series := []Sample{{Age: 0, Price: cur}, {Age: time.Hour, Price: refA}, {Age: 2 * time.Hour, Price: refBcalc}}
	r := Calculate(b, map[string][]Sample{"BTC/USDT": series}, time.Minute)
	if !r.ScoreBps.Round(0).Equal(dec("50")) {
		t.Errorf("timeframe-weighted score = %s, want ~50", r.ScoreBps)
	}
}

func TestMissingSymbolReducesConfidence(t *testing.T) {
	b := Basket{
		ID: 1, Symbols: []SymbolWeight{sym("BTC/USDT", 1), sym("ETH/USDT", 1)},
		Timeframes: []Timeframe{tf("1h", 3600, 1)}, NeutralBandBps: 5, ModerateBps: 30, StrongBps: 100,
	}
	// ETH absent from the series entirely.
	r := Calculate(b, map[string][]Sample{"BTC/USDT": scoreSeries(40)}, time.Minute)
	if r.Direction != DirBullish {
		t.Errorf("direction = %s, want BULLISH from the one fresh symbol", r.Direction)
	}
	if !r.Confidence.Equal(dec("0.5")) {
		t.Errorf("confidence = %s, want 0.5 (1 of 2 symbols)", r.Confidence)
	}
	if r.StaleReason == "" {
		t.Error("expected a stale reason noting the missing symbol")
	}
}

func TestStaleSymbolExcluded(t *testing.T) {
	b := basket1(5, 30, 100)
	// freshest sample is 5 minutes old, maxAge is 1 minute -> stale -> excluded -> UNKNOWN.
	stale := []Sample{{Age: 5 * time.Minute, Price: dec("110")}, {Age: time.Hour, Price: dec("100")}}
	r := Calculate(b, map[string][]Sample{"BTC/USDT": stale}, time.Minute)
	if r.Direction != DirUnknown || r.StaleReason == "" {
		t.Errorf("stale data must yield UNKNOWN with a reason, got %s / %q", r.Direction, r.StaleReason)
	}
	if !r.Confidence.IsZero() {
		t.Errorf("confidence = %s, want 0 for all-stale", r.Confidence)
	}
}

func TestNoFreshDataUnknown(t *testing.T) {
	b := basket1(5, 30, 100)
	r := Calculate(b, map[string][]Sample{}, time.Minute)
	if r.Direction != DirUnknown || r.Level != LvlUnknown || !r.Confidence.IsZero() {
		t.Errorf("no data -> UNKNOWN/UNKNOWN/0, got %s/%s/%s", r.Direction, r.Level, r.Confidence)
	}
}

func TestInsufficientHistoryForTimeframe(t *testing.T) {
	b := basket1(5, 30, 100) // timeframe 1h
	// Series only spans 10 minutes -> cannot represent a 1h timeframe -> insufficient.
	short := []Sample{{Age: 0, Price: dec("110")}, {Age: 10 * time.Minute, Price: dec("100")}}
	r := Calculate(b, map[string][]Sample{"BTC/USDT": short}, time.Minute)
	if r.Direction != DirUnknown || r.StaleReason == "" {
		t.Errorf("insufficient history -> UNKNOWN + reason, got %s / %q", r.Direction, r.StaleReason)
	}
}

func TestEmptyBasketIsUnknown(t *testing.T) {
	r := Calculate(Basket{ID: 1}, map[string][]Sample{}, time.Minute)
	if r.Direction != DirUnknown || r.StaleReason == "" {
		t.Errorf("empty basket -> UNKNOWN + reason, got %s / %q", r.Direction, r.StaleReason)
	}
}
