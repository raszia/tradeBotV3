// Package regime computes a normalized market regime (direction/level/confidence)
// from Binance market data. It reads Binance prices ONLY from the existing Redis
// market-data cache (never calls Binance), reads basket config from MariaDB, and
// writes regime current/history to MariaDB. It makes NO trading decisions and never
// touches cycles/orders/queue/locks/trading-config (a later trade-engine PR may
// consume the regime, but PR15 only computes + stores it).
//
// The scoring is multi-timeframe momentum: for each basket symbol, the percent change
// (in bps) between the current price and a price ~T ago is the symbol's score for
// timeframe T; these are weighted across symbols (per symbol weight) into a
// per-timeframe score, then across timeframes (per timeframe weight) into the basket
// score. Direction/level come from the score vs the configured thresholds; confidence
// reflects how much fresh data backed the calculation.
package regime

import (
	"sort"
	"time"

	"github.com/shopspring/decimal"
)

// Direction / Level vocabularies (fixed; never adapter/basket-invented).
const (
	DirBullish = "BULLISH"
	DirBearish = "BEARISH"
	DirNeutral = "NEUTRAL"
	DirUnknown = "UNKNOWN"

	LvlStrong   = "STRONG"
	LvlModerate = "MODERATE"
	LvlWeak     = "WEAK"
	LvlFlat     = "FLAT"
	LvlUnknown  = "UNKNOWN"
)

var bps = decimal.NewFromInt(10000)

// SymbolWeight is one basket symbol and its weight.
type SymbolWeight struct {
	Symbol string
	Weight decimal.Decimal
}

// Timeframe is one lookback window and its weight.
type Timeframe struct {
	Label   string
	Seconds int
	Weight  decimal.Decimal
}

// Basket is the full regime configuration for one basket.
type Basket struct {
	ID                    int64
	Name                  string
	Symbols               []SymbolWeight
	Timeframes            []Timeframe
	NeutralBandBps        int
	ModerateBps           int
	StrongBps             int
	UpdateIntervalSeconds int
	ConfigVersion         int64
}

// Sample is one observed price at a given age (now − observedAt). The freshest sample
// (smallest Age) is the current price; older samples are timeframe references.
type Sample struct {
	Age   time.Duration
	Price decimal.Decimal
}

// Result is the computed regime.
type Result struct {
	Direction           string
	Level               string
	Confidence          decimal.Decimal            // 0..1
	ScoreBps            decimal.Decimal            // signed basket momentum, bps
	TimeframeScores     map[string]decimal.Decimal // label -> bps
	SymbolContributions map[string]decimal.Decimal // symbol -> its multi-timeframe score, bps
	StaleReason         string                     // non-empty when data was missing/stale
}

// Calculate computes the regime for a basket from per-symbol price series. A series
// must be the recent observations for that symbol (any order). maxAge is the
// freshness threshold for the CURRENT price; a symbol whose freshest sample is older
// than maxAge (or absent) is excluded (lowering confidence). When no symbol has fresh
// data the regime is UNKNOWN with a stale reason — stale data is never silently
// treated as valid.
func Calculate(b Basket, series map[string][]Sample, maxAge time.Duration) Result {
	res := Result{
		Direction: DirUnknown, Level: LvlUnknown, Confidence: decimal.Zero,
		TimeframeScores: map[string]decimal.Decimal{}, SymbolContributions: map[string]decimal.Decimal{},
	}
	if len(b.Symbols) == 0 || len(b.Timeframes) == 0 {
		res.StaleReason = "basket has no symbols or timeframes"
		return res
	}

	// Per symbol: current price (freshest fresh sample) + a sorted-by-age view.
	type symData struct {
		current decimal.Decimal
		samples []Sample // sorted ascending by Age
		fresh   bool
	}
	sd := make(map[string]symData, len(b.Symbols))
	freshSymbols := 0
	staleSymbols := 0
	for _, sw := range b.Symbols {
		s := series[sw.Symbol]
		if len(s) == 0 {
			staleSymbols++
			continue
		}
		sorted := append([]Sample(nil), s...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].Age < sorted[j].Age })
		cur := sorted[0]
		if cur.Age > maxAge || !cur.Price.IsPositive() {
			staleSymbols++
			continue
		}
		sd[sw.Symbol] = symData{current: cur.Price, samples: sorted, fresh: true}
		freshSymbols++
	}
	if freshSymbols == 0 {
		res.StaleReason = "no basket symbol has fresh data"
		return res
	}

	// Per timeframe: weighted-average symbol score; track per-symbol accumulation for
	// contributions; track how many timeframes had usable data (for confidence).
	timeframesWithData := 0
	overallNum := decimal.Zero // Σ tf_weight * tf_score
	overallDen := decimal.Zero // Σ tf_weight (with data)
	// per-symbol multi-timeframe accumulation for contributions.
	symNum := map[string]decimal.Decimal{}
	symDen := map[string]decimal.Decimal{}
	insufficientTF := 0

	for _, tf := range b.Timeframes {
		target := time.Duration(tf.Seconds) * time.Second
		tfNum := decimal.Zero // Σ sym_weight * sym_score
		tfDen := decimal.Zero // Σ sym_weight (with data)
		for _, sw := range b.Symbols {
			d, ok := sd[sw.Symbol]
			if !ok {
				continue
			}
			ref, ok := referencePrice(d.samples, target)
			if !ok {
				continue // series doesn't span this timeframe yet for this symbol
			}
			score := d.current.Sub(ref).Div(ref).Mul(bps) // bps change
			tfNum = tfNum.Add(sw.Weight.Mul(score))
			tfDen = tfDen.Add(sw.Weight)
			symNum[sw.Symbol] = symNum[sw.Symbol].Add(tf.Weight.Mul(score))
			symDen[sw.Symbol] = symDen[sw.Symbol].Add(tf.Weight)
		}
		if tfDen.IsZero() {
			insufficientTF++
			continue
		}
		tfScore := tfNum.Div(tfDen)
		res.TimeframeScores[tf.Label] = tfScore
		overallNum = overallNum.Add(tf.Weight.Mul(tfScore))
		overallDen = overallDen.Add(tf.Weight)
		timeframesWithData++
	}

	for sym, num := range symNum {
		if den := symDen[sym]; den.IsPositive() {
			res.SymbolContributions[sym] = num.Div(den)
		}
	}

	if timeframesWithData == 0 || overallDen.IsZero() {
		res.StaleReason = "insufficient price history to span any timeframe"
		return res
	}

	score := overallNum.Div(overallDen)
	res.ScoreBps = score
	res.Direction, res.Level = classify(score, b.NeutralBandBps, b.ModerateBps, b.StrongBps)

	// Confidence = (fresh symbols / total) * (timeframes with data / total). 0..1.
	symFrac := decimal.NewFromInt(int64(freshSymbols)).Div(decimal.NewFromInt(int64(len(b.Symbols))))
	tfFrac := decimal.NewFromInt(int64(timeframesWithData)).Div(decimal.NewFromInt(int64(len(b.Timeframes))))
	res.Confidence = symFrac.Mul(tfFrac).Round(6)

	if staleSymbols > 0 || insufficientTF > 0 {
		res.StaleReason = staleReason(staleSymbols, len(b.Symbols), insufficientTF, len(b.Timeframes))
	}
	return res
}

// referencePrice returns the price of the sample whose age is closest to target,
// requiring the series to actually span at least ~half the target (so a too-short
// series doesn't fabricate a momentum). samples must be sorted ascending by Age.
func referencePrice(samples []Sample, target time.Duration) (decimal.Decimal, bool) {
	if len(samples) == 0 {
		return decimal.Zero, false
	}
	oldest := samples[len(samples)-1].Age
	if oldest < target/2 {
		return decimal.Zero, false // not enough history to represent this timeframe
	}
	best := samples[0]
	bestDiff := absDur(best.Age - target)
	for _, s := range samples[1:] {
		if d := absDur(s.Age - target); d < bestDiff {
			best, bestDiff = s, d
		}
	}
	if !best.Price.IsPositive() {
		return decimal.Zero, false
	}
	return best.Price, true
}

// classify maps a signed bps score to (direction, level) via the thresholds.
func classify(score decimal.Decimal, neutralBps, moderateBps, strongBps int) (string, string) {
	mag := score.Abs()
	neutral := decimal.NewFromInt(int64(neutralBps))
	moderate := decimal.NewFromInt(int64(moderateBps))
	strong := decimal.NewFromInt(int64(strongBps))

	if mag.LessThanOrEqual(neutral) {
		return DirNeutral, LvlFlat
	}
	dir := DirBullish
	if score.IsNegative() {
		dir = DirBearish
	}
	switch {
	case mag.GreaterThanOrEqual(strong):
		return dir, LvlStrong
	case mag.GreaterThanOrEqual(moderate):
		return dir, LvlModerate
	default:
		return dir, LvlWeak
	}
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func staleReason(staleSym, totalSym, insufTF, totalTF int) string {
	out := ""
	if staleSym > 0 {
		out = itoa(staleSym) + "/" + itoa(totalSym) + " symbols stale/missing"
	}
	if insufTF > 0 {
		if out != "" {
			out += "; "
		}
		out += itoa(insufTF) + "/" + itoa(totalTF) + " timeframes lacked history"
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
