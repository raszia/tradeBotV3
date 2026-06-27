// Package engine is the trade-engine signal loop. It consumes market events,
// reads the latest books/prices from Redis and config from the in-memory
// configstore cache, computes the spread, and writes comparison_events / signals.
//
// HARD boundaries (PR8): the engine NEVER calls an exchange (no private clients,
// no PlaceOrder/CancelOrder). It does not create cycles/orders (PR9). It may
// update/remove an existing not-yet-claimed QUEUED buy request to avoid duplicate
// pending intent (§2a), but never sends anything.
package engine

import (
	"github.com/shopspring/decimal"
)

var bps = decimal.NewFromInt(10000)

// SpreadInputs are unit-normalized prices for one comparison. IranianAsk and
// BinanceRef MUST already be in the SAME quote unit (the caller converts IRT/IRR
// markets using a USDT/IRT rate — see the engine).
type SpreadInputs struct {
	IranianAsk decimal.Decimal // best ASK on the Iranian exchange (our buy price)
	BinanceRef decimal.Decimal // Binance best BID, converted into the Iranian quote unit
	BuyFeeBps  decimal.Decimal // Iranian buy (taker) fee, in bps
	SellFeeBps decimal.Decimal // Iranian sell (maker) fee, in bps
}

// SpreadResult is the computed spread. OK is false for invalid inputs.
type SpreadResult struct {
	SpreadBps      decimal.Decimal
	FeeAdjustedBps decimal.Decimal
	OK             bool
}

// ComputeSpread returns the raw and fee-adjusted spread in bps. The strategy buys
// on the Iranian exchange (at its ask) when it is cheaper than the Binance
// reference (best bid), so:
//
//	spread_bps        = (binanceRef - iranianAsk) / iranianAsk * 10000
//	fee_adjusted_bps  = spread_bps - buyFeeBps - sellFeeBps
//
// A positive spread means the Iranian ask is below the Binance bid (an edge).
func ComputeSpread(in SpreadInputs) SpreadResult {
	if !in.IranianAsk.IsPositive() {
		return SpreadResult{OK: false}
	}
	spread := in.BinanceRef.Sub(in.IranianAsk).Div(in.IranianAsk).Mul(bps)
	feeAdj := spread.Sub(in.BuyFeeBps).Sub(in.SellFeeBps)
	return SpreadResult{SpreadBps: spread, FeeAdjustedBps: feeAdj, OK: true}
}

// feeFractionToBps converts a fee fraction (e.g. 0.001 = 0.1%) to bps (10).
func feeFractionToBps(fraction decimal.Decimal) decimal.Decimal {
	return fraction.Mul(bps)
}

// roundToInt rounds a decimal bps value to the nearest int (for the INT columns).
func roundToInt(d decimal.Decimal) int {
	return int(d.Round(0).IntPart())
}
