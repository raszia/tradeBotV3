// Package sellflow is the exit-sell side of a cycle (PR11). After a buy is confirmed
// filled or partially filled, it creates and manages a resting limit sell on the
// FILLED quantity only — priced slightly below the Binance reference and repriced
// (cancel/replace) no more often than the configured interval. Like every
// decision-side component it NEVER calls an exchange: it writes DB rows and queue
// requests; the order-executor sends them and internal/orders processes the results.
package sellflow

import "github.com/shopspring/decimal"

var bps = decimal.NewFromInt(10000)

// SellPrice computes the resting sell limit price: the Binance reference (already in
// the Iranian quote unit) minus the configured offset, floored to the venue tick:
//
//	price = floor( binanceRef × (1 − sellOffsetBps/10000) , tick )
//
// Selling slightly BELOW the Binance reference is what makes the resting sell fill
// against local buyers. Flooring to the tick keeps the price at/below the target
// (never above) and venue-valid.
func SellPrice(binanceRef decimal.Decimal, sellOffsetBps int, tick decimal.Decimal) decimal.Decimal {
	if sellOffsetBps < 0 {
		sellOffsetBps = 0
	}
	p := binanceRef.Mul(bps.Sub(decimal.NewFromInt(int64(sellOffsetBps)))).Div(bps)
	return SnapDownToTick(p, tick)
}

// SnapDownToTick floors price to a multiple of tick (no-op when tick is zero).
func SnapDownToTick(price, tick decimal.Decimal) decimal.Decimal {
	if !tick.IsPositive() {
		return price
	}
	return price.Div(tick).Floor().Mul(tick)
}

// SnapQtyToStep floors qty to a multiple of step (no-op when step is zero). Flooring
// ensures we never try to sell more than the inventory we actually hold.
func SnapQtyToStep(qty, step decimal.Decimal) decimal.Decimal {
	if !step.IsPositive() {
		return qty
	}
	return qty.Div(step).Floor().Mul(step)
}

// MeetsMinimums reports whether qty/price clear the venue minimums. A zero minimum
// means "no constraint". A non-positive qty or price never qualifies.
func MeetsMinimums(qty, price, minQty, minNotional decimal.Decimal) bool {
	if !qty.IsPositive() || !price.IsPositive() {
		return false
	}
	if minQty.IsPositive() && qty.LessThan(minQty) {
		return false
	}
	if minNotional.IsPositive() && qty.Mul(price).LessThan(minNotional) {
		return false
	}
	return true
}
