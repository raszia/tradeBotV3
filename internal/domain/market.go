package domain

import (
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// Quote currencies the system understands. IRR is normalized to IRT (rial vs
// toman is a x10 display difference; the canonical quote is IRT).
const (
	CurrencyIRT  = "IRT"
	CurrencyIRR  = "IRR"
	CurrencyUSDT = "USDT"
)

// Level is one price level in an order book.
type Level struct {
	Price    decimal.Decimal `json:"price"`
	Quantity decimal.Decimal `json:"quantity"`
}

// OrderBook is a normalized, exchange-agnostic order book. Bids are sorted
// descending by price, asks ascending. Symbol is the canonical BASE/QUOTE form.
// QuoteUnit records whether prices are in IRT or USDT (or another quote).
type OrderBook struct {
	Exchange  string    `json:"exchange"`
	Symbol    string    `json:"symbol"`
	QuoteUnit string    `json:"quote_unit"`
	Bids      []Level   `json:"bids"`
	Asks      []Level   `json:"asks"`
	UpdatedAt time.Time `json:"updated_at"`
	Source    string    `json:"source"`
}

// Fresh reports whether the book was updated within maxAge of now.
func (b OrderBook) Fresh(maxAge time.Duration, now time.Time) bool {
	if b.UpdatedAt.IsZero() {
		return false
	}
	return now.Sub(b.UpdatedAt) <= maxAge
}

// BestBid returns the highest bid level, ok=false if none.
func (b OrderBook) BestBid() (Level, bool) {
	if len(b.Bids) == 0 {
		return Level{}, false
	}
	return b.Bids[0], true
}

// BestAsk returns the lowest ask level, ok=false if none.
func (b OrderBook) BestAsk() (Level, bool) {
	if len(b.Asks) == 0 {
		return Level{}, false
	}
	return b.Asks[0], true
}

// Balance is a normalized balance for one asset on one exchange.
type Balance struct {
	Exchange  string          `json:"exchange"`
	Asset     string          `json:"asset"`
	Available decimal.Decimal `json:"available"`
	Locked    decimal.Decimal `json:"locked"`
	Total     decimal.Decimal `json:"total"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// SymbolRules describes per-exchange, per-symbol trading constraints. All fields
// are optional: a zero value means "no constraint" for that field.
type SymbolRules struct {
	Exchange     string          `json:"exchange"`
	Symbol       string          `json:"symbol"`
	PriceTick    decimal.Decimal `json:"price_tick"`    // smallest price increment
	QuantityStep decimal.Decimal `json:"quantity_step"` // smallest qty increment
	MinQuantity  decimal.Decimal `json:"min_quantity"`  // minimum order qty (base)
	MaxQuantity  decimal.Decimal `json:"max_quantity"`  // maximum order qty (base)
	MinNotional  decimal.Decimal `json:"min_notional"`  // minimum order value (quote)
}

// RoundQuantityDown rounds qty down to the nearest QuantityStep (unchanged if
// QuantityStep is zero).
func (r SymbolRules) RoundQuantityDown(qty decimal.Decimal) decimal.Decimal {
	if r.QuantityStep.IsZero() {
		return qty
	}
	return qty.Div(r.QuantityStep).Floor().Mul(r.QuantityStep)
}

// RoundPriceDown rounds price down to the nearest PriceTick (unchanged if
// PriceTick is zero). Used for sell limits so the order is at-or-better.
func (r SymbolRules) RoundPriceDown(price decimal.Decimal) decimal.Decimal {
	if r.PriceTick.IsZero() {
		return price
	}
	return price.Div(r.PriceTick).Floor().Mul(r.PriceTick)
}

// RoundPriceUp rounds price up to the nearest PriceTick (unchanged if PriceTick
// is zero). Used for buy limits so the order is at-or-better.
func (r SymbolRules) RoundPriceUp(price decimal.Decimal) decimal.Decimal {
	if r.PriceTick.IsZero() {
		return price
	}
	return price.Div(r.PriceTick).Ceil().Mul(r.PriceTick)
}

// Validate returns nil if (qty, notional) satisfies the rules. notional is
// qty*price in the quote currency.
func (r SymbolRules) Validate(qty, notional decimal.Decimal) error {
	if r.MinQuantity.IsPositive() && qty.LessThan(r.MinQuantity) {
		return fmt.Errorf("qty %s below min %s for %s/%s", qty, r.MinQuantity, r.Exchange, r.Symbol)
	}
	if r.MaxQuantity.IsPositive() && qty.GreaterThan(r.MaxQuantity) {
		return fmt.Errorf("qty %s above max %s for %s/%s", qty, r.MaxQuantity, r.Exchange, r.Symbol)
	}
	if r.MinNotional.IsPositive() && notional.LessThan(r.MinNotional) {
		return fmt.Errorf("notional %s below min %s for %s/%s", notional, r.MinNotional, r.Exchange, r.Symbol)
	}
	return nil
}

// NormalizeSymbol canonicalizes a "BASE/QUOTE" symbol: upper-cased, trimmed,
// with IRR normalized to IRT. Returns an error for malformed input.
func NormalizeSymbol(symbol string) (string, error) {
	parts := strings.Split(strings.ToUpper(strings.TrimSpace(symbol)), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("invalid symbol %q, expected BASE/QUOTE", symbol)
	}
	if parts[1] == CurrencyIRR {
		parts[1] = CurrencyIRT
	}
	return parts[0] + "/" + parts[1], nil
}

// QuoteAssetType classifies a canonical symbol's quote into the DB enum domain
// (USDT / IRT / IRR / OTHER). IRR maps to IRT after normalization.
func QuoteAssetType(canonicalSymbol string) string {
	parts := strings.Split(canonicalSymbol, "/")
	if len(parts) != 2 {
		return "OTHER"
	}
	switch parts[1] {
	case CurrencyUSDT:
		return "USDT"
	case CurrencyIRT, CurrencyIRR:
		return "IRT"
	default:
		return "OTHER"
	}
}
