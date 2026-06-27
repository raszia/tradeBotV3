package exchanges

import (
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/domain"
)

// SortBook sorts bids descending and asks ascending by price, in place.
func SortBook(b *domain.OrderBook) {
	sort.SliceStable(b.Bids, func(i, j int) bool { return b.Bids[i].Price.GreaterThan(b.Bids[j].Price) })
	sort.SliceStable(b.Asks, func(i, j int) bool { return b.Asks[i].Price.LessThan(b.Asks[j].Price) })
}

// BuildOrderBook assembles a normalized order book. priceMultiplier rescales raw
// venue prices into the canonical quote unit (e.g. 0.1 to convert rial→toman for
// venues that quote in IRR); pass decimal 1 for no rescale. The result has its
// QuoteUnit derived from the canonical symbol and is sorted.
func BuildOrderBook(exchange, canonicalSymbol string, bids, asks []domain.Level, priceMultiplier decimal.Decimal, source string, updatedAt time.Time) domain.OrderBook {
	if priceMultiplier.IsZero() {
		priceMultiplier = decimal.NewFromInt(1)
	}
	scale := func(levels []domain.Level) []domain.Level {
		if priceMultiplier.Equal(decimal.NewFromInt(1)) {
			return levels
		}
		out := make([]domain.Level, len(levels))
		for i, l := range levels {
			out[i] = domain.Level{Price: l.Price.Mul(priceMultiplier), Quantity: l.Quantity}
		}
		return out
	}
	book := domain.OrderBook{
		Exchange:  exchange,
		Symbol:    canonicalSymbol,
		QuoteUnit: quoteUnit(canonicalSymbol),
		Bids:      scale(bids),
		Asks:      scale(asks),
		UpdatedAt: updatedAt,
		Source:    source,
	}
	SortBook(&book)
	return book
}

func quoteUnit(canonicalSymbol string) string {
	parts := strings.Split(canonicalSymbol, "/")
	if len(parts) == 2 {
		return parts[1]
	}
	return ""
}

// VenueSymbol maps a canonical BASE/QUOTE symbol to the venue-native symbol via
// cfg.Symbols. ok is false if the symbol is not configured for the exchange.
func VenueSymbol(cfg ClientConfig, canonical string) (string, bool) {
	if cfg.Symbols == nil {
		return "", false
	}
	if v, ok := cfg.Symbols[canonical]; ok {
		return v, true
	}
	// tolerate non-canonical lookups
	if norm, err := domain.NormalizeSymbol(canonical); err == nil {
		if v, ok := cfg.Symbols[norm]; ok {
			return v, true
		}
	}
	return "", false
}

// CanonicalForVenueSymbol reverses cfg.Symbols: venue-native -> canonical.
func CanonicalForVenueSymbol(cfg ClientConfig, venueSymbol string) (string, bool) {
	for canonical, native := range cfg.Symbols {
		if native == venueSymbol {
			return canonical, true
		}
	}
	return "", false
}
