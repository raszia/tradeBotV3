package exchanges

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/domain"
)

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func TestSortBook(t *testing.T) {
	b := domain.OrderBook{
		Bids: []domain.Level{{Price: dec("99")}, {Price: dec("101")}, {Price: dec("100")}},
		Asks: []domain.Level{{Price: dec("103")}, {Price: dec("102")}, {Price: dec("104")}},
	}
	SortBook(&b)
	if !b.Bids[0].Price.Equal(dec("101")) || !b.Bids[2].Price.Equal(dec("99")) {
		t.Errorf("bids not sorted desc: %v", b.Bids)
	}
	if !b.Asks[0].Price.Equal(dec("102")) || !b.Asks[2].Price.Equal(dec("104")) {
		t.Errorf("asks not sorted asc: %v", b.Asks)
	}
}

func TestBuildOrderBookAppliesMultiplierAndQuoteUnit(t *testing.T) {
	// Rial venue: multiply by 0.1 to get toman (IRT).
	book := BuildOrderBook("nobitex", "BTC/IRT",
		[]domain.Level{{Price: dec("1000"), Quantity: dec("1")}},
		[]domain.Level{{Price: dec("1010"), Quantity: dec("2")}},
		dec("0.1"), "rest", time.Now())

	if book.QuoteUnit != "IRT" {
		t.Errorf("quote unit = %q, want IRT", book.QuoteUnit)
	}
	if !book.Bids[0].Price.Equal(dec("100")) {
		t.Errorf("bid not rescaled: %s", book.Bids[0].Price)
	}
	if !book.Asks[0].Price.Equal(dec("101")) {
		t.Errorf("ask not rescaled: %s", book.Asks[0].Price)
	}
}

func TestBuildOrderBookNoMultiplier(t *testing.T) {
	book := BuildOrderBook("binance", "BTC/USDT",
		[]domain.Level{{Price: dec("50000"), Quantity: dec("1")}},
		[]domain.Level{{Price: dec("50010"), Quantity: dec("1")}},
		decimal.Zero, "ws", time.Now()) // zero multiplier => treated as 1
	if !book.Bids[0].Price.Equal(dec("50000")) {
		t.Errorf("price changed unexpectedly: %s", book.Bids[0].Price)
	}
	if book.QuoteUnit != "USDT" {
		t.Errorf("quote unit = %q", book.QuoteUnit)
	}
}

func TestVenueSymbolMapping(t *testing.T) {
	cfg := ClientConfig{Symbols: map[string]string{"BTC/USDT": "BTCUSDT", "USDT/IRT": "USDTIRT"}}
	if v, ok := VenueSymbol(cfg, "BTC/USDT"); !ok || v != "BTCUSDT" {
		t.Errorf("VenueSymbol = %q ok=%v", v, ok)
	}
	// Non-canonical input is normalized first.
	if v, ok := VenueSymbol(cfg, "btc/usdt"); !ok || v != "BTCUSDT" {
		t.Errorf("VenueSymbol(non-canonical) = %q ok=%v", v, ok)
	}
	if _, ok := VenueSymbol(cfg, "ETH/USDT"); ok {
		t.Error("unknown symbol should be not-ok")
	}
	if c, ok := CanonicalForVenueSymbol(cfg, "USDTIRT"); !ok || c != "USDT/IRT" {
		t.Errorf("CanonicalForVenueSymbol = %q ok=%v", c, ok)
	}
}
