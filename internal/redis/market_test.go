package redis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/config"
	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/events"
)

func TestKeyBuilders(t *testing.T) {
	// Canonical symbols contain '/'; Redis keys are binary-safe so it is kept.
	if got := OrderBookKey("nobitex", "BTC/IRT"); got != "orderbook:nobitex:BTC/IRT" {
		t.Errorf("OrderBookKey = %q", got)
	}
	if got := PriceKey("binance", "BTC/USDT"); got != "price:binance:BTC/USDT" {
		t.Errorf("PriceKey = %q", got)
	}
}

// TestMarketRoundTripIntegration exercises the real Redis store. Skipped unless
// V3_TEST_REDIS_ADDR is set (e.g. 127.0.0.1:6379). It uses Redis DB 15 and unique
// test keys and deletes them afterward, so it never touches real market keys.
//
//	V3_TEST_REDIS_ADDR=127.0.0.1:6379 go test ./internal/redis/... -run Integration
func TestMarketRoundTripIntegration(t *testing.T) {
	addr := os.Getenv("V3_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set V3_TEST_REDIS_ADDR to run the Redis integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := New(ctx, config.RedisConfig{Addr: addr, DB: 15})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close()

	const ex, sym = "test_collector", "BTC/IRT"
	defer c.Redis().Del(context.Background(), OrderBookKey(ex, sym), PriceKey(ex, sym))

	// Missing key -> ErrNotFound.
	if _, err := c.LoadOrderBook(ctx, ex, sym); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	bk := domain.OrderBook{
		Exchange: ex, Symbol: sym, QuoteUnit: "IRT", UpdatedAt: now,
		Bids: []domain.Level{{Price: decimal.RequireFromString("100"), Quantity: decimal.RequireFromString("1")}},
		Asks: []domain.Level{{Price: decimal.RequireFromString("101"), Quantity: decimal.RequireFromString("2")}},
	}
	bs, ps, ev := events.Build(bk, now, now)

	if err := c.SaveOrderBook(ctx, bs); err != nil {
		t.Fatal(err)
	}
	gotBS, err := c.LoadOrderBook(ctx, ex, sym)
	if err != nil {
		t.Fatal(err)
	}
	if gotBS.Symbol != sym || !gotBS.Book.Bids[0].Price.Equal(decimal.RequireFromString("100")) {
		t.Errorf("loaded book mismatch: %+v", gotBS)
	}

	if err := c.SavePrice(ctx, ps); err != nil {
		t.Fatal(err)
	}
	gotPS, err := c.LoadPrice(ctx, ex, sym)
	if err != nil {
		t.Fatal(err)
	}
	if !gotPS.BestAsk.Equal(decimal.RequireFromString("101")) {
		t.Errorf("loaded price mismatch: %+v", gotPS)
	}

	// Pub/sub round-trip.
	sub, err := c.SubscribeMarketEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PublishEvent(ctx, ev); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-sub:
		if got.Exchange != ex || got.Symbol != sym || got.Type != events.EventBookUpdate {
			t.Errorf("subscribed event mismatch: %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive published event")
	}
}
