package redis

import (
	"context"
	"encoding/json"
	"errors"

	goredis "github.com/redis/go-redis/v9"

	"v3TradeBot/internal/events"
)

// Redis market-data layer. Keys hold the LATEST normalized order book and price
// per (exchange, canonical symbol); a single pub/sub channel carries market
// events. This is CACHE only — never authoritative state (no cycles/orders/
// fills/balances/locks/queue/config live in Redis).
//
// Key schema (documented in PROJECT_ARCHITECTURE.md §7):
//
//	orderbook:{exchange_code}:{canonical_symbol}   -> JSON events.BookSnapshot
//	price:{exchange_code}:{canonical_symbol}       -> JSON events.PriceSnapshot
//	channel "market_events"                        -> JSON events.MarketEvent
//
// Canonical symbols (e.g. "BTC/USDT") contain '/'. Redis keys are binary-safe, so
// '/' is kept verbatim — the format stays human-readable and consistent.

// MarketEventsChannel is the shared pub/sub channel for market events.
const MarketEventsChannel = "market_events"

// ErrNotFound is returned by Load* when the key is absent (e.g. not yet
// collected, or expired). Callers treat it as "no fresh data", never an error of
// record — Redis is a cache.
var ErrNotFound = errors.New("redis: key not found")

// OrderBookKey builds the order-book key for an exchange + canonical symbol.
func OrderBookKey(exchange, canonicalSymbol string) string {
	return "orderbook:" + exchange + ":" + canonicalSymbol
}

// PriceKey builds the price key for an exchange + canonical symbol.
func PriceKey(exchange, canonicalSymbol string) string {
	return "price:" + exchange + ":" + canonicalSymbol
}

// SaveOrderBook stores the latest order-book snapshot with the market TTL.
func (c *Client) SaveOrderBook(ctx context.Context, bs events.BookSnapshot) error {
	raw, err := json.Marshal(bs)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, OrderBookKey(bs.Exchange, bs.Symbol), raw, c.marketTTL).Err()
}

// LoadOrderBook reads the latest order-book snapshot; ErrNotFound if absent.
func (c *Client) LoadOrderBook(ctx context.Context, exchange, symbol string) (events.BookSnapshot, error) {
	raw, err := c.rdb.Get(ctx, OrderBookKey(exchange, symbol)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return events.BookSnapshot{}, ErrNotFound
	}
	if err != nil {
		return events.BookSnapshot{}, err
	}
	var bs events.BookSnapshot
	if err := json.Unmarshal(raw, &bs); err != nil {
		return events.BookSnapshot{}, err
	}
	return bs, nil
}

// SavePrice stores the latest price snapshot with the market TTL.
func (c *Client) SavePrice(ctx context.Context, ps events.PriceSnapshot) error {
	raw, err := json.Marshal(ps)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, PriceKey(ps.Exchange, ps.Symbol), raw, c.marketTTL).Err()
}

// LoadPrice reads the latest price snapshot; ErrNotFound if absent.
func (c *Client) LoadPrice(ctx context.Context, exchange, symbol string) (events.PriceSnapshot, error) {
	raw, err := c.rdb.Get(ctx, PriceKey(exchange, symbol)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return events.PriceSnapshot{}, ErrNotFound
	}
	if err != nil {
		return events.PriceSnapshot{}, err
	}
	var ps events.PriceSnapshot
	if err := json.Unmarshal(raw, &ps); err != nil {
		return events.PriceSnapshot{}, err
	}
	return ps, nil
}

// PublishEvent publishes a market event on the shared channel.
func (c *Client) PublishEvent(ctx context.Context, ev events.MarketEvent) error {
	raw, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return c.rdb.Publish(ctx, MarketEventsChannel, raw).Err()
}

// SubscribeMarketEvents subscribes to the market-events channel and returns a
// channel of decoded events. It runs until ctx is cancelled, then closes the
// channel and the subscription. Malformed messages are skipped.
func (c *Client) SubscribeMarketEvents(ctx context.Context) (<-chan events.MarketEvent, error) {
	sub := c.rdb.Subscribe(ctx, MarketEventsChannel)
	if _, err := sub.Receive(ctx); err != nil { // confirm subscription
		_ = sub.Close()
		return nil, err
	}
	out := make(chan events.MarketEvent, 256)
	go func() {
		defer close(out)
		defer sub.Close()
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var ev events.MarketEvent
				if json.Unmarshal([]byte(msg.Payload), &ev) != nil {
					continue
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}
