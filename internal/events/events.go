// Package events defines the normalized market-data event/snapshot payloads the
// collector writes to Redis and publishes on the market-events channel. These are
// CACHE/EVENT payloads only — never authoritative state.
//
// Every payload carries explicit timestamps (rule #4) so later PRs can detect
// stale data: ExchangeTime (when the data is from, per the venue/adapter),
// ReceivedAt (when the collector received it), and StoredAt/PublishedAt (when it
// was written to / published on Redis). Source exchange and canonical symbol are
// always present.
package events

import (
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/domain"
)

// EventType classifies a market event.
type EventType string

const (
	// EventBookUpdate signals that an exchange's order book / top-of-book changed.
	EventBookUpdate EventType = "book_update"
)

// BookSnapshot is the payload stored at the orderbook:{exchange}:{symbol} key.
type BookSnapshot struct {
	Exchange     string           `json:"exchange"`
	Symbol       string           `json:"symbol"` // canonical BASE/QUOTE
	Book         domain.OrderBook `json:"book"`
	ExchangeTime time.Time        `json:"exchange_time"`
	ReceivedAt   time.Time        `json:"received_at"`
	StoredAt     time.Time        `json:"stored_at"`
}

// PriceSnapshot is the payload stored at the price:{exchange}:{symbol} key — the
// top-of-book derived from the order book.
type PriceSnapshot struct {
	Exchange     string          `json:"exchange"`
	Symbol       string          `json:"symbol"`
	BestBid      decimal.Decimal `json:"best_bid"`
	BestAsk      decimal.Decimal `json:"best_ask"`
	BidQty       decimal.Decimal `json:"bid_qty"`
	AskQty       decimal.Decimal `json:"ask_qty"`
	ExchangeTime time.Time       `json:"exchange_time"`
	ReceivedAt   time.Time       `json:"received_at"`
	StoredAt     time.Time       `json:"stored_at"`
}

// MarketEvent is published on the market-events channel when a book changes.
type MarketEvent struct {
	Type         EventType       `json:"type"`
	Exchange     string          `json:"exchange"`
	Symbol       string          `json:"symbol"`
	BestBid      decimal.Decimal `json:"best_bid"`
	BestAsk      decimal.Decimal `json:"best_ask"`
	ExchangeTime time.Time       `json:"exchange_time"`
	ReceivedAt   time.Time       `json:"received_at"`
	PublishedAt  time.Time       `json:"published_at"`
}

// Build derives the BookSnapshot, PriceSnapshot, and MarketEvent for one observed
// order book. receivedAt is when the collector got the data; now is the
// store/publish instant. The exchange time is the book's UpdatedAt.
func Build(book domain.OrderBook, receivedAt, now time.Time) (BookSnapshot, PriceSnapshot, MarketEvent) {
	var bestBid, bestAsk, bidQty, askQty decimal.Decimal
	if bid, ok := book.BestBid(); ok {
		bestBid, bidQty = bid.Price, bid.Quantity
	}
	if ask, ok := book.BestAsk(); ok {
		bestAsk, askQty = ask.Price, ask.Quantity
	}
	exTime := book.UpdatedAt
	if exTime.IsZero() {
		exTime = receivedAt
	}

	bs := BookSnapshot{
		Exchange: book.Exchange, Symbol: book.Symbol, Book: book,
		ExchangeTime: exTime, ReceivedAt: receivedAt, StoredAt: now,
	}
	ps := PriceSnapshot{
		Exchange: book.Exchange, Symbol: book.Symbol,
		BestBid: bestBid, BestAsk: bestAsk, BidQty: bidQty, AskQty: askQty,
		ExchangeTime: exTime, ReceivedAt: receivedAt, StoredAt: now,
	}
	ev := MarketEvent{
		Type: EventBookUpdate, Exchange: book.Exchange, Symbol: book.Symbol,
		BestBid: bestBid, BestAsk: bestAsk,
		ExchangeTime: exTime, ReceivedAt: receivedAt, PublishedAt: now,
	}
	return bs, ps, ev
}

// Age returns how old the snapshot's exchange data is relative to now.
func (b BookSnapshot) Age(now time.Time) time.Duration { return now.Sub(b.ExchangeTime) }

// Stale reports whether the snapshot's exchange data is older than maxAge.
func (b BookSnapshot) Stale(maxAge time.Duration, now time.Time) bool {
	return b.ExchangeTime.IsZero() || now.Sub(b.ExchangeTime) > maxAge
}

// Stale reports whether the price snapshot's exchange data is older than maxAge.
func (p PriceSnapshot) Stale(maxAge time.Duration, now time.Time) bool {
	return p.ExchangeTime.IsZero() || now.Sub(p.ExchangeTime) > maxAge
}
