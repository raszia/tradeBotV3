package events

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/domain"
)

func dd(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func sampleBook(updated time.Time) domain.OrderBook {
	return domain.OrderBook{
		Exchange:  "nobitex",
		Symbol:    "BTC/IRT",
		QuoteUnit: "IRT",
		Bids:      []domain.Level{{Price: dd("100"), Quantity: dd("1.5")}},
		Asks:      []domain.Level{{Price: dd("101"), Quantity: dd("2")}},
		UpdatedAt: updated,
	}
}

func TestBuildDerivesSnapshotsAndEvent(t *testing.T) {
	exTime := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	received := exTime.Add(50 * time.Millisecond)
	now := received.Add(5 * time.Millisecond)

	bs, ps, ev := Build(sampleBook(exTime), received, now)

	// timestamps wired through (rule #4)
	if !bs.ExchangeTime.Equal(exTime) || !bs.ReceivedAt.Equal(received) || !bs.StoredAt.Equal(now) {
		t.Errorf("book snapshot timestamps wrong: %+v", bs)
	}
	if bs.Exchange != "nobitex" || bs.Symbol != "BTC/IRT" {
		t.Errorf("book snapshot id wrong: %+v", bs)
	}
	// top-of-book derived
	if !ps.BestBid.Equal(dd("100")) || !ps.BestAsk.Equal(dd("101")) || !ps.BidQty.Equal(dd("1.5")) {
		t.Errorf("price snapshot wrong: %+v", ps)
	}
	if ev.Type != EventBookUpdate || !ev.BestBid.Equal(dd("100")) || !ev.PublishedAt.Equal(now) {
		t.Errorf("event wrong: %+v", ev)
	}
}

func TestBuildEmptyBookHasZeroTopOfBookAndFallbackTime(t *testing.T) {
	received := time.Now()
	var emptyBook domain.OrderBook // zero UpdatedAt, no levels
	bs, ps, _ := Build(emptyBook, received, received)
	if !ps.BestBid.IsZero() || !ps.BestAsk.IsZero() {
		t.Errorf("empty book should have zero top-of-book: %+v", ps)
	}
	// ExchangeTime falls back to receivedAt when the book has no UpdatedAt.
	if !bs.ExchangeTime.Equal(received) {
		t.Errorf("fallback exchange time wrong: %v", bs.ExchangeTime)
	}
}

func TestSnapshotStaleness(t *testing.T) {
	exTime := time.Date(2026, 6, 27, 10, 0, 0, 0, time.UTC)
	bs, _, _ := Build(sampleBook(exTime), exTime, exTime)
	if bs.Stale(time.Minute, exTime.Add(30*time.Second)) {
		t.Error("should be fresh within maxAge")
	}
	if !bs.Stale(time.Minute, exTime.Add(2*time.Minute)) {
		t.Error("should be stale beyond maxAge")
	}
	if bs.Age(exTime.Add(time.Second)) != time.Second {
		t.Errorf("age = %v", bs.Age(exTime.Add(time.Second)))
	}
}

func TestPayloadsRoundTripJSON(t *testing.T) {
	exTime := time.Now().UTC().Truncate(time.Millisecond)
	bs, ps, ev := Build(sampleBook(exTime), exTime, exTime)

	for name, v := range map[string]any{"book": bs, "price": ps, "event": ev} {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("%s marshal: %v", name, err)
		}
		if len(raw) == 0 {
			t.Fatalf("%s empty json", name)
		}
	}
	// Order book round-trips with prices intact.
	raw, _ := json.Marshal(bs)
	var back BookSnapshot
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if !back.Book.Bids[0].Price.Equal(dd("100")) || back.Symbol != "BTC/IRT" {
		t.Errorf("round-trip mismatch: %+v", back)
	}
}
