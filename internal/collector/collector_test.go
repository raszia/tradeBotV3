package collector

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/events"
	"v3TradeBot/internal/exchanges"
)

// --- test doubles ---

type fakeStore struct {
	mu       sync.Mutex
	books    []events.BookSnapshot
	prices   []events.PriceSnapshot
	pubs     []events.MarketEvent
	saveErr  error // injected SaveOrderBook error
	priceErr error // injected SavePrice error
}

func (s *fakeStore) SaveOrderBook(_ context.Context, bs events.BookSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveErr != nil {
		return s.saveErr
	}
	s.books = append(s.books, bs)
	return nil
}
func (s *fakeStore) SavePrice(_ context.Context, ps events.PriceSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.priceErr != nil {
		return s.priceErr
	}
	s.prices = append(s.prices, ps)
	return nil
}
func (s *fakeStore) PublishEvent(_ context.Context, ev events.MarketEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pubs = append(s.pubs, ev)
	return nil
}
func (s *fakeStore) counts() (int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.books), len(s.prices), len(s.pubs)
}

type fakeHealth struct {
	mu       sync.Mutex
	success  int
	failures int
}

func (h *fakeHealth) RecordSuccess(context.Context, string, time.Duration) {
	h.mu.Lock()
	h.success++
	h.mu.Unlock()
}
func (h *fakeHealth) RecordFailure(context.Context, string, error) {
	h.mu.Lock()
	h.failures++
	h.mu.Unlock()
}
func (h *fakeHealth) fails() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.failures
}

func book(sym string) domain.OrderBook {
	return domain.OrderBook{
		Exchange: "fakeex", Symbol: sym, QuoteUnit: "IRT",
		Bids:      []domain.Level{{Price: decimal.RequireFromString("100"), Quantity: decimal.RequireFromString("1")}},
		Asks:      []domain.Level{{Price: decimal.RequireFromString("101"), Quantity: decimal.RequireFromString("1")}},
		UpdatedAt: time.Now(),
	}
}

func newTestCollector(targets []Target, store *fakeStore, health *fakeHealth) *Collector {
	return New(targets, store, health, nil, clock.NewSystem(), Config{PollInterval: 10 * time.Millisecond})
}

// --- tests ---

func TestPollOnceIngestsEverySymbol(t *testing.T) {
	pub := exchanges.NewFakePublicClient("fakeex")
	pub.SetOrderBook("BTC/IRT", book("BTC/IRT"))
	pub.SetOrderBook("ETH/IRT", book("ETH/IRT"))

	store := &fakeStore{}
	health := &fakeHealth{}
	c := newTestCollector(nil, store, health)
	c.pollOnce(context.Background(), Target{ExchangeCode: "fakeex", Client: pub, Symbols: []string{"BTC/IRT", "ETH/IRT"}})

	b, p, e := store.counts()
	if b != 2 || p != 2 || e != 2 {
		t.Fatalf("ingest counts books=%d prices=%d events=%d, want 2/2/2", b, p, e)
	}
	if health.success != 2 || health.failures != 0 {
		t.Errorf("health success=%d failures=%d, want 2/0", health.success, health.failures)
	}
}

func TestPollOnceRecordsFailureAndDoesNotWrite(t *testing.T) {
	pub := exchanges.NewFakePublicClient("fakeex")
	pub.GetOrderBookErr = errors.New("venue down")

	store := &fakeStore{}
	health := &fakeHealth{}
	c := newTestCollector(nil, store, health)
	c.pollOnce(context.Background(), Target{ExchangeCode: "fakeex", Client: pub, Symbols: []string{"BTC/IRT"}})

	if b, p, e := store.counts(); b+p+e != 0 {
		t.Errorf("nothing should be written on failure, got %d/%d/%d", b, p, e)
	}
	if health.failures != 1 || health.success != 0 {
		t.Errorf("expected one failure recorded, got success=%d failures=%d", health.success, health.failures)
	}
}

func TestRunPollLoopAndGracefulShutdown(t *testing.T) {
	pub := exchanges.NewFakePublicClient("fakeex")
	pub.SetOrderBook("BTC/IRT", book("BTC/IRT"))

	store := &fakeStore{}
	c := newTestCollector(
		[]Target{{ExchangeCode: "fakeex", Client: pub, Symbols: []string{"BTC/IRT"}}},
		store, &fakeHealth{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	// Wait for at least the immediate poll to land.
	deadline := time.After(2 * time.Second)
	for {
		if b, _, _ := store.counts(); b >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("collector did not ingest within timeout")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error on shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel (graceful shutdown failed)")
	}
}

func TestRunWSPathIngests(t *testing.T) {
	ws := make(chan domain.OrderBook, 1)
	pub := exchanges.NewFakePublicClient("fakeex")
	pub.Caps = &exchanges.Capabilities{OrderBookREST: true, OrderBookWS: true}
	pub.WS = ws

	store := &fakeStore{}
	c := newTestCollector(
		[]Target{{ExchangeCode: "fakeex", Client: pub, Symbols: []string{"BTC/IRT"}}},
		store, &fakeHealth{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	ws <- book("BTC/IRT") // push one WS update
	deadline := time.After(2 * time.Second)
	for {
		if b, _, e := store.counts(); b >= 1 && e >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("WS update not ingested within timeout")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestRunZeroTargetsIdlesUntilShutdown(t *testing.T) {
	c := newTestCollector(nil, &fakeStore{}, &fakeHealth{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case <-done:
		t.Fatal("Run returned before shutdown with zero targets")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// TestCollectorUsesPublicClientType is a compile-time guard that Target carries a
// PublicClient (no private/credentialed surface is reachable from the collector).
func TestCollectorUsesPublicClientType(t *testing.T) {
	var _ exchanges.PublicClient = exchanges.NewFakePublicClient("x")
	tgt := Target{Client: exchanges.NewFakePublicClient("x")}
	if _, ok := any(tgt.Client).(exchanges.PrivateClient); ok {
		t.Fatal("collector Target.Client must not be a PrivateClient")
	}
}

// TestNoPublishWhenSaveOrderBookFails (PR5 correction) — a market_event must NOT be
// published if the order-book snapshot failed to save.
func TestNoPublishWhenSaveOrderBookFails(t *testing.T) {
	pub := exchanges.NewFakePublicClient("fakeex")
	pub.SetOrderBook("BTC/IRT", book("BTC/IRT"))
	store := &fakeStore{saveErr: errors.New("redis down")}
	c := newTestCollector(nil, store, &fakeHealth{})

	c.pollOnce(context.Background(), Target{ExchangeCode: "fakeex", Client: pub, Symbols: []string{"BTC/IRT"}})

	b, p, e := store.counts()
	if e != 0 {
		t.Errorf("PublishEvent must NOT be called when SaveOrderBook fails (events=%d)", e)
	}
	if b != 0 || p != 0 {
		t.Errorf("nothing should be stored when SaveOrderBook fails, got books=%d prices=%d", b, p)
	}
}

// TestNoPublishWhenSavePriceFails (PR5 correction) — a market_event must NOT be
// published if the price snapshot failed to save (even though the book saved first).
func TestNoPublishWhenSavePriceFails(t *testing.T) {
	pub := exchanges.NewFakePublicClient("fakeex")
	pub.SetOrderBook("BTC/IRT", book("BTC/IRT"))
	store := &fakeStore{priceErr: errors.New("redis down")}
	c := newTestCollector(nil, store, &fakeHealth{})

	c.pollOnce(context.Background(), Target{ExchangeCode: "fakeex", Client: pub, Symbols: []string{"BTC/IRT"}})

	b, p, e := store.counts()
	if e != 0 {
		t.Errorf("PublishEvent must NOT be called when SavePrice fails (events=%d)", e)
	}
	if b != 1 || p != 0 {
		t.Errorf("book saved, price not, no event: got books=%d prices=%d events=%d", b, p, e)
	}
}

// wsReconnectClient is a PublicClient whose WebSocket delivers exactly ONE book and
// then closes — simulating an unexpected disconnect. Each SubscribeOrderBook call
// returns a fresh channel and increments a counter, so a test can observe reconnects.
type wsReconnectClient struct {
	*exchanges.FakePublicClient
	mu       sync.Mutex
	subCount int
}

func (c *wsReconnectClient) Capabilities() exchanges.Capabilities {
	return exchanges.Capabilities{OrderBookREST: true, OrderBookWS: true}
}

func (c *wsReconnectClient) SubscribeOrderBook(context.Context, []string) (<-chan domain.OrderBook, error) {
	c.mu.Lock()
	c.subCount++
	c.mu.Unlock()
	ch := make(chan domain.OrderBook, 1)
	ch <- book("BTC/IRT")
	close(ch) // one message, then an UNEXPECTED close (ctx is still active)
	return ch, nil
}

func (c *wsReconnectClient) subscribes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subCount
}

// TestWSReconnectsOnUnexpectedClose (PR5 correction) — when a WebSocket stream closes
// while the collector context is still active, the collector must detect it, count it
// as a failure, and RECONNECT (not silently abandon the target). Cancelling the
// context must then stop the reconnect loop cleanly.
func TestWSReconnectsOnUnexpectedClose(t *testing.T) {
	client := &wsReconnectClient{FakePublicClient: exchanges.NewFakePublicClient("fakeex")}
	client.SetOrderBook("BTC/IRT", book("BTC/IRT"))
	store := &fakeStore{}
	health := &fakeHealth{}
	c := New(
		[]Target{{ExchangeCode: "fakeex", Client: client, Symbols: []string{"BTC/IRT"}}},
		store, health, nil, clock.NewSystem(),
		Config{PollInterval: 10 * time.Millisecond, WSReconnectMinBackoff: time.Millisecond, WSReconnectMaxBackoff: 5 * time.Millisecond},
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()

	// The target must be re-subscribed after the first unexpected close — prove it
	// reconnects several times rather than stopping.
	deadline := time.After(2 * time.Second)
	for client.subscribes() < 3 {
		select {
		case <-deadline:
			t.Fatalf("collector did not reconnect after unexpected WS close (subscribes=%d)", client.subscribes())
		case <-time.After(2 * time.Millisecond):
		}
	}
	// The unexpected closes were counted as failures (not treated as normal shutdown).
	if health.fails() == 0 {
		t.Error("unexpected WS close must be recorded as a health failure")
	}
	if c.WSFailureCount() == 0 {
		t.Error("WSFailureCount must be > 0 after unexpected disconnects")
	}
	// The delivered book(s) were ingested (target stayed productive while reconnecting).
	if b, _, _ := store.counts(); b == 0 {
		t.Error("expected at least one ingested book across reconnects")
	}

	// Cancelling the context must stop the reconnect loop cleanly.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect loop did not stop after ctx cancel")
	}
}
