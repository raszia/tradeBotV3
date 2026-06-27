// Package collector normalizes market data from the exchange PUBLIC clients into
// Redis and publishes market events. It is the read-only market-data plane.
//
// HARD constraints (PR5):
//   - Uses ONLY exchanges.PublicClient — never a private client, never credentials.
//   - Makes NO trading decisions: no signal evaluation, no cycle creation, no
//     order/queue writes. It only caches market data and records health.
//   - Redis is a CACHE: losing it loses nothing authoritative; on restart the
//     collector reconnects and repopulates.
package collector

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/events"
	"v3TradeBot/internal/exchanges"
)

// MarketStore is the subset of the Redis client the collector writes to. Defined
// as an interface so the collector is unit-testable without Redis.
type MarketStore interface {
	SaveOrderBook(ctx context.Context, bs events.BookSnapshot) error
	SavePrice(ctx context.Context, ps events.PriceSnapshot) error
	PublishEvent(ctx context.Context, ev events.MarketEvent) error
}

// HealthRecorder records per-exchange market-data health. Defined as an interface
// so the collector is unit-testable without a database.
type HealthRecorder interface {
	RecordSuccess(ctx context.Context, exchange string, latency time.Duration)
	RecordFailure(ctx context.Context, exchange string, err error)
}

// Target is one exchange the collector polls/subscribes, plus its symbols.
type Target struct {
	ExchangeCode string
	Client       exchanges.PublicClient
	Symbols      []string // canonical BASE/QUOTE
}

// Config tunes the collector.
type Config struct {
	PollInterval time.Duration // REST poll cadence (default 1s)
}

// Collector runs market-data ingestion for a set of targets.
type Collector struct {
	targets []Target
	store   MarketStore
	health  HealthRecorder
	log     *slog.Logger
	clock   clock.Clock
	cfg     Config
}

// New builds a Collector. A nil clock uses the system clock.
func New(targets []Target, store MarketStore, health HealthRecorder, log *slog.Logger, clk clock.Clock, cfg Config) *Collector {
	if clk == nil {
		clk = clock.NewSystem()
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	return &Collector{targets: targets, store: store, health: health, log: log, clock: clk, cfg: cfg}
}

// Run starts ingestion for every target and blocks until ctx is cancelled. Each
// target runs in its own goroutine: a WebSocket subscriber if the venue supports
// an order-book stream, otherwise a REST poll loop. On a clean shutdown it
// returns nil.
func (c *Collector) Run(ctx context.Context) error {
	if len(c.targets) == 0 {
		// Nothing enabled for collection yet — idle until shutdown rather than
		// exit (markets are enabled via the dashboard / discovery in later PRs).
		if c.log != nil {
			c.log.Info("collector: no targets enabled for collection; idling")
		}
		<-ctx.Done()
		return nil
	}
	var wg sync.WaitGroup
	for _, t := range c.targets {
		t := t
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.runTarget(ctx, t)
		}()
	}
	wg.Wait()
	return nil
}

// runTarget picks the WS path when available, else polls.
func (c *Collector) runTarget(ctx context.Context, t Target) {
	if t.Client.Capabilities().OrderBookWS {
		if c.runWS(ctx, t) {
			return // WS ran (until ctx done); done
		}
		// WS unavailable at runtime: fall back to polling.
	}
	c.runPoll(ctx, t)
}

// runWS subscribes to the venue's order-book stream and ingests updates. Returns
// false if the subscription could not be established (caller falls back to poll).
func (c *Collector) runWS(ctx context.Context, t Target) bool {
	ch, err := t.Client.SubscribeOrderBook(ctx, t.Symbols)
	if err != nil {
		c.logWarn("ws subscribe failed; falling back to poll", t.ExchangeCode, err)
		return false
	}
	for {
		select {
		case <-ctx.Done():
			return true
		case book, ok := <-ch:
			if !ok {
				return true // stream closed (likely ctx cancel)
			}
			received := c.clock.Now()
			c.ingest(ctx, t.ExchangeCode, book, received)
			c.health.RecordSuccess(ctx, t.ExchangeCode, 0)
		}
	}
}

// runPoll polls every symbol on PollInterval until ctx is cancelled.
func (c *Collector) runPoll(ctx context.Context, t Target) {
	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()
	c.pollOnce(ctx, t) // immediate first poll
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.pollOnce(ctx, t)
		}
	}
}

func (c *Collector) pollOnce(ctx context.Context, t Target) {
	for _, sym := range t.Symbols {
		start := c.clock.Now()
		book, err := t.Client.GetOrderBook(ctx, sym)
		if err != nil {
			c.health.RecordFailure(ctx, t.ExchangeCode, err)
			c.logWarn("orderbook fetch failed", t.ExchangeCode, err)
			continue
		}
		c.ingest(ctx, t.ExchangeCode, book, start)
		c.health.RecordSuccess(ctx, t.ExchangeCode, c.clock.Now().Sub(start))
	}
}

// ingest normalizes one observed book into Redis cache + a published event. It
// performs NO trading logic.
func (c *Collector) ingest(ctx context.Context, exchange string, book domain.OrderBook, received time.Time) {
	if book.Exchange == "" {
		book.Exchange = exchange
	}
	now := c.clock.Now()
	bs, ps, ev := events.Build(book, received, now)
	if err := c.store.SaveOrderBook(ctx, bs); err != nil {
		c.logWarn("save order book failed", exchange, err)
	}
	if err := c.store.SavePrice(ctx, ps); err != nil {
		c.logWarn("save price failed", exchange, err)
	}
	if err := c.store.PublishEvent(ctx, ev); err != nil {
		c.logWarn("publish market event failed", exchange, err)
	}
}

func (c *Collector) logWarn(msg, exchange string, err error) {
	if c.log != nil {
		c.log.Warn(msg, "exchange", exchange, "err", err)
	}
}
