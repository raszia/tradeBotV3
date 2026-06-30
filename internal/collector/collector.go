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
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/events"
	"v3TradeBot/internal/exchanges"
)

// errWSDisconnected marks an unexpected WebSocket close (the stream ended while
// the collector context was still active) — recorded as a health failure.
var errWSDisconnected = errors.New("collector: websocket stream closed unexpectedly")

// WebSocket reconnect defaults (overridable via Config for tests).
const (
	defaultWSMinBackoff = 200 * time.Millisecond
	defaultWSMaxBackoff = 30 * time.Second
	// wsFailuresBeforePollFallback: after this many consecutive FAILED (re)connects
	// — i.e. a sustained outage where the stream never delivers — the collector also
	// does a one-shot REST poll between attempts so market data keeps flowing while it
	// keeps trying to restore the WebSocket. Sequential (no concurrent double-ingest).
	wsFailuresBeforePollFallback = 5
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
	// WSReconnectMinBackoff / WSReconnectMaxBackoff bound the exponential backoff
	// between WebSocket reconnect attempts (defaults 200ms / 30s).
	WSReconnectMinBackoff time.Duration
	WSReconnectMaxBackoff time.Duration
}

// Collector runs market-data ingestion for a set of targets.
type Collector struct {
	targets    []Target
	store      MarketStore
	health     HealthRecorder
	log        *slog.Logger
	clock      clock.Clock
	cfg        Config
	wsFailures atomic.Int64 // count of unexpected WS disconnects (observability/tests)
}

// New builds a Collector. A nil clock uses the system clock.
func New(targets []Target, store MarketStore, health HealthRecorder, log *slog.Logger, clk clock.Clock, cfg Config) *Collector {
	if clk == nil {
		clk = clock.NewSystem()
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.WSReconnectMinBackoff <= 0 {
		cfg.WSReconnectMinBackoff = defaultWSMinBackoff
	}
	if cfg.WSReconnectMaxBackoff < cfg.WSReconnectMinBackoff {
		cfg.WSReconnectMaxBackoff = defaultWSMaxBackoff
		if cfg.WSReconnectMaxBackoff < cfg.WSReconnectMinBackoff {
			cfg.WSReconnectMaxBackoff = cfg.WSReconnectMinBackoff
		}
	}
	return &Collector{targets: targets, store: store, health: health, log: log, clock: clk, cfg: cfg}
}

// WSFailureCount returns the number of unexpected WebSocket disconnects observed
// across all targets (exposed for metrics/tests).
func (c *Collector) WSFailureCount() int64 { return c.wsFailures.Load() }

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

// runTarget picks the WS path when the venue advertises a stream, else polls.
func (c *Collector) runTarget(ctx context.Context, t Target) {
	if t.Client.Capabilities().OrderBookWS {
		c.runWSWithReconnect(ctx, t)
		return
	}
	c.runPoll(ctx, t)
}

// runWSWithReconnect keeps a venue's order-book WebSocket alive for the LIFETIME of
// ctx. An unexpected close (or a subscribe failure) while ctx is still active is
// logged + counted as a health failure and the stream is reconnected with capped
// exponential backoff — the target is NEVER silently abandoned. The ONLY clean exit
// is ctx cancellation. During a sustained outage (the stream cannot deliver for
// several attempts) it also does a one-shot REST poll between attempts as an
// additional safety net, without ever giving up on the WebSocket.
func (c *Collector) runWSWithReconnect(ctx context.Context, t Target) {
	backoff := c.cfg.WSReconnectMinBackoff
	failures := 0
	for {
		if ctx.Err() != nil {
			return // ctx cancelled: clean shutdown
		}
		delivered := c.runWSConnection(ctx, t)
		if ctx.Err() != nil {
			return // the connection ended because ctx was cancelled: clean shutdown
		}

		// We are here ONLY because of an unexpected close / failed subscribe while
		// ctx is active. Never treat this as a normal shutdown.
		c.wsFailures.Add(1)
		c.health.RecordFailure(ctx, t.ExchangeCode, errWSDisconnected)
		c.logWarn("ws stream closed unexpectedly; reconnecting", t.ExchangeCode, errWSDisconnected)

		if delivered {
			// The stream worked then blipped: recover fast and treat the outage as
			// over (do not count it toward the sustained-outage poll fallback).
			backoff = c.cfg.WSReconnectMinBackoff
			failures = 0
		} else {
			failures++
			if failures >= wsFailuresBeforePollFallback {
				// Sustained outage: keep data fresh via a single REST poll while we
				// keep trying the WebSocket. Sequential — no concurrent double-ingest.
				c.pollOnce(ctx, t)
			}
		}

		if !c.sleepCtx(ctx, backoff) {
			return // cancelled during backoff
		}
		backoff = minDuration(2*backoff, c.cfg.WSReconnectMaxBackoff)
	}
}

// runWSConnection establishes ONE subscription and ingests updates until ctx is
// cancelled or the stream channel closes. It returns whether at least one book was
// delivered (so the caller can tell a working-then-dropped stream from one that
// never connected). It does NOT itself retry.
func (c *Collector) runWSConnection(ctx context.Context, t Target) (delivered bool) {
	ch, err := t.Client.SubscribeOrderBook(ctx, t.Symbols)
	if err != nil {
		c.logWarn("ws subscribe failed", t.ExchangeCode, err)
		return false
	}
	for {
		select {
		case <-ctx.Done():
			return delivered
		case book, ok := <-ch:
			if !ok {
				return delivered // stream closed (unexpected unless ctx is done — caller checks)
			}
			delivered = true
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
		// received_at is the moment we have a SUCCESSFUL response in hand — not the
		// time the request started — so it reflects when the data was actually observed.
		received := c.clock.Now()
		c.ingest(ctx, t.ExchangeCode, book, received)
		c.health.RecordSuccess(ctx, t.ExchangeCode, received.Sub(start))
	}
}

// ingest normalizes one observed book into the Redis cache and, ONLY IF both the
// order-book and price snapshots were stored successfully, publishes the market
// event. Publishing an event for a snapshot that is not in the cache would point
// consumers at missing data, so a failed save suppresses the event. It performs NO
// trading logic.
func (c *Collector) ingest(ctx context.Context, exchange string, book domain.OrderBook, received time.Time) {
	if book.Exchange == "" {
		book.Exchange = exchange
	}
	now := c.clock.Now()
	bs, ps, ev := events.Build(book, received, now)

	if err := c.store.SaveOrderBook(ctx, bs); err != nil {
		c.logWarn("save order book failed; not publishing event", exchange, err)
		return
	}
	if err := c.store.SavePrice(ctx, ps); err != nil {
		c.logWarn("save price failed; not publishing event", exchange, err)
		return
	}
	// Both snapshots are cached: now it is safe to announce the event.
	if err := c.store.PublishEvent(ctx, ev); err != nil {
		c.logWarn("publish market event failed", exchange, err)
	}
}

// sleepCtx sleeps for d or until ctx is cancelled; returns false if cancelled.
func (c *Collector) sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (c *Collector) logWarn(msg, exchange string, err error) {
	if c.log != nil {
		c.log.Warn(msg, "exchange", exchange, "err", err)
	}
}
