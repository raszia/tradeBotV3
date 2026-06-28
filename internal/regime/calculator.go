package regime

import (
	"context"
	"log/slog"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
)

// PriceSource supplies the current Binance price for a symbol FROM THE REDIS CACHE
// (never a direct exchange call). observedAt is the venue/exchange time of the price;
// ok is false when the price is missing. The Calculator records distinct observations
// over time and derives multi-timeframe momentum from them.
type PriceSource interface {
	Price(ctx context.Context, symbol string) (price decimal.Decimal, observedAt time.Time, ok bool)
}

// Config tunes the calculator.
type Config struct {
	// SampleInterval is how often prices are sampled into the rolling series and
	// baskets are (re)evaluated (default 15s). Per-basket update_interval_seconds
	// gates how often each basket's regime is actually recomputed.
	SampleInterval time.Duration
	// MaxAge is the freshness threshold for the CURRENT price of a symbol (default
	// 30s). A stale current price excludes the symbol.
	MaxAge time.Duration
	// MaxSamplesPerSymbol caps the rolling series length (default 2000).
	MaxSamplesPerSymbol int
}

func (c *Config) withDefaults() {
	if c.SampleInterval <= 0 {
		c.SampleInterval = 15 * time.Second
	}
	if c.MaxAge <= 0 {
		c.MaxAge = 30 * time.Second
	}
	if c.MaxSamplesPerSymbol <= 0 {
		c.MaxSamplesPerSymbol = 2000
	}
}

type observation struct {
	at    time.Time
	price decimal.Decimal
}

// Calculator samples Binance prices from Redis into a rolling per-symbol series and
// computes/persists each enabled basket's regime on its configured interval. It makes
// no exchange calls and never touches cycles/orders/queue.
type Calculator struct {
	store  *Store
	src    PriceSource
	clk    clock.Clock
	log    *slog.Logger
	cfg    Config
	series map[string][]observation
	last   map[int64]time.Time // basket id -> last computed
}

// NewCalculator builds a Calculator.
func NewCalculator(store *Store, src PriceSource, clk clock.Clock, log *slog.Logger, cfg Config) *Calculator {
	cfg.withDefaults()
	if clk == nil {
		clk = clock.NewSystem()
	}
	if log == nil {
		log = slog.Default()
	}
	return &Calculator{store: store, src: src, clk: clk, log: log, cfg: cfg, series: map[string][]observation{}, last: map[int64]time.Time{}}
}

// Run samples + evaluates every SampleInterval until ctx is cancelled.
func (c *Calculator) Run(ctx context.Context) error {
	t := time.NewTicker(c.cfg.SampleInterval)
	defer t.Stop()
	c.Pass(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			c.Pass(ctx)
		}
	}
}

// Pass loads baskets, samples all referenced symbols once, and recomputes each basket
// whose interval has elapsed. Safe to call repeatedly. A single basket's failure is
// isolated.
func (c *Calculator) Pass(ctx context.Context) {
	baskets, err := c.store.LoadBaskets(ctx)
	if err != nil {
		c.log.Warn("regime: load baskets failed", "err", err)
		return
	}
	now := c.clk.Now()

	// Sample every referenced symbol once (deduped by observedAt), and find the
	// horizon (largest timeframe) for pruning.
	horizon := time.Duration(0)
	symbols := map[string]bool{}
	for _, b := range baskets {
		for _, sw := range b.Symbols {
			symbols[sw.Symbol] = true
		}
		for _, tf := range b.Timeframes {
			if d := time.Duration(tf.Seconds) * time.Second; d > horizon {
				horizon = d
			}
		}
	}
	for sym := range symbols {
		c.sample(ctx, sym, now, horizon)
	}

	// Recompute baskets that are due.
	for _, b := range baskets {
		interval := time.Duration(b.UpdateIntervalSeconds) * time.Second
		if interval <= 0 {
			interval = c.cfg.SampleInterval
		}
		if last, ok := c.last[b.ID]; ok && now.Sub(last) < interval {
			continue
		}
		series := c.snapshot(b, now)
		res := Calculate(b, series, c.cfg.MaxAge)
		if err := c.store.WriteResult(ctx, b, res, now); err != nil {
			c.log.Warn("regime: write result failed", "basket", b.Name, "err", err)
			continue
		}
		c.last[b.ID] = now
	}
}

// sample reads one price for a symbol and appends a NEW observation (deduped by
// observedAt), pruning anything older than the horizon. A missing price (Redis miss
// or error) simply records nothing — the series ages out and the regime degrades to
// UNKNOWN rather than fabricating data.
func (c *Calculator) sample(ctx context.Context, symbol string, now time.Time, horizon time.Duration) {
	price, observedAt, ok := c.src.Price(ctx, symbol)
	if !ok || !price.IsPositive() || observedAt.IsZero() {
		return
	}
	obs := c.series[symbol]
	if len(obs) > 0 && !observedAt.After(obs[len(obs)-1].at) {
		return // not a newer observation; don't duplicate
	}
	obs = append(obs, observation{at: observedAt, price: price})

	// Prune old (keep a little beyond the horizon) and cap length.
	if horizon > 0 {
		cutoff := now.Add(-horizon - c.cfg.SampleInterval)
		i := 0
		for i < len(obs) && obs[i].at.Before(cutoff) {
			i++
		}
		// Keep one sample just before the cutoff so a timeframe reference still exists.
		if i > 0 {
			i--
		}
		obs = obs[i:]
	}
	if len(obs) > c.cfg.MaxSamplesPerSymbol {
		obs = obs[len(obs)-c.cfg.MaxSamplesPerSymbol:]
	}
	c.series[symbol] = obs
}

// snapshot builds the per-symbol Sample series (ages relative to now) for a basket.
func (c *Calculator) snapshot(b Basket, now time.Time) map[string][]Sample {
	out := make(map[string][]Sample, len(b.Symbols))
	for _, sw := range b.Symbols {
		obs := c.series[sw.Symbol]
		if len(obs) == 0 {
			continue
		}
		samples := make([]Sample, 0, len(obs))
		for _, o := range obs {
			samples = append(samples, Sample{Age: now.Sub(o.at), Price: o.price})
		}
		out[sw.Symbol] = samples
	}
	return out
}
