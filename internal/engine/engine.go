package engine

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/events"
	"v3TradeBot/internal/redis"
)

// Config tunes the signal loop. Defaults are filled by New.
type Config struct {
	// ReferenceExchange is the venue whose price is the comparison reference.
	ReferenceExchange string
	// QuoteRateSymbol is the canonical symbol used to convert the reference venue's
	// USDT price into a rial quote for IRT/IRR markets (the same Iranian exchange's
	// USDT/IRT rate is read from Redis).
	QuoteRateSymbol string
	// MaxBookAge is the staleness threshold: any book/price older than this is
	// rejected and produces no signal (rule: never trade on stale data).
	MaxBookAge time.Duration
}

func (c *Config) withDefaults() {
	if c.ReferenceExchange == "" {
		c.ReferenceExchange = "binance"
	}
	if c.QuoteRateSymbol == "" {
		c.QuoteRateSymbol = "USDT/IRT"
	}
	if c.MaxBookAge <= 0 {
		c.MaxBookAge = 10 * time.Second
	}
}

// Engine is the trade-engine signal loop. It reads market data from Redis and
// trading config from the configstore cache, computes spreads, and writes
// comparison_events / signals. It NEVER calls an exchange and NEVER creates
// cycles/orders (that is PR9); the only writes it makes are observability rows and
// (defensively) updates/removals of an existing not-yet-claimed QUEUED buy intent.
type Engine struct {
	store *db.Store
	rc    *redis.Client
	cache *configstore.Cache
	clk   clock.Clock
	log   *slog.Logger
	cfg   Config
}

// New builds an Engine. cfg is copied and defaulted.
func New(store *db.Store, rc *redis.Client, cache *configstore.Cache, clk clock.Clock, log *slog.Logger, cfg Config) *Engine {
	cfg.withDefaults()
	if clk == nil {
		clk = clock.System{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Engine{store: store, rc: rc, cache: cache, clk: clk, log: log, cfg: cfg}
}

// Run subscribes to market events and evaluates the affected Iranian markets until
// ctx is cancelled. A single subscriber drives the loop; evaluation is synchronous
// per event (PR8 has no real throughput — correctness over concurrency here).
func (e *Engine) Run(ctx context.Context) error {
	sub, err := e.rc.SubscribeMarketEvents(ctx)
	if err != nil {
		return err
	}
	e.log.Info("trade-engine signal loop started",
		"reference_exchange", e.cfg.ReferenceExchange, "max_book_age", e.cfg.MaxBookAge)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-sub:
			if !ok {
				return nil
			}
			snap := e.cache.Snapshot()
			for _, m := range e.targets(snap, ev) {
				if err := e.evaluate(ctx, snap, m); err != nil {
					// A single market's failure must never stop the loop or leak.
					e.log.Warn("evaluate failed", "exchange", m.ExchangeCode,
						"symbol", m.CanonicalSymbol, "err", err)
				}
			}
		}
	}
}

// targets returns the Iranian markets to (re)evaluate for a market event. An
// unconfigured system (config version 0) produces nothing (rule: no signal when
// config is version 0). When the event is from the reference venue, every Iranian
// signal-enabled market on the same base asset is re-evaluated (its reference
// moved); otherwise only the specific Iranian market that ticked.
func (e *Engine) targets(snap *configstore.Snapshot, ev events.MarketEvent) []configstore.MarketConfig {
	if snap == nil || snap.Version == 0 {
		return nil
	}
	var out []configstore.MarketConfig
	if ev.Exchange == e.cfg.ReferenceExchange {
		base := baseOf(ev.Symbol)
		for _, m := range snap.MarketsByID {
			if m.ExchangeCode != e.cfg.ReferenceExchange && m.EnabledForSignal && baseOf(m.CanonicalSymbol) == base {
				out = append(out, m)
			}
		}
		return out
	}
	for _, m := range snap.MarketsBySymbol[ev.Symbol] {
		if m.ExchangeCode == ev.Exchange && m.EnabledForSignal {
			out = append(out, m)
		}
	}
	return out
}

// evaluate runs one comparison for an Iranian market against the reference venue.
// It writes a comparison_event for every computable (fresh, both-sided) comparison
// and a signal only when the fee-adjusted spread clears the per-symbol threshold.
// Missing/stale/invalid data → no write, no signal (fail-safe).
func (e *Engine) evaluate(ctx context.Context, snap *configstore.Snapshot, m configstore.MarketConfig) error {
	if !m.EnabledForSignal || !m.HasSymbolConfig {
		return nil // not eligible / no threshold to compare against
	}
	now := e.clk.Now()

	// --- Iranian side: our buy price is the Iranian best ask. ---
	ib, err := e.rc.LoadOrderBook(ctx, m.ExchangeCode, m.CanonicalSymbol)
	if err != nil {
		if errors.Is(err, redis.ErrNotFound) {
			return nil // no data → no signal
		}
		return err
	}
	iAsk, ok := ib.Book.BestAsk()
	if !ok || !iAsk.Price.IsPositive() || ib.Stale(e.cfg.MaxBookAge, now) {
		return nil
	}

	// --- Reference side: Binance best bid for BASE/USDT. ---
	base := baseOf(m.CanonicalSymbol)
	rb, err := e.rc.LoadOrderBook(ctx, e.cfg.ReferenceExchange, base+"/USDT")
	if err != nil {
		if errors.Is(err, redis.ErrNotFound) {
			return nil
		}
		return err
	}
	refBid, ok := rb.Book.BestBid()
	if !ok || !refBid.Price.IsPositive() || rb.Stale(e.cfg.MaxBookAge, now) {
		return nil
	}

	// --- Quote handling: convert the reference into the Iranian quote unit. ---
	// USDT markets compare directly; IRT/IRR markets convert via the SAME exchange's
	// USDT/IRT rate so we never mix quote units silently. The rate (and quote unit)
	// are stored on the comparison/signal rows for audit.
	quote := quoteOf(m.CanonicalSymbol)
	binanceRef := refBid.Price
	var refRate *decimal.Decimal
	if !isUSDTQuote(quote) {
		ps, err := e.rc.LoadPrice(ctx, m.ExchangeCode, e.cfg.QuoteRateSymbol)
		if err != nil {
			if errors.Is(err, redis.ErrNotFound) {
				return nil // no conversion rate → cannot compare safely → no signal
			}
			return err
		}
		if !ps.BestBid.IsPositive() || ps.Stale(e.cfg.MaxBookAge, now) {
			return nil
		}
		rate := ps.BestBid
		binanceRef = refBid.Price.Mul(rate) // Binance USDT price valued in the rial quote
		refRate = &rate
	}

	// --- Fees: Iranian buy (taker) + sell (maker), per-market or exchange default. ---
	fee, ok := snap.Fees[m.ExchangeMarketID]
	if !ok {
		fee = snap.Fees[0] // exchange-wide default (zero value if none configured)
	}
	res := ComputeSpread(SpreadInputs{
		IranianAsk: iAsk.Price,
		BinanceRef: binanceRef,
		BuyFeeBps:  feeFractionToBps(fee.TakerFee),
		SellFeeBps: feeFractionToBps(fee.MakerFee),
	})
	if !res.OK {
		return nil
	}
	passed := res.FeeAdjustedBps.GreaterThanOrEqual(decimal.NewFromInt(int64(m.MinSpreadBps)))

	if err := e.writeComparison(ctx, m, quote, binanceRef, iAsk.Price, refRate, res, passed, snap.Version); err != nil {
		return err
	}
	if !passed {
		// Signal no longer valid: if a not-yet-claimed QUEUED buy intent exists for
		// this scope, remove it before it can be sent (§2a). Never touches a
		// CLAIMED/IN_FLIGHT request.
		if m.EnabledForTrading {
			if _, err := RemovePendingBuyRequest(ctx, e.store, m.ExchangeMarketID); err != nil {
				return err
			}
		}
		return nil
	}

	sigID, err := e.writeSignal(ctx, m, quote, binanceRef, iAsk.Price, refRate, res, snap.Version)
	if err != nil {
		return err
	}
	// One active pending buy intent per scope (§2a). PR8 does NOT create the buy
	// request (that is PR9); it only refreshes an existing not-yet-claimed QUEUED
	// intent so signal spam cannot create competing requests.
	if m.EnabledForTrading {
		payload := newIntentPayload(m, iAsk.Price, snap.Version, sigID)
		if _, err := UpdatePendingBuyRequest(ctx, e.store, m.ExchangeMarketID, payload); err != nil {
			return err
		}
	}
	return nil
}

// writeComparison records one comparison_event (config-version stamped, with the
// quote unit and conversion rate for audit). binancePrice is already in the
// Iranian quote unit so it is directly comparable to iranianPrice.
func (e *Engine) writeComparison(ctx context.Context, m configstore.MarketConfig, quote string,
	binancePrice, iranianPrice decimal.Decimal, refRate *decimal.Decimal, res SpreadResult, passed bool, version int64) error {
	_, err := e.store.DB().ExecContext(ctx, `
INSERT INTO comparison_events
  (exchange_id, canonical_symbol, quote_unit, binance_price, iranian_price, reference_rate,
   spread_bps, fee_adjusted_spread_bps, passed, config_version, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(6))`,
		nullID(m.ExchangeID), m.CanonicalSymbol, quote,
		binancePrice.String(), iranianPrice.String(), nullRate(refRate),
		roundToInt(res.SpreadBps), roundToInt(res.FeeAdjustedBps), boolToInt(passed), version)
	return err
}

// writeSignal records an accepted signal (no cycle yet — cycle_id NULL). The row
// carries enough price/config context for later audit.
func (e *Engine) writeSignal(ctx context.Context, m configstore.MarketConfig, quote string,
	binancePrice, iranianPrice decimal.Decimal, refRate *decimal.Decimal, res SpreadResult, version int64) (int64, error) {
	r, err := e.store.DB().ExecContext(ctx, `
INSERT INTO signals
  (cycle_id, exchange_id, canonical_symbol, quote_unit, binance_price, iranian_price, reference_rate,
   spread_bps, fee_adjusted_spread_bps, buy_size, config_version, accepted, signal_time)
VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NOW(6))`,
		nullID(m.ExchangeID), m.CanonicalSymbol, quote,
		binancePrice.String(), iranianPrice.String(), nullRate(refRate),
		roundToInt(res.SpreadBps), roundToInt(res.FeeAdjustedBps), m.BuySize.String(), version)
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

// ---- small helpers ----

func baseOf(canonical string) string {
	if i := strings.IndexByte(canonical, '/'); i >= 0 {
		return canonical[:i]
	}
	return canonical
}

func quoteOf(canonical string) string {
	if i := strings.IndexByte(canonical, '/'); i >= 0 {
		return canonical[i+1:]
	}
	return ""
}

// isUSDTQuote reports whether a quote unit needs no rial conversion.
func isUSDTQuote(quote string) bool {
	switch strings.ToUpper(quote) {
	case "USDT", "USD", "USDC":
		return true
	}
	return false
}

func nullID(id int64) any {
	if id == 0 {
		return nil
	}
	return id
}

func nullRate(r *decimal.Decimal) any {
	if r == nil {
		return nil
	}
	return r.String()
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
