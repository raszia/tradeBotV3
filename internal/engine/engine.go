package engine

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/buyflow"
	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/events"
	"v3TradeBot/internal/live"
	"v3TradeBot/internal/queue"
	"v3TradeBot/internal/redis"
	"v3TradeBot/internal/regime"
	"v3TradeBot/internal/sellflow"
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
	// LockLeaseSeconds is the symbol-lock lease applied when a buy cycle is created.
	LockLeaseSeconds int
	// SellManageInterval is how often the sell-management pass runs (default 2s).
	SellManageInterval time.Duration
	// DryRun stamps created cycles as dry-run (PR19); set from the bootstrap execution
	// mode. It does not by itself simulate execution — that is the executor wiring the
	// simulated client — it only marks cycles so the dashboard/reconciler can identify
	// them.
	DryRun bool
	// LiveGuard, when set (LIVE mode), gates NEW buy-cycle creation against the live
	// caps + kill switch (PR20). It is the first check; the executor re-checks before
	// the actual send.
	LiveGuard *live.Guard
	// PrepareBuyCycles enables PR9 buy-cycle preparation: on a PASSING signal for a
	// trading-enabled market the engine creates (or refreshes the QUEUED buy of) a cycle +
	// symbol lock + order + queued PLACE request, transactionally via internal/buyflow.
	//
	// PR8 is SIGNAL-ONLY and leaves this FALSE — the engine's only writes are then
	// comparison_events + signals, and it touches no cycles/orders/exchange_requests/locks.
	// The PR9 wiring (cmd/trade-engine) sets it true.
	PrepareBuyCycles bool
	// SubscribeMinBackoff / SubscribeMaxBackoff bound the exponential backoff between
	// market_events resubscribe attempts after an unexpected close (defaults 200ms / 30s).
	SubscribeMinBackoff time.Duration
	SubscribeMaxBackoff time.Duration
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
	if c.LockLeaseSeconds <= 0 {
		c.LockLeaseSeconds = 600
	}
	if c.SellManageInterval <= 0 {
		c.SellManageInterval = 2 * time.Second
	}
	if c.SubscribeMinBackoff <= 0 {
		c.SubscribeMinBackoff = 200 * time.Millisecond
	}
	if c.SubscribeMaxBackoff < c.SubscribeMinBackoff {
		c.SubscribeMaxBackoff = 30 * time.Second
		if c.SubscribeMaxBackoff < c.SubscribeMinBackoff {
			c.SubscribeMaxBackoff = c.SubscribeMinBackoff
		}
	}
}

// marketSubscriber is the market-event subscription seam the signal loop depends on, so
// reconnect behaviour is unit-testable without a live Redis. *redis.Client satisfies it.
type marketSubscriber interface {
	SubscribeMarketEvents(ctx context.Context) (<-chan events.MarketEvent, error)
}

// Engine is the trade-engine signal loop. It reads market data from Redis and
// trading config from the configstore cache, computes spreads, and writes
// comparison_events / signals. On an accepted signal for a trading-enabled market
// it prepares the buy intent via internal/buyflow (cycle + lock + order + QUEUED
// PLACE_ORDER request, one transaction). It NEVER calls an exchange and NEVER sends
// an order — the order-executor does that later (PR10+).
type Engine struct {
	store      *db.Store
	rc         *redis.Client
	subSource  marketSubscriber // market_events source for Run; defaults to rc (test-overridable)
	cache      *configstore.Cache
	q          *queue.Queue
	sellMgr    *sellflow.Manager
	regimeCalc *regime.Calculator
	clk        clock.Clock
	log        *slog.Logger
	cfg        Config
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
	var q *queue.Queue
	if store != nil {
		q = queue.New(store.DB(), clk)
	}
	e := &Engine{store: store, rc: rc, cache: cache, q: q, clk: clk, log: log, cfg: cfg}
	if rc != nil {
		e.subSource = rc // the live market_events source; tests inject a fake
	}
	if store != nil && rc != nil {
		// Sell-side manager (PR11): drives exit-sell creation/poll/reprice using the
		// engine's Binance reference price. It writes DB rows + queue requests only.
		e.sellMgr = sellflow.NewManager(store, q, cache, e.sellRefPrice, clk, log)
		// Market-regime calculator (PR15): samples Binance prices from Redis and writes
		// regime current/history. Read-only toward Redis; no exchange calls, no trading.
		e.regimeCalc = regime.NewCalculator(regime.NewStore(store.DB()), e, clk, log, regime.Config{})
	}
	return e
}

// Price implements regime.PriceSource: the current Binance price for a symbol read
// from the Redis market-data cache (mid when both sides are present, else best bid).
func (e *Engine) Price(ctx context.Context, symbol string) (decimal.Decimal, time.Time, bool) {
	ps, err := e.rc.LoadPrice(ctx, e.cfg.ReferenceExchange, symbol)
	if err != nil {
		return decimal.Zero, time.Time{}, false
	}
	price := ps.BestBid
	if ps.BestBid.IsPositive() && ps.BestAsk.IsPositive() {
		price = ps.BestBid.Add(ps.BestAsk).Div(decimal.NewFromInt(2))
	}
	if !price.IsPositive() || ps.ExchangeTime.IsZero() {
		return decimal.Zero, time.Time{}, false
	}
	return price, ps.ExchangeTime, true
}

// sellRefPrice is the sellflow.RefPrice provider: the Binance reference for a market,
// converted into the Iranian quote unit (same conversion as the buy signal).
func (e *Engine) sellRefPrice(m configstore.MarketConfig) (decimal.Decimal, string, string, bool) {
	return e.binanceRefFor(context.Background(), m)
}

// binanceRefFor returns the Binance best-bid reference for a market, converted into
// the Iranian quote unit, with ok=false when any input is missing or stale.
func (e *Engine) binanceRefFor(ctx context.Context, m configstore.MarketConfig) (decimal.Decimal, string, string, bool) {
	now := e.clk.Now()
	base := baseOf(m.CanonicalSymbol)
	rb, err := e.rc.LoadOrderBook(ctx, e.cfg.ReferenceExchange, base+"/USDT")
	if err != nil {
		return decimal.Zero, "", "", false
	}
	refBid, ok := rb.Book.BestBid()
	if !ok || !refBid.Price.IsPositive() || rb.Stale(e.cfg.MaxBookAge, now) {
		return decimal.Zero, "", "", false
	}
	quote := quoteOf(m.CanonicalSymbol)
	if isUSDTQuote(quote) {
		return refBid.Price, quote, "", true
	}
	ps, err := e.rc.LoadPrice(ctx, m.ExchangeCode, e.cfg.QuoteRateSymbol)
	if err != nil || !ps.BestBid.IsPositive() || ps.Stale(e.cfg.MaxBookAge, now) {
		return decimal.Zero, "", "", false
	}
	return refBid.Price.Mul(ps.BestBid), quote, ps.BestBid.String(), true
}

// Run subscribes to market events and evaluates the affected Iranian markets until
// ctx is cancelled. A single subscriber drives the loop; evaluation is synchronous
// per event (PR8 has no real throughput — correctness over concurrency here).
func (e *Engine) Run(ctx context.Context) error {
	e.log.Info("trade-engine signal loop started",
		"reference_exchange", e.cfg.ReferenceExchange, "max_book_age", e.cfg.MaxBookAge)

	// Sell-side management runs on its own cadence (creating/polling/repricing exit
	// sells for filled cycles) alongside the market-event signal loop.
	if e.sellMgr != nil {
		go e.runSellManagement(ctx)
	}
	if e.regimeCalc != nil {
		go func() {
			if err := e.regimeCalc.Run(ctx); err != nil {
				e.log.Warn("regime calculator stopped", "err", err)
			}
		}()
	}

	return e.runSignalLoop(ctx)
}

// runSignalLoop keeps a market_events subscription alive for the LIFETIME of ctx. An
// unexpected close (or a failed subscribe) while ctx is still active is NEVER treated as a
// normal shutdown — it is logged and the subscription is re-established with capped
// exponential backoff. It returns ONLY when ctx is cancelled (returning ctx.Err(), a
// non-nil error), so a Redis pub/sub drop can never silently stop the engine.
func (e *Engine) runSignalLoop(ctx context.Context) error {
	if e.subSource == nil {
		return errors.New("engine: no market_events source configured")
	}
	backoff := e.cfg.SubscribeMinBackoff
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		sub, err := e.subSource.SubscribeMarketEvents(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			e.log.Warn("market_events subscribe failed; retrying with backoff", "err", err)
			if !e.sleep(ctx, backoff) {
				return ctx.Err()
			}
			backoff = minDur(2*backoff, e.cfg.SubscribeMaxBackoff)
			continue
		}

		delivered := e.consumeEvents(ctx, sub)
		if ctx.Err() != nil {
			return ctx.Err() // the only clean exit: context cancelled
		}
		// Unexpected close while ctx is active: do NOT return nil — resubscribe.
		e.log.Warn("market_events subscription closed unexpectedly; resubscribing with backoff")
		if delivered {
			backoff = e.cfg.SubscribeMinBackoff // a working subscription blipped: recover fast
		}
		if !e.sleep(ctx, backoff) {
			return ctx.Err()
		}
		backoff = minDur(2*backoff, e.cfg.SubscribeMaxBackoff)
	}
}

// consumeEvents reads + evaluates events until the channel closes or ctx is cancelled.
// Returns whether at least one event was delivered (so the caller can reset backoff after a
// connection that actually worked).
func (e *Engine) consumeEvents(ctx context.Context, sub <-chan events.MarketEvent) (delivered bool) {
	for {
		select {
		case <-ctx.Done():
			return delivered
		case ev, ok := <-sub:
			if !ok {
				return delivered // channel closed; caller checks ctx to classify it
			}
			delivered = true
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

// sleep waits d or until ctx is cancelled; returns false if cancelled.
func (e *Engine) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// runSellManagement ticks the sell Manager until ctx is cancelled.
func (e *Engine) runSellManagement(ctx context.Context) {
	t := time.NewTicker(e.cfg.SellManageInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := e.sellMgr.Pass(ctx); err != nil {
				e.log.Warn("sell-management pass failed", "err", err)
			}
		}
	}
}

// targets returns the Iranian markets to (re)evaluate for a market event. An
// unconfigured system (config version 0) produces nothing (rule: no signal when
// config is version 0).
//
//   - Reference-venue event (e.g. Binance BASE/USDT): every Iranian signal-enabled market
//     on the same base asset is re-evaluated (its reference price moved).
//   - Iranian-venue event for the configured QUOTE-RATE symbol (e.g. Nobitex USDT/IRT):
//     EVERY signal-enabled rial-quoted (non-USDT) market on the SAME exchange is
//     re-evaluated, because their reference is derived as Binance(USDT) × this rate. So a
//     Nobitex USDT/IRT tick re-evaluates Nobitex BTC/IRT, ETH/IRT, SOL/IRT, … Without this
//     a quote-rate move would silently NOT refresh dependent IRT comparisons/signals.
//   - Any other Iranian-venue event: only the specific market that ticked.
//
// USDT-quoted markets carry no quote-rate dependency, so they are not pulled in by a
// USDT/IRT event.
func (e *Engine) targets(snap *configstore.Snapshot, ev events.MarketEvent) []configstore.MarketConfig {
	if snap == nil || snap.Version == 0 {
		return nil
	}
	if ev.Exchange == e.cfg.ReferenceExchange {
		base := baseOf(ev.Symbol)
		var out []configstore.MarketConfig
		for _, m := range snap.MarketsByID {
			if m.ExchangeCode != e.cfg.ReferenceExchange && m.EnabledForSignal && baseOf(m.CanonicalSymbol) == base {
				out = append(out, m)
			}
		}
		return out
	}

	// Iranian-venue event. Dedup by exchange_market_id (the ticked symbol and the
	// quote-rate dependents can overlap).
	seen := make(map[int64]bool)
	var out []configstore.MarketConfig
	add := func(m configstore.MarketConfig) {
		if !seen[m.ExchangeMarketID] {
			seen[m.ExchangeMarketID] = true
			out = append(out, m)
		}
	}
	for _, m := range snap.MarketsBySymbol[ev.Symbol] {
		if m.ExchangeCode == ev.Exchange && m.EnabledForSignal {
			add(m)
		}
	}
	// Quote-rate dependency: a USDT/IRT tick re-evaluates every rial-quoted IRT/IRR market
	// on the same exchange (their Binance reference is converted through this rate).
	if ev.Symbol == e.cfg.QuoteRateSymbol {
		for _, m := range snap.MarketsByID {
			if m.ExchangeCode == ev.Exchange && m.EnabledForSignal && !isUSDTQuote(quoteOf(m.CanonicalSymbol)) {
				add(m)
			}
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
	// FeeFor resolves a market-specific override, else THIS exchange's default (never
	// another exchange's). Zero-value fee if none is configured for this exchange.
	fee, _ := snap.FeeFor(m.ExchangeID, m.ExchangeMarketID)
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
		// Signal not (or no longer) valid. PR9 does NOT delete a cycle-tied buy
		// request (audit rule, §2a) — invalidating a started cycle's resting/queued
		// buy is the PR10/PR11 lifecycle's job. So there is nothing to do here.
		return nil
	}

	signalID, err := e.writeSignal(ctx, m, quote, binanceRef, iAsk.Price, refRate, res, snap.Version)
	if err != nil {
		return err
	}
	// SIGNAL-ONLY boundary (PR8): the engine writes only comparison_events + signals unless
	// buy-cycle preparation is explicitly enabled. Buy-cycle preparation (cycle + lock + order
	// + QUEUED request, transactionally via internal/buyflow) is PR9's responsibility, gated
	// behind Config.PrepareBuyCycles. PR8 leaves it FALSE (signal-only); PR9 enables it in
	// cmd/trade-engine. When enabled, the created/refreshed cycle is linked back to signalID.
	if m.EnabledForTrading && e.cfg.PrepareBuyCycles {
		return e.prepareBuy(ctx, m, iAsk.Price, binanceRef, refRate, quote, fee, res, snap.Version, signalID)
	}
	return nil
}

// prepareBuy creates a new buy cycle for the scope, or refreshes the existing
// not-yet-sent QUEUED buy when the scope already holds an active cycle (no
// duplicate). ErrSymbolLocked is the normal "already have an active cycle" path,
// not an error.
func (e *Engine) prepareBuy(ctx context.Context, m configstore.MarketConfig, ask, binanceRef decimal.Decimal, refRate *decimal.Decimal, quote string, fee configstore.FeeConfig, res SpreadResult, version, signalID int64) error {
	sig := buyflow.SignalContext{
		ConfigVersion:  version,
		BinancePrice:   binanceRef,
		IranianPrice:   ask,
		SpreadBps:      roundToInt(res.SpreadBps),
		FeeAdjustedBps: roundToInt(res.FeeAdjustedBps),
		QuoteUnit:      quote,
		ReferenceRate:  refRate,
		BuyFeeBps:      roundToInt(feeFractionToBps(fee.TakerFee)),
		SellFeeBps:     roundToInt(feeFractionToBps(fee.MakerFee)),
		DryRun:         e.cfg.DryRun,
		SignalID:       signalID,
	}
	// LIVE mode: the live guard gates new buy-cycle creation (kill switch + caps).
	if e.cfg.LiveGuard != nil {
		if d := e.cfg.LiveGuard.AllowNewBuyCycle(ctx, m.ExchangeID, m.ExchangeMarketID); !d.Allow {
			e.log.Info("live guard blocked new buy cycle", "symbol", m.CanonicalSymbol, "reason", d.Reason)
			return nil
		}
	}
	_, err := buyflow.CreateBuyCycle(ctx, e.store, e.q, m, ask, sig, e.cfg.LockLeaseSeconds)
	if err == nil {
		return nil
	}
	if errors.Is(err, buyflow.ErrSymbolLocked) {
		// Scope already has an active cycle: refresh its QUEUED (unsent) buy to the
		// newest valid signal rather than creating a competing intent.
		_, rErr := buyflow.RefreshActiveCycleBuy(ctx, e.store, m, ask, sig)
		return rErr
	}
	return err
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
