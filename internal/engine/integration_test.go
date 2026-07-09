package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/config"
	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/events"
	"v3TradeBot/internal/migrate"
	redisx "v3TradeBot/internal/redis"
)

// The engine integration tests exercise the real signal loop end to end against a
// real MariaDB AND a real Redis. They are skipped unless BOTH V3_TEST_MYSQL_DSN
// and V3_TEST_REDIS_ADDR are set (Redis DB 15, unique keys cleaned up). No exchange
// is ever contacted.

var eseq int

type efix struct {
	t      *testing.T
	ctx    context.Context
	db     *sql.DB
	store  *db.Store
	rc     *redisx.Client
	cache  *configstore.Cache
	cfg    *configstore.Store
	e      *Engine
	now    time.Time
	exCode string
	exID   int64
}

func setupE(t *testing.T) *efix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	addr := os.Getenv("V3_TEST_REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("set V3_TEST_MYSQL_DSN and V3_TEST_REDIS_ADDR to run the engine integration test")
	}
	ctx := context.Background()
	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := migrate.Run(ctx, sqlDB, migrate.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	rc, err := redisx.New(ctx, redisConfig(addr))
	if err != nil {
		t.Fatalf("redis: %v", err)
	}
	t.Cleanup(func() { rc.Close() })

	eseq++
	exCode := fmt.Sprintf("eng_%d_%d", time.Now().UnixNano(), eseq)
	res, err := sqlDB.Exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'Eng Test', 1)", exCode)
	if err != nil {
		t.Fatal(err)
	}
	exID, _ := res.LastInsertId()

	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	store := db.NewFromDB(sqlDB)
	cache := configstore.NewCache()
	cfg := configstore.New(sqlDB)
	e := New(store, rc, cache, clock.NewManual(now), nil, Config{MaxBookAge: 10 * time.Second})
	return &efix{t: t, ctx: ctx, db: sqlDB, store: store, rc: rc, cache: cache, cfg: cfg, e: e, now: now, exCode: exCode, exID: exID}
}

// seedMarket inserts a market + symbol_config + fees, activates a config version,
// reloads the cache, and returns the loaded MarketConfig + its canonical symbol.
func (f *efix) seedMarket(quote string, minSpreadBps int, makerFee, takerFee string, signal, trading bool) (configstore.MarketConfig, string) {
	f.t.Helper()
	eseq++
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	ex := func(q string, a ...any) sql.Result {
		r, err := f.db.Exec(q, a...)
		if err != nil {
			f.t.Fatalf("seed %q: %v", q, err)
		}
		return r
	}
	// Include the (globally unique, monotonic) exID so symbols never collide across
	// repeated runs against the same persistent test DB.
	base := fmt.Sprintf("B%d_%d", f.exID, eseq)
	canonical := base + "/" + quote
	bID := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", base))
	qID := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", fmt.Sprintf("Q%d_%d", f.exID, eseq)))
	mID := last(ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", canonical, bID, qID))
	emID := last(ex(`INSERT INTO exchange_markets
		(exchange_id, market_id, exchange_symbol, canonical_symbol, enabled_for_collection, enabled_for_signal, enabled_for_trading, enabled_for_sell_manage)
		VALUES (?, ?, ?, ?, 1, ?, ?, 1)`, f.exID, mID, base+quote, canonical, b2i(signal), b2i(trading)))
	ex(`INSERT INTO symbol_configs (exchange_market_id, min_spread_bps, buy_size, buy_size_unit, sell_offset_bps, reprice_interval_seconds, order_timeout_ms, max_retries, retry_backoff_ms)
		VALUES (?, ?, '1', 'base', 20, 5, 3000, 3, 500)`, emID, minSpreadBps)
	ex("INSERT INTO exchange_fees (exchange_id, exchange_market_id, maker_fee, taker_fee) VALUES (?, ?, ?, ?)", f.exID, emID, makerFee, takerFee)

	if _, err := f.cfg.ActivateVersion(f.ctx, "test", "seed"); err != nil {
		f.t.Fatalf("activate: %v", err)
	}
	if err := f.cache.Reload(f.ctx, f.cfg); err != nil {
		f.t.Fatalf("reload: %v", err)
	}
	mc, ok := f.cache.Snapshot().Market(emID)
	if !ok {
		f.t.Fatalf("market %d not in snapshot", emID)
	}
	return mc, canonical
}

func (f *efix) seedBook(exchange, symbol, bid, ask string, age time.Duration) {
	f.t.Helper()
	bk := domain.OrderBook{
		Exchange: exchange, Symbol: symbol, UpdatedAt: f.now.Add(-age),
		Bids: []domain.Level{{Price: decimal.RequireFromString(bid), Quantity: decimal.RequireFromString("1")}},
		Asks: []domain.Level{{Price: decimal.RequireFromString(ask), Quantity: decimal.RequireFromString("1")}},
	}
	bs, _, _ := events.Build(bk, f.now.Add(-age), f.now.Add(-age))
	if err := f.rc.SaveOrderBook(f.ctx, bs); err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { f.rc.Redis().Del(context.Background(), redisx.OrderBookKey(exchange, symbol)) })
}

func (f *efix) seedRate(exchange, bid string, age time.Duration) {
	f.t.Helper()
	ps := events.PriceSnapshot{
		Exchange: exchange, Symbol: "USDT/IRT",
		BestBid: decimal.RequireFromString(bid), BestAsk: decimal.RequireFromString(bid),
		ExchangeTime: f.now.Add(-age),
	}
	if err := f.rc.SavePrice(f.ctx, ps); err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { f.rc.Redis().Del(context.Background(), redisx.PriceKey(exchange, "USDT/IRT")) })
}

// enableBuyPrep turns on PR9 buy-cycle preparation for tests that exercise cycle creation.
// (The engine is signal-only by default — PR8 boundary.)
func (f *efix) enableBuyPrep() { f.e.cfg.PrepareBuyCycles = true }

// run evaluates one market and fails on a hard error.
func (f *efix) run(mc configstore.MarketConfig) {
	f.t.Helper()
	if err := f.e.evaluate(f.ctx, f.cache.Snapshot(), mc); err != nil {
		f.t.Fatalf("evaluate: %v", err)
	}
}

func (f *efix) comparisonCount(canonical string) int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM comparison_events WHERE canonical_symbol=?", canonical).Scan(&n)
	return n
}
func (f *efix) signalCount(canonical string) int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM signals WHERE canonical_symbol=?", canonical).Scan(&n)
	return n
}

// ---- tests ----

func TestUSDTSignalCreatedWhenCheap(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("USDT", 50, "0", "0", true, false)
	base := baseOf(sym)
	f.seedBook(f.exCode, sym, "99", "100", 0)            // Iranian ask 100
	f.seedBook("binance", base+"/USDT", "101", "102", 0) // Binance bid 101
	f.run(mc)

	if f.comparisonCount(sym) != 1 {
		t.Fatalf("comparison_events = %d, want 1", f.comparisonCount(sym))
	}
	var (
		quote               string
		binPrice, iranPrice string
		refRate             sql.NullString
		spread, feeAdj      int
		passed              int
		version             int64
	)
	err := f.db.QueryRow(`SELECT quote_unit, binance_price, iranian_price, reference_rate, spread_bps, fee_adjusted_spread_bps, passed, config_version
		FROM comparison_events WHERE canonical_symbol=?`, sym).
		Scan(&quote, &binPrice, &iranPrice, &refRate, &spread, &feeAdj, &passed, &version)
	if err != nil {
		t.Fatal(err)
	}
	if quote != "USDT" || refRate.Valid {
		t.Errorf("quote=%s refRate=%v, want USDT / NULL", quote, refRate)
	}
	if !decimal.RequireFromString(binPrice).Equal(dec("101")) || !decimal.RequireFromString(iranPrice).Equal(dec("100")) {
		t.Errorf("prices bin=%s iran=%s, want 101/100", binPrice, iranPrice)
	}
	if spread != 100 || feeAdj != 100 || passed != 1 {
		t.Errorf("spread=%d feeAdj=%d passed=%d, want 100/100/1", spread, feeAdj, passed)
	}
	if version == 0 {
		t.Error("config_version not stamped on comparison")
	}
	// A signal was created, also config-stamped.
	if f.signalCount(sym) != 1 {
		t.Fatalf("signals = %d, want 1", f.signalCount(sym))
	}
	var sigVer int64
	f.db.QueryRow("SELECT config_version FROM signals WHERE canonical_symbol=?", sym).Scan(&sigVer)
	if sigVer != version {
		t.Errorf("signal config_version=%d, want %d", sigVer, version)
	}
}

func TestNoSignalBelowThreshold(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("USDT", 200, "0", "0", true, false) // need 200 bps
	base := baseOf(sym)
	f.seedBook(f.exCode, sym, "99", "100", 0)
	f.seedBook("binance", base+"/USDT", "101", "102", 0) // only 100 bps
	f.run(mc)

	var passed int
	if err := f.db.QueryRow("SELECT passed FROM comparison_events WHERE canonical_symbol=?", sym).Scan(&passed); err != nil {
		t.Fatal(err)
	}
	if passed != 0 {
		t.Error("comparison should be passed=0 below threshold")
	}
	if f.signalCount(sym) != 0 {
		t.Error("no signal expected below threshold")
	}
}

func TestNoSignalStaleIranian(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("USDT", 50, "0", "0", true, false)
	base := baseOf(sym)
	f.seedBook(f.exCode, sym, "99", "100", time.Hour) // stale
	f.seedBook("binance", base+"/USDT", "101", "102", 0)
	f.run(mc)
	if f.comparisonCount(sym) != 0 || f.signalCount(sym) != 0 {
		t.Error("stale Iranian data must produce no comparison/signal")
	}
}

func TestNoSignalStaleBinance(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("USDT", 50, "0", "0", true, false)
	base := baseOf(sym)
	f.seedBook(f.exCode, sym, "99", "100", 0)
	f.seedBook("binance", base+"/USDT", "101", "102", time.Hour) // stale reference
	f.run(mc)
	if f.comparisonCount(sym) != 0 || f.signalCount(sym) != 0 {
		t.Error("stale Binance data must produce no comparison/signal")
	}
}

func TestNoSignalMissingBinance(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("USDT", 50, "0", "0", true, false)
	f.seedBook(f.exCode, sym, "99", "100", 0) // no binance book at all
	f.run(mc)
	if f.comparisonCount(sym) != 0 || f.signalCount(sym) != 0 {
		t.Error("missing Binance data must produce no comparison/signal")
	}
}

func TestDisabledForSignalNoComparison(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("USDT", 50, "0", "0", false, false) // signal disabled
	base := baseOf(sym)
	f.seedBook(f.exCode, sym, "99", "100", 0)
	f.seedBook("binance", base+"/USDT", "101", "102", 0)
	f.run(mc)
	if f.comparisonCount(sym) != 0 || f.signalCount(sym) != 0 {
		t.Error("signal-disabled market must produce no comparison/signal")
	}
}

func TestIRTConversionUsesRate(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("IRT", 50, "0", "0", true, false)
	base := baseOf(sym)
	// Binance bid 2 USDT, USDT/IRT rate 50000 -> reference 100000 IRT. Iranian ask
	// 99000 IRT -> ~101 bps edge.
	f.seedBook(f.exCode, sym, "98000", "99000", 0)
	f.seedBook("binance", base+"/USDT", "2", "2.1", 0)
	f.seedRate(f.exCode, "50000", 0)
	f.run(mc)

	var quote, binPrice string
	var refRate sql.NullString
	var passed int
	err := f.db.QueryRow("SELECT quote_unit, binance_price, reference_rate, passed FROM comparison_events WHERE canonical_symbol=?", sym).
		Scan(&quote, &binPrice, &refRate, &passed)
	if err != nil {
		t.Fatal(err)
	}
	if quote != "IRT" || !refRate.Valid || !decimal.RequireFromString(refRate.String).Equal(dec("50000")) {
		t.Errorf("quote=%s refRate=%v, want IRT / 50000", quote, refRate)
	}
	if !decimal.RequireFromString(binPrice).Equal(dec("100000")) {
		t.Errorf("binance_price=%s, want 100000 (converted to IRT)", binPrice)
	}
	if passed != 1 {
		t.Error("IRT comparison should pass")
	}
}

func TestIRTMissingRateNoSignal(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("IRT", 50, "0", "0", true, false)
	base := baseOf(sym)
	f.seedBook(f.exCode, sym, "98000", "99000", 0)
	f.seedBook("binance", base+"/USDT", "2", "2.1", 0)
	// No USDT/IRT rate seeded -> cannot convert -> no comparison.
	f.run(mc)
	if f.comparisonCount(sym) != 0 {
		t.Error("missing USDT/IRT rate must block IRT comparison")
	}
}

func TestFeeAdjustedReducesSpread(t *testing.T) {
	f := setupE(t)
	// fees 0.001 maker + 0.001 taker = 20 bps; raw spread 100 -> adjusted 80.
	mc, sym := f.seedMarket("USDT", 90, "0.001", "0.001", true, false) // need 90, get 80
	base := baseOf(sym)
	f.seedBook(f.exCode, sym, "99", "100", 0)
	f.seedBook("binance", base+"/USDT", "101", "102", 0)
	f.run(mc)

	var spread, feeAdj, passed int
	err := f.db.QueryRow("SELECT spread_bps, fee_adjusted_spread_bps, passed FROM comparison_events WHERE canonical_symbol=?", sym).
		Scan(&spread, &feeAdj, &passed)
	if err != nil {
		t.Fatal(err)
	}
	if spread != 100 || feeAdj != 80 {
		t.Errorf("spread=%d feeAdj=%d, want 100/80", spread, feeAdj)
	}
	if passed != 0 {
		t.Error("fee-adjusted (80) is below threshold (90) -> must not pass")
	}
}

// ---- buy-cycle creation wiring (§2a / §10a) — engine-level; buyflow internals
// are tested in internal/buyflow ----

func (f *efix) buyRequestCount(mc configstore.MarketConfig) int {
	var n int
	f.db.QueryRow(`SELECT COUNT(*) FROM exchange_requests er JOIN orders o ON o.id=er.order_id
		WHERE o.exchange_market_id=? AND er.request_type='PLACE_ORDER'`, mc.ExchangeMarketID).Scan(&n)
	return n
}
func (f *efix) cycleCount(mc configstore.MarketConfig) int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM cycles WHERE exchange_market_id=?", mc.ExchangeMarketID).Scan(&n)
	return n
}

func TestPassingSignalCreatesBuyCycle(t *testing.T) {
	f := setupE(t)
	f.enableBuyPrep()                                         // PR9 behavior
	mc, sym := f.seedMarket("USDT", 50, "0", "0", true, true) // trading enabled
	base := baseOf(sym)
	f.seedBook(f.exCode, sym, "99", "100", 0)
	f.seedBook("binance", base+"/USDT", "101", "102", 0)
	f.run(mc)

	if f.cycleCount(mc) != 1 || f.buyRequestCount(mc) != 1 {
		t.Fatalf("cycles=%d buyRequests=%d, want 1/1", f.cycleCount(mc), f.buyRequestCount(mc))
	}
	// The cycle, its lock, the buy order, and a QUEUED PLACE_ORDER request all exist.
	var cycState, ordState, reqStatus, mode string
	f.db.QueryRow(`SELECT c.state, o.state, er.status, o.intended_execution_mode
		FROM cycles c JOIN orders o ON o.cycle_id=c.id JOIN exchange_requests er ON er.order_id=o.id
		WHERE c.exchange_market_id=?`, mc.ExchangeMarketID).Scan(&cycState, &ordState, &reqStatus, &mode)
	if cycState != "BUY_REQUEST_QUEUED" || ordState != "QUEUED" || reqStatus != "QUEUED" {
		t.Errorf("states = cyc:%s ord:%s req:%s, want BUY_REQUEST_QUEUED/QUEUED/QUEUED", cycState, ordState, reqStatus)
	}
	if mode != "MAKER_FIRST" {
		t.Errorf("first attempt mode = %s, want MAKER_FIRST", mode)
	}
	var lockN int
	f.db.QueryRow("SELECT COUNT(*) FROM symbol_locks WHERE scope=? AND canonical_symbol=? AND state='ACTIVE'", f.exCode, sym).Scan(&lockN)
	if lockN != 1 {
		t.Errorf("active locks = %d, want 1", lockN)
	}
	// PR9 #8: the signal that created the cycle is linked to it (signals.cycle_id = cycle.id).
	var cycleID int64
	f.db.QueryRow("SELECT id FROM cycles WHERE exchange_market_id=?", mc.ExchangeMarketID).Scan(&cycleID)
	var linked sql.NullInt64
	f.db.QueryRow("SELECT cycle_id FROM signals WHERE canonical_symbol=? ORDER BY id DESC LIMIT 1", sym).Scan(&linked)
	if !linked.Valid || linked.Int64 != cycleID {
		t.Errorf("signals.cycle_id = %v, want linked to created cycle %d", linked, cycleID)
	}
}

func TestRepeatedSignalDoesNotDuplicateCycle(t *testing.T) {
	f := setupE(t)
	f.enableBuyPrep() // PR9 behavior
	mc, sym := f.seedMarket("USDT", 50, "0", "0", true, true)
	base := baseOf(sym)
	f.seedBook(f.exCode, sym, "99", "100", 0)
	f.seedBook("binance", base+"/USDT", "101", "102", 0)
	f.run(mc)
	f.run(mc) // repeated event — must refresh, not duplicate

	if f.cycleCount(mc) != 1 || f.buyRequestCount(mc) != 1 {
		t.Errorf("after repeat: cycles=%d buyRequests=%d, want 1/1 (lock blocks duplicate)", f.cycleCount(mc), f.buyRequestCount(mc))
	}
}

func TestRepeatedSignalRefreshesQueuedPrice(t *testing.T) {
	f := setupE(t)
	f.enableBuyPrep() // PR9 behavior
	mc, sym := f.seedMarket("USDT", 50, "0", "0", true, true)
	base := baseOf(sym)
	f.seedBook(f.exCode, sym, "99", "100", 0)
	f.seedBook("binance", base+"/USDT", "101", "102", 0)
	f.run(mc) // creates the cycle at ask 100

	// A newer, still-passing ask (99 vs Binance bid 101) -> the still-QUEUED buy is
	// refreshed in place (no new cycle).
	f.seedBook(f.exCode, sym, "98", "99", 0)
	f.run(mc)

	var ask string
	f.db.QueryRow(`SELECT o.ask_price_at_decision FROM orders o JOIN cycles c ON c.id=o.cycle_id
		WHERE c.exchange_market_id=? AND o.role='entry_buy'`, mc.ExchangeMarketID).Scan(&ask)
	if !decimal.RequireFromString(ask).Equal(dec("99")) {
		t.Errorf("refreshed ask_price_at_decision = %s, want 99", ask)
	}
}

func TestDisabledForTradingNoCycle(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("USDT", 50, "0", "0", true, false) // signal yes, trading no
	base := baseOf(sym)
	f.seedBook(f.exCode, sym, "99", "100", 0)
	f.seedBook("binance", base+"/USDT", "101", "102", 0)
	f.run(mc)

	if f.cycleCount(mc) != 0 || f.buyRequestCount(mc) != 0 {
		t.Error("trading-disabled market must create no cycle/buy request")
	}
	if f.signalCount(sym) != 1 {
		t.Error("a signal should still be recorded (signal-enabled)")
	}
}

func TestBelowThresholdNoCycle(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("USDT", 500, "0", "0", true, true) // need 500 bps
	base := baseOf(sym)
	f.seedBook(f.exCode, sym, "99", "100", 0)
	f.seedBook("binance", base+"/USDT", "101", "102", 0) // only 100 bps
	f.run(mc)
	if f.cycleCount(mc) != 0 || f.buyRequestCount(mc) != 0 {
		t.Error("below-threshold signal must create no cycle/buy request")
	}
}

// TestSignalOnlyMarketTouchesNoExecutionState (PR8 correction) — a signal-enabled but
// NOT-trading market writes ONLY comparison_events + signals. The engine must create no
// cycle/order/symbol-lock and must NOT mutate exchange_requests.
func TestSignalOnlyMarketTouchesNoExecutionState(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("USDT", 50, "0", "0", true, false) // signal yes, trading NO
	base := baseOf(sym)
	f.seedBook(f.exCode, sym, "99", "100", 0)
	f.seedBook("binance", base+"/USDT", "101", "102", 0) // ~100 bps > 50 → passes
	reqBefore := f.exchangeRequestCount()

	f.run(mc)

	if f.comparisonCount(sym) != 1 || f.signalCount(sym) != 1 {
		t.Errorf("signal path: comparison=%d signal=%d, want 1/1", f.comparisonCount(sym), f.signalCount(sym))
	}
	if n := f.cycleCount(mc); n != 0 {
		t.Errorf("cycles=%d, want 0 (signal-only must not create a cycle)", n)
	}
	if n := f.orderCountForMarket(mc); n != 0 {
		t.Errorf("orders=%d, want 0", n)
	}
	if n := f.lockCountForMarket(mc); n != 0 {
		t.Errorf("symbol_locks=%d, want 0", n)
	}
	if n := f.exchangeRequestCount(); n != reqBefore {
		t.Errorf("exchange_requests changed %d -> %d; the trade-engine must not mutate the queue", reqBefore, n)
	}
}

// TestSignalOnlyEvenWhenTradingEnabled (PR8 correction) — the STRONG signal-only proof: with
// enabled_for_signal AND enabled_for_trading AND a PASSING signal, PR8 still writes ONLY
// comparison_events + signals and creates NO cycle/order/exchange_request/symbol_lock (the
// engine does not call buyflow). Buy-prep is OFF by default; this fails under the old
// unconditional prepareBuy behavior.
func TestSignalOnlyEvenWhenTradingEnabled(t *testing.T) {
	f := setupE(t)
	// Deliberately do NOT call f.enableBuyPrep(): PR8 default is signal-only.
	mc, sym := f.seedMarket("USDT", 50, "0", "0", true, true) // signal AND trading enabled
	base := baseOf(sym)
	f.seedBook(f.exCode, sym, "99", "100", 0)
	f.seedBook("binance", base+"/USDT", "101", "102", 0) // ~100 bps > 50 → signal passes
	reqBefore := f.exchangeRequestCount()

	f.run(mc)

	if f.comparisonCount(sym) != 1 || f.signalCount(sym) != 1 {
		t.Fatalf("signal path: comparison=%d signal=%d, want 1/1 (signal must pass)", f.comparisonCount(sym), f.signalCount(sym))
	}
	if n := f.cycleCount(mc); n != 0 {
		t.Errorf("cycles=%d, want 0 (PR8 is signal-only even when trading is enabled)", n)
	}
	if n := f.orderCountForMarket(mc); n != 0 {
		t.Errorf("orders=%d, want 0 (no buyflow in PR8)", n)
	}
	if n := f.buyRequestCount(mc); n != 0 {
		t.Errorf("buy requests=%d, want 0 (no enqueue in PR8)", n)
	}
	if n := f.symbolLockCount(sym); n != 0 {
		t.Errorf("symbol_locks=%d, want 0 (no lock in PR8)", n)
	}
	if n := f.exchangeRequestCount(); n != reqBefore {
		t.Errorf("exchange_requests changed %d -> %d; PR8 must not touch the queue", reqBefore, n)
	}
}

// TestQuoteRateEventReevaluatesDependentIRTMarket (PR8 correction) — a USDT/IRT tick must
// re-evaluate dependent IRT markets on the same exchange and write a signal when the spread
// passes. Drives targets()→evaluate exactly as Run does.
func TestQuoteRateEventReevaluatesDependentIRTMarket(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("IRT", 50, "0", "0", true, false) // IRT market depends on USDT/IRT
	base := baseOf(sym)
	f.seedBook(f.exCode, sym, "9", "10", 0)                    // iranian ask 10 (toman)
	f.seedBook("binance", base+"/USDT", "0.0101", "0.0102", 0) // binance bid 0.0101 USDT
	f.seedRate(f.exCode, "1000", 0)                            // USDT/IRT = 1000 → ref = 10.1 > 10 → ~100 bps

	snap := f.cache.Snapshot()
	ev := events.MarketEvent{Exchange: f.exCode, Symbol: "USDT/IRT"}
	tg := f.e.targets(snap, ev)
	found := false
	for _, m := range tg {
		if m.ExchangeMarketID == mc.ExchangeMarketID {
			found = true
		}
	}
	if !found {
		t.Fatalf("USDT/IRT event did not select dependent IRT market %s", sym)
	}
	for _, m := range tg {
		if err := f.e.evaluate(f.ctx, snap, m); err != nil {
			t.Fatalf("evaluate: %v", err)
		}
	}
	if f.comparisonCount(sym) < 1 {
		t.Errorf("dependent IRT market got no comparison_event on USDT/IRT tick")
	}
	if f.signalCount(sym) < 1 {
		t.Errorf("dependent IRT market got no signal (spread should pass)")
	}
}

// TestEngineUsesPerExchangeDefaultFeeAndOverride (PR8 correction) — the engine resolves fees
// via Snapshot.FeeFor(exchangeID, exchangeMarketID): two exchanges with DIFFERENT default
// fees must produce different fee-adjusted spreads (no shared key-0 leak), and a
// market-specific fee overrides the exchange default.
func TestEngineUsesPerExchangeDefaultFeeAndOverride(t *testing.T) {
	f := setupE(t)
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	ex := func(q string, a ...any) sql.Result {
		r, err := f.db.Exec(q, a...)
		if err != nil {
			f.t.Fatalf("seed %q: %v", q, err)
		}
		return r
	}
	// Exchange B, distinct from the fixture's exchange A (f.exID).
	eseq++
	bCode := fmt.Sprintf("engB_%d_%d", time.Now().UnixNano(), eseq)
	bID := last(ex("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'Eng B', 1)", bCode))

	// DEFAULT fees (exchange_market_id NULL): A has zero fee, B has 100 bps taker.
	ex("INSERT INTO exchange_fees (exchange_id, exchange_market_id, maker_fee, taker_fee) VALUES (?, NULL, '0', '0')", f.exID)
	ex("INSERT INTO exchange_fees (exchange_id, exchange_market_id, maker_fee, taker_fee) VALUES (?, NULL, '0', '0.01')", bID)

	// A USDT market on each exchange with the SAME prices and NO market-specific fee.
	seedNoFeeMarket := func(exID int64) (int64, string) {
		eseq++
		base := fmt.Sprintf("FB%d_%d", exID, eseq)
		canonical := base + "/USDT"
		bAsset := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", base))
		qAsset := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", fmt.Sprintf("FQ%d_%d", exID, eseq)))
		mID := last(ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", canonical, bAsset, qAsset))
		emID := last(ex(`INSERT INTO exchange_markets
			(exchange_id, market_id, exchange_symbol, canonical_symbol, enabled_for_collection, enabled_for_signal, enabled_for_trading, enabled_for_sell_manage)
			VALUES (?, ?, ?, ?, 1, 1, 0, 0)`, exID, mID, base+"USDT", canonical))
		ex(`INSERT INTO symbol_configs (exchange_market_id, min_spread_bps, buy_size, buy_size_unit, sell_offset_bps, reprice_interval_seconds, order_timeout_ms, max_retries, retry_backoff_ms)
			VALUES (?, 10, '1', 'base', 20, 5, 3000, 3, 500)`, emID)
		return emID, canonical
	}
	emA, symA := seedNoFeeMarket(f.exID)
	emB, symB := seedNoFeeMarket(bID)

	if _, err := f.cfg.ActivateVersion(f.ctx, "test", "fee"); err != nil {
		f.t.Fatal(err)
	}
	if err := f.cache.Reload(f.ctx, f.cfg); err != nil {
		f.t.Fatal(err)
	}
	snap := f.cache.Snapshot()
	mcA, _ := snap.Market(emA)
	mcB, _ := snap.Market(emB)

	// Same prices for both → same RAW spread (1000 bps), different fee-adjusted by exchange.
	f.seedBook(f.exCode, symA, "99", "100", 0)
	f.seedBook(bCode, symB, "99", "100", 0)
	f.seedBook("binance", baseOf(symA)+"/USDT", "110", "111", 0)
	f.seedBook("binance", baseOf(symB)+"/USDT", "110", "111", 0)

	if err := f.e.evaluate(f.ctx, snap, mcA); err != nil {
		f.t.Fatal(err)
	}
	if err := f.e.evaluate(f.ctx, snap, mcB); err != nil {
		f.t.Fatal(err)
	}
	faA, faB := f.lastFeeAdjusted(symA), f.lastFeeAdjusted(symB)
	if faA <= faB {
		t.Errorf("fee-adjusted A=%d B=%d; A (no fee) must exceed B (100bps default) — per-exchange fee not applied", faA, faB)
	}
	if faA-faB != 100 {
		t.Errorf("fee-adjusted gap = %d, want 100 bps (exchange B's default taker fee)", faA-faB)
	}

	// Market-specific override on B (zero fee) must beat B's exchange default.
	ex("INSERT INTO exchange_fees (exchange_id, exchange_market_id, maker_fee, taker_fee) VALUES (?, ?, '0', '0')", bID, emB)
	if _, err := f.cfg.ActivateVersion(f.ctx, "test", "override"); err != nil {
		f.t.Fatal(err)
	}
	if err := f.cache.Reload(f.ctx, f.cfg); err != nil {
		f.t.Fatal(err)
	}
	snap2 := f.cache.Snapshot()
	mcB2, _ := snap2.Market(emB)
	if err := f.e.evaluate(f.ctx, snap2, mcB2); err != nil {
		f.t.Fatal(err)
	}
	if faB2 := f.lastFeeAdjusted(symB); faB2 != faA {
		t.Errorf("after override, B fee-adjusted = %d, want %d (override fee 0 beats the 100bps default)", faB2, faA)
	}
}

// ---- helpers ----

func (f *efix) orderCountForMarket(mc configstore.MarketConfig) int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM orders WHERE exchange_market_id=?", mc.ExchangeMarketID).Scan(&n)
	return n
}
func (f *efix) lockCountForMarket(mc configstore.MarketConfig) int {
	var n int
	f.db.QueryRow(`SELECT COUNT(*) FROM symbol_locks sl JOIN cycles c ON c.id=sl.cycle_id WHERE c.exchange_market_id=?`, mc.ExchangeMarketID).Scan(&n)
	return n
}
func (f *efix) exchangeRequestCount() int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE exchange_id=?", f.exID).Scan(&n)
	return n
}
func (f *efix) symbolLockCount(canonical string) int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM symbol_locks WHERE scope=? AND canonical_symbol=?", f.exCode, canonical).Scan(&n)
	return n
}
func (f *efix) lastFeeAdjusted(canonical string) int {
	var n int
	f.db.QueryRow("SELECT fee_adjusted_spread_bps FROM comparison_events WHERE canonical_symbol=? ORDER BY id DESC LIMIT 1", canonical).Scan(&n)
	return n
}

func redisConfig(addr string) config.RedisConfig { return config.RedisConfig{Addr: addr, DB: 15} }

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
