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
	base := fmt.Sprintf("B%d", eseq)
	canonical := base + "/" + quote
	bID := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", base))
	qID := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", fmt.Sprintf("Q%d", eseq)))
	mID := last(ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", canonical, bID, qID))
	emID := last(ex(`INSERT INTO exchange_markets
		(exchange_id, market_id, exchange_symbol, canonical_symbol, enabled_for_collection, enabled_for_signal, enabled_for_trading, enabled_for_sell_manage)
		VALUES (?, ?, ?, ?, 1, ?, ?, 0)`, f.exID, mID, base+quote, canonical, b2i(signal), b2i(trading)))
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

// ---- pending-intent (§2a) ----

// seedQueuedBuy creates a cycle + entry_buy order + QUEUED PLACE_ORDER request for
// the market, with the given request status and payload. Returns the request id.
func (f *efix) seedQueuedBuy(mc configstore.MarketConfig, status, payload string) int64 {
	f.t.Helper()
	eseq++
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	cyc := last(mustExec(f.t, f.db, "INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state) VALUES (?, ?, ?, 'BUY_REQUEST_QUEUED')",
		mc.ExchangeMarketID, mc.ExchangeID, mc.CanonicalSymbol))
	ord := last(mustExec(f.t, f.db, `INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, quantity)
		VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'QUEUED', '1')`,
		cyc, mc.ExchangeID, mc.ExchangeMarketID, fmt.Sprintf("loc%d", eseq)))
	req := last(mustExec(f.t, f.db, `INSERT INTO exchange_requests (exchange_id, order_id, request_type, status, payload, idempotency_key)
		VALUES (?, ?, 'PLACE_ORDER', ?, ?, ?)`,
		mc.ExchangeID, ord, status, payload, fmt.Sprintf("idem%d", eseq)))
	return req
}

func (f *efix) reqPayload(id int64) string {
	var p string
	f.db.QueryRow("SELECT payload FROM exchange_requests WHERE id=?", id).Scan(&p)
	return p
}
func (f *efix) reqExists(id int64) bool {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE id=?", id).Scan(&n)
	return n == 1
}

func TestEnabledForTradingUpdatesPendingIntent(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("USDT", 50, "0", "0", true, true) // trading enabled
	base := baseOf(sym)
	req := f.seedQueuedBuy(mc, "QUEUED", `{"old":true}`)
	f.seedBook(f.exCode, sym, "99", "100", 0)
	f.seedBook("binance", base+"/USDT", "101", "102", 0)
	f.run(mc)

	p := f.reqPayload(req)
	if p == `{"old":true}` || !contains(p, "signal_id") {
		t.Errorf("QUEUED intent should be refreshed to the new signal, got %s", p)
	}
}

func TestDisabledForTradingLeavesPendingIntent(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("USDT", 50, "0", "0", true, false) // trading disabled
	base := baseOf(sym)
	req := f.seedQueuedBuy(mc, "QUEUED", `{"old":true}`)
	f.seedBook(f.exCode, sym, "99", "100", 0)
	f.seedBook("binance", base+"/USDT", "101", "102", 0)
	f.run(mc)

	if f.reqPayload(req) != `{"old":true}` {
		t.Error("trading-disabled market must not touch the pending intent")
	}
	if f.signalCount(sym) != 1 {
		t.Error("a signal should still be recorded (signal-enabled)")
	}
}

func TestInvalidatedSignalRemovesPendingIntent(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("USDT", 500, "0", "0", true, true) // need 500 bps -> fails
	base := baseOf(sym)
	req := f.seedQueuedBuy(mc, "QUEUED", `{"old":true}`)
	f.seedBook(f.exCode, sym, "99", "100", 0)
	f.seedBook("binance", base+"/USDT", "101", "102", 0) // only 100 bps -> invalid
	f.run(mc)

	if f.reqExists(req) {
		t.Error("an invalidated signal must remove the not-yet-sent QUEUED intent")
	}
}

func TestRepeatedEventsNoDuplicateIntent(t *testing.T) {
	f := setupE(t)
	mc, sym := f.seedMarket("USDT", 50, "0", "0", true, true)
	base := baseOf(sym)
	f.seedQueuedBuy(mc, "QUEUED", `{"old":true}`)
	f.seedBook(f.exCode, sym, "99", "100", 0)
	f.seedBook("binance", base+"/USDT", "101", "102", 0)
	f.run(mc)
	f.run(mc) // repeated event

	var n int
	f.db.QueryRow(`SELECT COUNT(*) FROM exchange_requests er JOIN orders o ON o.id=er.order_id
		WHERE o.exchange_market_id=? AND er.request_type='PLACE_ORDER'`, mc.ExchangeMarketID).Scan(&n)
	if n != 1 {
		t.Errorf("pending buy requests = %d, want exactly 1 (no duplicate intent)", n)
	}
}

func TestUpdatePendingBuyOnlyQueued(t *testing.T) {
	f := setupE(t)
	mc, _ := f.seedMarket("USDT", 50, "0", "0", true, true)
	// QUEUED -> updated.
	reqQ := f.seedQueuedBuy(mc, "QUEUED", `{"old":true}`)
	updated, err := UpdatePendingBuyRequest(f.ctx, f.store, mc.ExchangeMarketID, []byte(`{"new":true}`))
	if err != nil || !updated {
		t.Fatalf("update QUEUED = %v, %v; want true", updated, err)
	}
	if f.reqPayload(reqQ) != `{"new":true}` {
		t.Errorf("QUEUED payload not updated: %s", f.reqPayload(reqQ))
	}
	// CLAIMED -> not touched.
	mc2, _ := f.seedMarket("USDT", 50, "0", "0", true, true)
	reqC := f.seedQueuedBuy(mc2, "CLAIMED", `{"claimed":true}`)
	updated, err = UpdatePendingBuyRequest(f.ctx, f.store, mc2.ExchangeMarketID, []byte(`{"new":true}`))
	if err != nil || updated {
		t.Fatalf("update CLAIMED = %v, %v; want false", updated, err)
	}
	if f.reqPayload(reqC) != `{"claimed":true}` {
		t.Error("CLAIMED request must not be modified")
	}
}

func TestRemovePendingBuyOnlyQueued(t *testing.T) {
	f := setupE(t)
	mc, _ := f.seedMarket("USDT", 50, "0", "0", true, true)
	reqQ := f.seedQueuedBuy(mc, "QUEUED", `{}`)
	removed, err := RemovePendingBuyRequest(f.ctx, f.store, mc.ExchangeMarketID)
	if err != nil || !removed || f.reqExists(reqQ) {
		t.Fatalf("remove QUEUED = %v, %v, exists=%v; want removed", removed, err, f.reqExists(reqQ))
	}
	mc2, _ := f.seedMarket("USDT", 50, "0", "0", true, true)
	reqC := f.seedQueuedBuy(mc2, "IN_FLIGHT", `{}`)
	removed, err = RemovePendingBuyRequest(f.ctx, f.store, mc2.ExchangeMarketID)
	if err != nil || removed || !f.reqExists(reqC) {
		t.Fatalf("remove IN_FLIGHT = %v, %v; must not remove a sent request", removed, err)
	}
}

func TestPendingHelpersNoopWhenNone(t *testing.T) {
	f := setupE(t)
	mc, _ := f.seedMarket("USDT", 50, "0", "0", true, true) // no request seeded
	if u, err := UpdatePendingBuyRequest(f.ctx, f.store, mc.ExchangeMarketID, []byte(`{}`)); u || err != nil {
		t.Errorf("update with no pending = %v, %v; want false,nil", u, err)
	}
	if r, err := RemovePendingBuyRequest(f.ctx, f.store, mc.ExchangeMarketID); r || err != nil {
		t.Errorf("remove with no pending = %v, %v; want false,nil", r, err)
	}
}

// ---- helpers ----

func redisConfig(addr string) config.RedisConfig { return config.RedisConfig{Addr: addr, DB: 15} }

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func mustExec(t *testing.T, db *sql.DB, q string, a ...any) sql.Result {
	t.Helper()
	r, err := db.Exec(q, a...)
	if err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
	return r
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
