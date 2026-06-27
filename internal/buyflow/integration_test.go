package buyflow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/configstore"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/migrate"
	"v3TradeBot/internal/queue"
)

// buyflow integration tests run the real cycle-creation transaction against a real
// MariaDB. Skipped unless V3_TEST_MYSQL_DSN points at a throwaway database. No
// exchange is ever contacted (buyflow holds no client by construction).

var bseq int

type bfix struct {
	t      *testing.T
	ctx    context.Context
	db     *sql.DB
	store  *db.Store
	q      *queue.Queue
	exCode string
	exID   int64
}

func setupB(t *testing.T) *bfix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the buyflow integration test")
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
	bseq++
	exCode := fmt.Sprintf("bf_%d_%d", time.Now().UnixNano(), bseq)
	res, err := sqlDB.Exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'BF', 1)", exCode)
	if err != nil {
		t.Fatal(err)
	}
	exID, _ := res.LastInsertId()
	return &bfix{t: t, ctx: ctx, db: sqlDB, store: db.NewFromDB(sqlDB), q: queue.New(sqlDB, clock.NewSystem()), exCode: exCode, exID: exID}
}

// market inserts the reference rows (assets/markets/exchange_markets) and returns a
// hand-built MarketConfig with the given maker policy + buy size.
func (f *bfix) market(quote, buySize, buySizeUnit string, p configstore.MakerPolicy) configstore.MarketConfig {
	f.t.Helper()
	bseq++
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	ex := func(q string, a ...any) sql.Result {
		r, err := f.db.Exec(q, a...)
		if err != nil {
			f.t.Fatalf("seed %q: %v", q, err)
		}
		return r
	}
	// Include the unique exID so seeded symbols never collide across repeated runs.
	canonical := fmt.Sprintf("BF%d_%d/%s", f.exID, bseq, quote)
	b := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", fmt.Sprintf("BB%d_%d", f.exID, bseq)))
	qa := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", fmt.Sprintf("BQ%d_%d", f.exID, bseq)))
	m := last(ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", canonical, b, qa))
	// collection ⊇ signal ⊇ trading (configstore validation invariant).
	em := last(ex("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol, enabled_for_collection, enabled_for_signal, enabled_for_trading) VALUES (?, ?, ?, ?, 1, 1, 1)",
		f.exID, m, fmt.Sprintf("BFES%d_%d", f.exID, bseq), canonical))
	// A trading-enabled market must have a symbol_config (configstore validation),
	// so the full-snapshot validation in other packages' tests stays green.
	ex("INSERT INTO symbol_configs (exchange_market_id, min_spread_bps, buy_size, buy_size_unit, order_timeout_ms, max_retries) VALUES (?, 50, ?, ?, 3000, 3)",
		em, buySize, buySizeUnit)
	return configstore.MarketConfig{
		ExchangeMarketID:  em,
		ExchangeID:        f.exID,
		ExchangeCode:      f.exCode,
		CanonicalSymbol:   canonical,
		EnabledForSignal:  true,
		EnabledForTrading: true,
		HasSymbolConfig:   true,
		BuySize:           decimal.RequireFromString(buySize),
		BuySizeUnit:       buySizeUnit,
		OrderTimeoutMs:    3000,
		MaxRetries:        3,
		Maker:             p,
	}
}

func (f *bfix) sig() SignalContext {
	return SignalContext{BinancePrice: dec("101"), IranianPrice: dec("100"), SpreadBps: 100, FeeAdjustedBps: 100, QuoteUnit: "USDT", BuyFeeBps: 0, SellFeeBps: 0}
}

func (f *bfix) releaseLock(cycleID int64) {
	f.t.Helper()
	if _, err := f.db.Exec("UPDATE symbol_locks SET state='RELEASED', released_at=NOW(6) WHERE cycle_id=? AND state='ACTIVE'", cycleID); err != nil {
		f.t.Fatal(err)
	}
}

func TestCreateBuyCycleAtomic(t *testing.T) {
	f := setupB(t)
	m := f.market("USDT", "0.5", "base", policy(true, 2, 10))
	r, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), f.sig(), 600)
	if err != nil {
		t.Fatalf("CreateBuyCycle: %v", err)
	}
	// Cycle / order / request / lock are all created and consistent.
	var cycState, ordState, reqStatus, role, mode, local, idem string
	var limit, qty, ask string
	err = f.db.QueryRow(`SELECT c.state, o.state, er.status, o.role, o.intended_execution_mode,
		o.local_client_order_id, er.idempotency_key, o.limit_price, o.quantity, o.ask_price_at_decision
		FROM cycles c JOIN orders o ON o.cycle_id=c.id JOIN exchange_requests er ON er.order_id=o.id
		WHERE c.id=?`, r.CycleID).Scan(&cycState, &ordState, &reqStatus, &role, &mode, &local, &idem, &limit, &qty, &ask)
	if err != nil {
		t.Fatal(err)
	}
	if cycState != "BUY_REQUEST_QUEUED" || ordState != "QUEUED" || reqStatus != "QUEUED" {
		t.Errorf("states = %s/%s/%s, want BUY_REQUEST_QUEUED/QUEUED/QUEUED", cycState, ordState, reqStatus)
	}
	if role != "entry_buy" || mode != "MAKER_FIRST" {
		t.Errorf("role/mode = %s/%s", role, mode)
	}
	if local != fmt.Sprintf("c%d-buy", r.CycleID) || idem != fmt.Sprintf("place-order:c%d:buy", r.CycleID) {
		t.Errorf("identifiers local=%s idem=%s", local, idem)
	}
	if !decimal.RequireFromString(limit).Equal(dec("99.9")) || !decimal.RequireFromString(qty).Equal(dec("0.5")) || !decimal.RequireFromString(ask).Equal(dec("100")) {
		t.Errorf("limit/qty/ask = %s/%s/%s, want 99.9/0.5/100", limit, qty, ask)
	}
	// State-machine events were written for both transitions of each entity.
	var cycEvents, ordEvents int
	f.db.QueryRow("SELECT COUNT(*) FROM cycle_state_events WHERE cycle_id=?", r.CycleID).Scan(&cycEvents)
	f.db.QueryRow("SELECT COUNT(*) FROM order_events WHERE order_id=?", r.OrderID).Scan(&ordEvents)
	if cycEvents != 2 || ordEvents != 2 {
		t.Errorf("events cyc=%d ord=%d, want 2/2 (NEW→SIGNAL_DETECTED→QUEUED, NEW→REGISTERED→QUEUED)", cycEvents, ordEvents)
	}
	// Payload carries the executor instruction set.
	var payload string
	f.db.QueryRow("SELECT payload FROM exchange_requests WHERE id=?", r.RequestID).Scan(&payload)
	for _, want := range []string{`"execution_mode":"MAKER_FIRST"`, `"simulated_ioc":true`, `"cancel_after_wait":true`, `"intended_price":"99.9"`} {
		if !strings.Contains(payload, want) {
			t.Errorf("payload missing %s: %s", want, payload)
		}
	}
}

func TestDuplicateLockBlocksAndRollsBack(t *testing.T) {
	f := setupB(t)
	m := f.market("USDT", "0.5", "base", policy(true, 2, 10))
	if _, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), f.sig(), 600); err != nil {
		t.Fatalf("first create: %v", err)
	}
	// Second attempt while the scope is locked must fail with ErrSymbolLocked and
	// create NOTHING (the cycle insert in that tx rolls back — no orphan).
	_, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), f.sig(), 600)
	if !errors.Is(err, ErrSymbolLocked) {
		t.Fatalf("second create err = %v, want ErrSymbolLocked", err)
	}
	if n := f.count("cycles", m); n != 1 {
		t.Errorf("cycles = %d, want 1 (second attempt rolled back, no orphan)", n)
	}
	if n := f.buyReqs(m); n != 1 {
		t.Errorf("buy requests = %d, want 1", n)
	}
}

func TestMakerEscalatesToTakerAcrossAttempts(t *testing.T) {
	f := setupB(t)
	m := f.market("USDT", "0.5", "base", policy(true, 2, 10)) // taker after 2 maker attempts
	// Each attempt must release the lock first (as a maker no-fill would in PR10).
	r1, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), f.sig(), 600)
	if err != nil {
		t.Fatal(err)
	}
	f.releaseLock(r1.CycleID)
	r2, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), f.sig(), 600)
	if err != nil {
		t.Fatal(err)
	}
	f.releaseLock(r2.CycleID)
	r3, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), f.sig(), 600)
	if err != nil {
		t.Fatal(err)
	}
	if r1.Decision.Mode != ModeMakerFirst || r2.Decision.Mode != ModeMakerRetry || r3.Decision.Mode != ModeTakerFallback {
		t.Errorf("modes = %s/%s/%s, want MAKER_FIRST/MAKER_RETRY/TAKER_FALLBACK", r1.Decision.Mode, r2.Decision.Mode, r3.Decision.Mode)
	}
	if r3.Decision.AttemptNumber != 3 {
		t.Errorf("third attempt number = %d, want 3", r3.Decision.AttemptNumber)
	}
}

func TestWindowResetReturnsToMaker(t *testing.T) {
	f := setupB(t)
	m := f.market("USDT", "0.5", "base", policy(true, 1, 10)) // taker after 1 maker attempt, 60s window
	r1, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), f.sig(), 600)
	if err != nil {
		t.Fatal(err)
	}
	f.releaseLock(r1.CycleID)
	// Backdate the first cycle's opportunity window beyond the configured window so
	// it no longer counts (the new attempt resets to maker-first).
	if _, err := f.db.Exec("UPDATE cycles SET opportunity_window_started_at = NOW(6) - INTERVAL 2 HOUR WHERE id=?", r1.CycleID); err != nil {
		t.Fatal(err)
	}
	r2, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), f.sig(), 600)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Decision.Mode != ModeMakerFirst {
		t.Errorf("after window reset mode = %s, want MAKER_FIRST", r2.Decision.Mode)
	}
}

func TestQuoteSizingDividesByPrice(t *testing.T) {
	f := setupB(t)
	m := f.market("USDT", "1000", "quote", policy(true, 2, 0)) // offset 0 -> maker limit == ask 100
	r, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), f.sig(), 600)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Quantity.Equal(dec("10")) { // 1000 quote / price 100
		t.Errorf("quote-sized quantity = %s, want 10", r.Quantity)
	}
}

func TestConfigVersionStamped(t *testing.T) {
	f := setupB(t)
	m := f.market("USDT", "0.5", "base", policy(true, 2, 10))
	ver, err := configstore.New(f.db).ActivateVersion(f.ctx, "test", "seed")
	if err != nil {
		t.Fatal(err)
	}
	sig := f.sig()
	sig.ConfigVersion = ver
	r, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), sig, 600)
	if err != nil {
		t.Fatal(err)
	}
	var got sql.NullInt64
	f.db.QueryRow("SELECT config_version FROM cycles WHERE id=?", r.CycleID).Scan(&got)
	if !got.Valid || got.Int64 != ver {
		t.Errorf("cycle config_version = %v, want %d", got, ver)
	}
}

// snapshot of an active cycle's buy state (cycle mode/attempt, order mode/attempt/
// price, request payload) for cross-entity consistency assertions.
func (f *bfix) buyState(m configstore.MarketConfig) (cycMode string, cycAttempt int, ordMode string, ordAttempt int, limit, ask, payload string) {
	f.t.Helper()
	err := f.db.QueryRow(`SELECT c.intended_execution_mode, c.maker_attempt_number,
		o.intended_execution_mode, o.maker_attempt_number, o.limit_price, o.ask_price_at_decision, er.payload
		FROM symbol_locks sl JOIN cycles c ON c.id=sl.cycle_id
		JOIN orders o ON o.cycle_id=c.id AND o.role='entry_buy'
		JOIN exchange_requests er ON er.order_id=o.id AND er.request_type='PLACE_ORDER'
		WHERE sl.state='ACTIVE' AND sl.canonical_symbol=?`, m.CanonicalSymbol).
		Scan(&cycMode, &cycAttempt, &ordMode, &ordAttempt, &limit, &ask, &payload)
	if err != nil {
		f.t.Fatalf("buyState: %v", err)
	}
	return
}

// TestRefreshAdvancesAttemptAndEscalates is the owner-required edge case: repeated
// valid signals on a still-QUEUED request advance the attempt counter and escalate
// MAKER_FIRST -> MAKER_RETRY -> TAKER_FALLBACK on the SAME request (no duplicate),
// with cycle/order/request kept consistent.
func TestRefreshAdvancesAttemptAndEscalates(t *testing.T) {
	f := setupB(t)
	m := f.market("USDT", "0.5", "base", policy(true, 2, 10)) // taker after 2 maker attempts, offset 10
	if _, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), f.sig(), 600); err != nil {
		t.Fatal(err)
	}
	// Attempt 1 at create.
	cm, ca, om, oa, limit, _, payload := f.buyState(m)
	if cm != "MAKER_FIRST" || ca != 1 || om != "MAKER_FIRST" || oa != 1 || !decimal.RequireFromString(limit).Equal(dec("99.9")) {
		t.Fatalf("after create: cyc=%s/%d ord=%s/%d limit=%s, want MAKER_FIRST/1 .. 99.9", cm, ca, om, oa, limit)
	}

	// Refresh 1 -> attempt 2, MAKER_RETRY, new ask 200 -> limit 199.8 (offset 10).
	if ok, err := RefreshActiveCycleBuy(f.ctx, f.store, m, dec("200"), f.sig()); err != nil || !ok {
		t.Fatalf("refresh1 = %v, %v; want true", ok, err)
	}
	cm, ca, om, oa, limit, ask, payload := f.buyState(m)
	if cm != "MAKER_RETRY" || ca != 2 || om != "MAKER_RETRY" || oa != 2 {
		t.Errorf("after refresh1: cyc=%s/%d ord=%s/%d, want MAKER_RETRY/2", cm, ca, om, oa)
	}
	if !decimal.RequireFromString(limit).Equal(dec("199.8")) || !decimal.RequireFromString(ask).Equal(dec("200")) {
		t.Errorf("after refresh1: limit/ask = %s/%s, want 199.8/200", limit, ask)
	}

	// Refresh 2 -> attempt 3 exceeds threshold 2 -> TAKER_FALLBACK at the ask.
	if ok, err := RefreshActiveCycleBuy(f.ctx, f.store, m, dec("300"), f.sig()); err != nil || !ok {
		t.Fatalf("refresh2 = %v, %v; want true", ok, err)
	}
	cm, ca, om, oa, limit, ask, payload = f.buyState(m)
	if cm != "TAKER_FALLBACK" || ca != 3 || om != "TAKER_FALLBACK" || oa != 3 {
		t.Errorf("after refresh2: cyc=%s/%d ord=%s/%d, want TAKER_FALLBACK/3", cm, ca, om, oa)
	}
	if !decimal.RequireFromString(limit).Equal(dec("300")) || !decimal.RequireFromString(ask).Equal(dec("300")) {
		t.Errorf("after refresh2: taker limit/ask = %s/%s, want 300/300 (at ask)", limit, ask)
	}
	// Payload reflects the escalated mode/price, and NO duplicate request was made.
	if !strings.Contains(payload, `"execution_mode":"TAKER_FALLBACK"`) || !strings.Contains(payload, `"intended_price":"300"`) {
		t.Errorf("payload not escalated: %s", payload)
	}
	if n := f.buyReqs(m); n != 1 {
		t.Errorf("buy requests after two refreshes = %d, want exactly 1 (no duplicate)", n)
	}
}

func TestRefreshWindowExpiryResetsToMaker(t *testing.T) {
	f := setupB(t)
	m := f.market("USDT", "0.5", "base", policy(true, 1, 10)) // taker after 1 maker attempt
	if _, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), f.sig(), 600); err != nil {
		t.Fatal(err)
	}
	// One refresh escalates to taker (threshold 1 -> attempt 2 is taker).
	if ok, err := RefreshActiveCycleBuy(f.ctx, f.store, m, dec("100"), f.sig()); err != nil || !ok {
		t.Fatal(err)
	}
	if cm, _, _, _, _, _, _ := f.buyState(m); cm != "TAKER_FALLBACK" {
		t.Fatalf("expected TAKER_FALLBACK before window reset, got %s", cm)
	}
	// Expire the window, then a refresh resets the decision to maker-first.
	var cid int64
	if err := f.db.QueryRow("SELECT cycle_id FROM symbol_locks WHERE state='ACTIVE' AND canonical_symbol=?", m.CanonicalSymbol).Scan(&cid); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Exec("UPDATE cycles SET opportunity_window_started_at = NOW(6) - INTERVAL 2 HOUR WHERE id=?", cid); err != nil {
		t.Fatal(err)
	}
	if ok, err := RefreshActiveCycleBuy(f.ctx, f.store, m, dec("100"), f.sig()); err != nil || !ok {
		t.Fatal(err)
	}
	cm, ca, _, _, _, _, _ := f.buyState(m)
	if cm != "MAKER_FIRST" || ca != 1 {
		t.Errorf("after window expiry: mode/attempt = %s/%d, want MAKER_FIRST/1 (reset)", cm, ca)
	}
}

func TestRefreshNoOpWhenClaimed(t *testing.T) {
	f := setupB(t)
	m := f.market("USDT", "0.5", "base", policy(true, 2, 10))
	r, err := CreateBuyCycle(f.ctx, f.store, f.q, m, dec("100"), f.sig(), 600)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate the executor having claimed the request.
	if _, err := f.db.Exec("UPDATE exchange_requests SET status='CLAIMED' WHERE id=?", r.RequestID); err != nil {
		t.Fatal(err)
	}
	ok, err := RefreshActiveCycleBuy(f.ctx, f.store, m, dec("200"), f.sig())
	if err != nil || ok {
		t.Fatalf("refresh of CLAIMED = %v, %v; want false (not touched)", ok, err)
	}
	var ask string
	f.db.QueryRow("SELECT ask_price_at_decision FROM orders WHERE id=?", r.OrderID).Scan(&ask)
	if !decimal.RequireFromString(ask).Equal(dec("100")) {
		t.Errorf("CLAIMED order's price changed to %s; must be untouched (100)", ask)
	}
}

func TestRefreshNoOpWhenNoActiveCycle(t *testing.T) {
	f := setupB(t)
	m := f.market("USDT", "0.5", "base", policy(true, 2, 10)) // nothing created
	if ok, err := RefreshActiveCycleBuy(f.ctx, f.store, m, dec("200"), f.sig()); ok || err != nil {
		t.Errorf("refresh with no active cycle = %v, %v; want false,nil", ok, err)
	}
}

func (f *bfix) count(table string, m configstore.MarketConfig) int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE exchange_market_id=?", m.ExchangeMarketID).Scan(&n)
	return n
}
func (f *bfix) buyReqs(m configstore.MarketConfig) int {
	var n int
	f.db.QueryRow(`SELECT COUNT(*) FROM exchange_requests er JOIN orders o ON o.id=er.order_id
		WHERE o.exchange_market_id=? AND er.request_type='PLACE_ORDER'`, m.ExchangeMarketID).Scan(&n)
	return n
}
