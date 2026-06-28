package regime

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/migrate"
)

// TestCalculatorUsesNoOrderClient is a structural guard: nothing the Calculator holds
// can place/cancel orders or read a trading queue — it only samples a PriceSource.
func TestCalculatorUsesNoOrderClient(t *testing.T) {
	type placer interface{ PlaceOrder(any) any }
	type canceller interface{ CancelOrder(any) any }
	pT := reflect.TypeOf((*placer)(nil)).Elem()
	cT := reflect.TypeOf((*canceller)(nil)).Elem()
	ct := reflect.TypeOf(Calculator{})
	for i := 0; i < ct.NumField(); i++ {
		ft := ct.Field(i).Type
		if ft.Implements(pT) || ft.Implements(cT) {
			t.Errorf("Calculator.%s can place/cancel — regime must not trade", ct.Field(i).Name)
		}
	}
}

// ---- gated ----

var rseq int

type rfix struct {
	t   *testing.T
	ctx context.Context
	db  *sql.DB
	st  *Store
}

func setupR(t *testing.T) *rfix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the regime integration test")
	}
	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := migrate.Run(ctx, db, migrate.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return &rfix{t: t, ctx: ctx, db: db, st: NewStore(db)}
}

// seedBasket inserts a basket + symbols + timeframes and returns its id. tfSeconds is
// a single timeframe's lookback (kept tiny for tests).
func (f *rfix) seedBasket(enabled int, tfSeconds int, symbols ...string) int64 {
	f.t.Helper()
	rseq++
	res, err := f.db.Exec(`INSERT INTO market_regime_baskets
		(name, enabled, update_interval_seconds, neutral_band_bps, moderate_threshold_bps, strong_threshold_bps, config_version)
		VALUES (?, ?, 1, 5, 30, 100, 7)`, fmt.Sprintf("basket_%d_%d", time.Now().UnixNano(), rseq), enabled)
	if err != nil {
		f.t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	for _, s := range symbols {
		if _, err := f.db.Exec("INSERT INTO market_regime_basket_symbols (basket_id, binance_symbol, weight, enabled) VALUES (?, ?, 1, 1)", id, s); err != nil {
			f.t.Fatal(err)
		}
	}
	if _, err := f.db.Exec("INSERT INTO market_regime_timeframes (basket_id, label, seconds, weight) VALUES (?, 'tf', ?, 1)", id, tfSeconds); err != nil {
		f.t.Fatal(err)
	}
	return id
}

func TestLoadBasketsConfig(t *testing.T) {
	f := setupR(t)
	id := f.seedBasket(1, 2, "BTC/USDT", "ETH/USDT")
	f.seedBasket(0, 2, "XRP/USDT") // disabled -> excluded

	baskets, err := f.st.LoadBaskets(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got *Basket
	for i := range baskets {
		if baskets[i].ID == id {
			got = &baskets[i]
		}
	}
	if got == nil {
		t.Fatal("enabled basket not loaded")
	}
	if len(got.Symbols) != 2 || len(got.Timeframes) != 1 || got.StrongBps != 100 || got.ConfigVersion != 7 {
		t.Errorf("loaded basket = %+v", got)
	}
	for _, b := range baskets {
		if b.ID != id { // any other loaded basket must be enabled
			var en int
			f.db.QueryRow("SELECT enabled FROM market_regime_baskets WHERE id=?", b.ID).Scan(&en)
			if en != 1 {
				t.Error("a disabled basket was loaded")
			}
		}
	}
}

func TestWriteResultUpsertAndHistoryOnChange(t *testing.T) {
	f := setupR(t)
	id := f.seedBasket(1, 2, "BTC/USDT")
	b := Basket{ID: id, ConfigVersion: 7}
	now := time.Now().UTC()

	r1 := Result{Direction: DirBullish, Level: LvlWeak, Confidence: dec("1"), ScoreBps: dec("20"),
		TimeframeScores: map[string]decimal.Decimal{"tf": dec("20")}, SymbolContributions: map[string]decimal.Decimal{"BTC/USDT": dec("20")}}
	if err := f.st.WriteResult(f.ctx, b, r1, now); err != nil {
		t.Fatal(err)
	}
	// Same regime again -> current upserted (still 1 row), NO new history.
	if err := f.st.WriteResult(f.ctx, b, r1, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if c := f.count("market_regime_current", id); c != 1 {
		t.Errorf("current rows = %d, want 1 (upsert)", c)
	}
	if h := f.count("market_regime_history", id); h != 1 {
		t.Errorf("history rows = %d, want 1 (deduped on unchanged)", h)
	}

	// Changed regime -> a new history row.
	r2 := r1
	r2.Direction, r2.Level = DirBearish, LvlModerate
	if err := f.st.WriteResult(f.ctx, b, r2, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if h := f.count("market_regime_history", id); h != 2 {
		t.Errorf("history rows = %d, want 2 (changed regime)", h)
	}
	// Current reflects the latest + stamps config version.
	var dir, lvl string
	var cv int64
	f.db.QueryRow("SELECT direction, level, config_version FROM market_regime_current WHERE basket_id=?", id).Scan(&dir, &lvl, &cv)
	if dir != DirBearish || lvl != LvlModerate || cv != 7 {
		t.Errorf("current = %s/%s cv=%d, want BEARISH/MODERATE/7", dir, lvl, cv)
	}
}

func TestHistoryCapturesFullEvolution(t *testing.T) {
	f := setupR(t)
	id := f.seedBasket(1, 2, "BTC/USDT")
	b := Basket{ID: id, ConfigVersion: 7}
	now := time.Now().UTC()
	mk := func(dir, lvl, conf, score string) Result {
		return Result{Direction: dir, Level: lvl, Confidence: dec(conf), ScoreBps: dec(score),
			TimeframeScores: map[string]decimal.Decimal{"tf": dec(score)}, SymbolContributions: map[string]decimal.Decimal{"BTC/USDT": dec(score)}}
	}
	// Same label, but confidence changes 0.35 -> 0.90 -> a NEW history row each time.
	if err := f.st.WriteResult(f.ctx, b, mk(DirBullish, LvlStrong, "0.35", "150"), now); err != nil {
		t.Fatal(err)
	}
	if err := f.st.WriteResult(f.ctx, b, mk(DirBullish, LvlStrong, "0.35", "150"), now); err != nil { // identical -> no dup
		t.Fatal(err)
	}
	if h := f.count("market_regime_history", id); h != 1 {
		t.Fatalf("identical regime history = %d, want 1", h)
	}
	if err := f.st.WriteResult(f.ctx, b, mk(DirBullish, LvlStrong, "0.90", "150"), now); err != nil { // confidence change
		t.Fatal(err)
	}
	if h := f.count("market_regime_history", id); h != 2 {
		t.Errorf("history after confidence change = %d, want 2 (full-field hash)", h)
	}

	// UNKNOWN whose stale_reason changes also records a new history row.
	u1 := Result{Direction: DirUnknown, Level: LvlUnknown, Confidence: dec("0"), StaleReason: "1/2 symbols stale"}
	u2 := Result{Direction: DirUnknown, Level: LvlUnknown, Confidence: dec("0"), StaleReason: "no fresh data"}
	if err := f.st.WriteResult(f.ctx, b, u1, now); err != nil {
		t.Fatal(err)
	}
	if err := f.st.WriteResult(f.ctx, b, u2, now); err != nil {
		t.Fatal(err)
	}
	if h := f.count("market_regime_history", id); h != 4 {
		t.Errorf("history after two distinct UNKNOWN reasons = %d, want 4", h)
	}
}

// fakeSource returns a settable price stamped at the manual clock's current time.
type fakeSource struct {
	clk   *clock.Manual
	price decimal.Decimal
	ok    bool
}

func (s *fakeSource) Price(context.Context, string) (decimal.Decimal, time.Time, bool) {
	return s.price, s.clk.Now(), s.ok
}

func TestCalculatorSamplesAndPersists(t *testing.T) {
	f := setupR(t)
	id := f.seedBasket(1, 2, "BTC/USDT") // timeframe = 2s
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManual(t0)
	src := &fakeSource{clk: clk, price: dec("100"), ok: true}
	c := NewCalculator(f.st, src, clk, nil, Config{SampleInterval: time.Second, MaxAge: 10 * time.Second})

	c.Pass(f.ctx) // t0: only one sample -> insufficient history -> UNKNOWN
	if dir := f.currentDir(id); dir != DirUnknown {
		t.Errorf("after first pass dir = %s, want UNKNOWN (no history)", dir)
	}

	clk.Advance(time.Second)
	src.price = dec("100.5") // +50 bps over the 2s window's reference
	c.Pass(f.ctx)            // t0+1s: now spans the timeframe -> a real regime
	if dir := f.currentDir(id); dir != DirBullish {
		t.Errorf("after rising price dir = %s, want BULLISH", dir)
	}
	if h := f.count("market_regime_history", id); h < 1 {
		t.Errorf("history rows = %d, want >=1", h)
	}
}

func TestCalculatorRedisMissDoesNotCrashOrFabricate(t *testing.T) {
	f := setupR(t)
	id := f.seedBasket(1, 2, "BTC/USDT")
	clk := clock.NewManual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	src := &fakeSource{clk: clk, ok: false} // price always missing
	c := NewCalculator(f.st, src, clk, nil, Config{SampleInterval: time.Second, MaxAge: 10 * time.Second})

	c.Pass(f.ctx)
	clk.Advance(time.Second)
	c.Pass(f.ctx)
	// No data ever -> regime stays UNKNOWN with a stale reason; no panic.
	if dir := f.currentDir(id); dir != DirUnknown {
		t.Errorf("missing redis data -> dir %s, want UNKNOWN", dir)
	}
	var reason sql.NullString
	f.db.QueryRow("SELECT stale_reason FROM market_regime_current WHERE basket_id=?", id).Scan(&reason)
	if !reason.Valid || reason.String == "" {
		t.Error("expected a stale reason when redis data is missing")
	}
}

func (f *rfix) count(table string, basketID int64) int {
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE basket_id=?", basketID).Scan(&n)
	return n
}
func (f *rfix) currentDir(basketID int64) string {
	var d string
	f.db.QueryRow("SELECT direction FROM market_regime_current WHERE basket_id=?", basketID).Scan(&d)
	return d
}
