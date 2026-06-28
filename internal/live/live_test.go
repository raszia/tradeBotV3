package live

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
	"v3TradeBot/internal/migrate"
)

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

var lseq int

type lfix struct {
	t   *testing.T
	ctx context.Context
	db  *sql.DB
	g   *Guard
	ex  int64
	em  int64
}

func setupL(t *testing.T) *lfix {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the live guard integration test")
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
	f := &lfix{t: t, ctx: ctx, db: db, g: NewGuard(db, clock.NewSystem(), nil)}
	f.ex, f.em = f.seedMarket(1, 1)
	f.seedCredential(1, "active")
	f.configure()
	return f
}

func (f *lfix) exec(q string, a ...any) sql.Result {
	r, err := f.db.Exec(q, a...)
	if err != nil {
		f.t.Fatalf("exec %q: %v", q, err)
	}
	return r
}

func (f *lfix) seedMarket(exLive, mkLive int) (int64, int64) {
	lseq++
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), lseq) }
	ex := last(f.exec("INSERT INTO exchanges (code, name, enabled, live_enabled) VALUES (?, 'L', 1, ?)", u("lx"), exLive))
	b := last(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B")))
	qa := last(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q")))
	m := last(f.exec("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", u("M")+"/IRT", b, qa))
	em := last(f.exec("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol, live_enabled) VALUES (?, ?, ?, ?, ?)", ex, m, u("ES"), u("M")+"/IRT", mkLive))
	return ex, em
}

func (f *lfix) seedCredential(enabled int, status string) {
	f.exec("DELETE FROM exchange_credentials WHERE exchange_id=?", f.ex)
	f.exec("INSERT INTO exchange_credentials (exchange_id, label, enabled, status) VALUES (?, 'default', ?, ?)", f.ex, enabled, status)
}

// configure writes a valid baseline (kill switch OFF). Count-based caps are HUGE so
// the shared-DB global counts (other packages' non-dry-run cycles/orders) never trip
// the baseline; per-request caps (notional/qty) stay tight to test those deterministically.
func (f *lfix) configure() {
	f.exec("DELETE FROM live_controls")
	f.exec(`INSERT INTO live_controls (id, kill_switch, max_open_cycles, max_daily_orders, max_daily_quote, max_order_notional, max_base_qty, max_consecutive_failures, max_unresolved_reconcile)
		VALUES (1, 0, 1000000000, 1000000000, '1000000000000000', '1000', '10', 1000000000, 1000000000)`)
}

// seedDailyOrder inserts a non-dry-run cycle + entry_buy order created today.
func (f *lfix) seedDailyOrder() {
	lseq++
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	uid := fmt.Sprintf("dol%d_%d", time.Now().UnixNano(), lseq)
	cyc := last(f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run) VALUES (?, ?, ?, 'BUY_SUBMITTED', 0)", f.em, f.ex, fmt.Sprintf("DO%d", lseq)))
	f.exec("INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, quantity, limit_price) VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'SUBMITTED', '1', '10')", cyc, f.ex, f.em, uid)
}

func (f *lfix) set(col string, val any) { f.exec("UPDATE live_controls SET "+col+"=? WHERE id=1", val) }

func (f *lfix) buy(notional, baseQty string) PlaceCheck {
	return PlaceCheck{ExchangeID: f.ex, ExchangeMarketID: f.em, Side: "buy", Notional: dec(notional), BaseQty: dec(baseQty)}
}

func (f *lfix) seedLiveOpenCycle() {
	lseq++
	f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run) VALUES (?, ?, ?, 'BUY_SUBMITTED', 0)", f.em, f.ex, fmt.Sprintf("OC%d", lseq))
}

// ---- tests ----

func TestAllowedBaselineAndAudit(t *testing.T) {
	f := setupL(t)
	d := f.g.CheckPlace(f.ctx, f.buy("50", "0.5"))
	if !d.Allow {
		t.Fatalf("baseline buy should be allowed, got deny: %s", d.Reason)
	}
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM live_audit WHERE decision='allow' AND action='place_buy'").Scan(&n)
	if n == 0 {
		t.Error("an allow decision must be audited")
	}
}

func TestDeniesMatrix(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(f *lfix)
		check  func(f *lfix) Decision
	}{
		{"dry-run never sent live", nil, func(f *lfix) Decision {
			p := f.buy("50", "0.5")
			p.DryRun = true
			return f.g.CheckPlace(f.ctx, p)
		}},
		{"kill switch blocks buy", func(f *lfix) { f.set("kill_switch", 1) }, func(f *lfix) Decision { return f.g.CheckPlace(f.ctx, f.buy("50", "0.5")) }},
		{"not configured (missing cap)", func(f *lfix) { f.set("max_order_notional", nil) }, func(f *lfix) Decision { return f.g.CheckPlace(f.ctx, f.buy("50", "0.5")) }},
		{"exchange not live-enabled", func(f *lfix) { f.exec("UPDATE exchanges SET live_enabled=0 WHERE id=?", f.ex) }, func(f *lfix) Decision { return f.g.CheckPlace(f.ctx, f.buy("50", "0.5")) }},
		{"symbol not live-enabled", func(f *lfix) { f.exec("UPDATE exchange_markets SET live_enabled=0 WHERE id=?", f.em) }, func(f *lfix) Decision { return f.g.CheckPlace(f.ctx, f.buy("50", "0.5")) }},
		{"no active credentials", func(f *lfix) { f.seedCredential(0, "disabled") }, func(f *lfix) Decision { return f.g.CheckPlace(f.ctx, f.buy("50", "0.5")) }},
		{"oversized notional", nil, func(f *lfix) Decision { return f.g.CheckPlace(f.ctx, f.buy("2000", "0.5")) }},
		{"oversized base qty", nil, func(f *lfix) Decision { return f.g.CheckPlace(f.ctx, f.buy("50", "20")) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := setupL(t)
			if c.mutate != nil {
				c.mutate(f)
			}
			if d := c.check(f); d.Allow {
				t.Errorf("%s should be DENIED, got allow", c.name)
			}
		})
	}
}

func TestKillSwitchAllowsSellAndCancel(t *testing.T) {
	f := setupL(t)
	f.set("kill_switch", 1)
	// A SELL place (exiting inventory) is allowed under the kill switch.
	sell := PlaceCheck{ExchangeID: f.ex, ExchangeMarketID: f.em, Side: "sell", Notional: dec("50"), BaseQty: dec("0.5")}
	if d := f.g.CheckPlace(f.ctx, sell); !d.Allow {
		t.Errorf("sell should be allowed under kill switch (risk-reducing exit): %s", d.Reason)
	}
	// A cancel is allowed under the kill switch.
	if d := f.g.CheckCancel(f.ctx, PlaceCheck{ExchangeID: f.ex}); !d.Allow {
		t.Errorf("cancel should be allowed under kill switch: %s", d.Reason)
	}
}

func TestCancelWithoutCredentialsDenied(t *testing.T) {
	f := setupL(t)
	f.seedCredential(0, "disabled")
	if d := f.g.CheckCancel(f.ctx, PlaceCheck{ExchangeID: f.ex}); d.Allow {
		t.Error("cancel without active credentials must be denied")
	}
}

func TestAllowNewBuyCycleCaps(t *testing.T) {
	f := setupL(t)
	// Baseline allows a new cycle (huge caps).
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); !d.Allow {
		t.Fatalf("baseline new cycle should be allowed: %s", d.Reason)
	}
	// Kill switch blocks a new cycle.
	f.set("kill_switch", 1)
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("kill switch must block a new buy cycle")
	}
	f.set("kill_switch", 0)
	// Open-cycle cap: set the cap to the CURRENT live-open-cycle count (pollution-proof),
	// then one more open cycle => count >= cap => deny.
	cur := f.g.openCycles(f.ctx)
	f.set("max_open_cycles", cur+1)
	f.seedLiveOpenCycle() // now cur+1 open
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("max open cycles must block a new buy cycle")
	}
}

// TestDailyOrderCapEnforced sets the cap to the current daily-order count after seeding
// one non-dry-run order, so the count >= cap regardless of shared-DB pollution.
func TestDailyOrderCapEnforced(t *testing.T) {
	f := setupL(t)
	f.seedDailyOrder()
	cur := f.g.dailyOrders(f.ctx) // >= 1
	f.set("max_daily_orders", cur)
	if d := f.g.CheckPlace(f.ctx, f.buy("50", "0.5")); d.Allow {
		t.Errorf("daily order cap (=%d) must block, got allow", cur)
	}
}
