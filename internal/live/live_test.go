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
	t      *testing.T
	ctx    context.Context
	db     *sql.DB
	g      *Guard
	ex     int64
	em     int64
	symbol string // the fixture market's canonical symbol (checks must match it exactly)
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

// seedMarket creates an exchange + market + exchange_market. The canonical symbol is
// computed ONCE and reused for both rows (the guard now proves the request symbol against
// exchange_markets.canonical_symbol, so they must agree) and recorded on the fixture.
func (f *lfix) seedMarket(exLive, mkLive int) (int64, int64) {
	lseq++
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), lseq) }
	sym := u("M") + "/IRT"
	ex := last(f.exec("INSERT INTO exchanges (code, name, enabled, live_enabled) VALUES (?, 'L', 1, ?)", u("lx"), exLive))
	b := last(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B")))
	qa := last(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q")))
	m := last(f.exec("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", sym, b, qa))
	em := last(f.exec("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol, live_enabled) VALUES (?, ?, ?, ?, ?)", ex, m, u("ES"), sym, mkLive))
	f.symbol = sym
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
	// require_canary_ack=0 here: these tests exercise the caps/kill-switch/credential guards;
	// the PR23 canary-ack gate is covered by its own tests.
	f.exec(`INSERT INTO live_controls (id, kill_switch, max_open_cycles, max_daily_orders, max_daily_quote, max_order_notional, max_base_qty, max_consecutive_failures, max_unresolved_reconcile, require_canary_ack)
		VALUES (1, 0, 1000000000, 1000000000, '1000000000000000', '1000', '10', 1000000000, 1000000000, 0)`)
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

// buy seeds a REAL live (dry_run=0) cycle + QUEUED entry_buy order on the fixture's market
// and returns the PlaceCheck that EXACTLY matches it. PR20 corrections #2/#5: the guard
// proves an entry buy's identity/ownership/market AND that the payload equals the persisted
// order, so a buy check must reference real rows and carry matching values. The limit price
// is derived so that price x quantity == the requested notional.
func (f *lfix) buy(notional, baseQty string) PlaceCheck {
	lseq++
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), lseq) }
	coid := u("eb")
	price := dec(notional).Div(dec(baseQty))
	cyc := last(f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run) VALUES (?, ?, ?, 'BUY_REQUEST_QUEUED', 0)",
		f.em, f.ex, f.symbol))
	ord := last(f.exec("INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, order_type, quantity, limit_price) VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'QUEUED', 'limit', ?, ?)",
		cyc, f.ex, f.em, coid, baseQty, price.String()))
	return PlaceCheck{ExchangeID: f.ex, ExchangeMarketID: f.em, Symbol: f.symbol,
		CycleID: cyc, OrderID: ord, Side: "buy", Notional: dec(notional), BaseQty: dec(baseQty),
		LimitPrice: price, OrderType: "limit", LocalClientOrderID: coid}
}

// cancelOf seeds a cancellable ACKED order with a known venue id and returns the matching
// cancel PlaceCheck (PR20 correction #3: the guard proves the cancel target from the DB).
func (f *lfix) cancelOf() PlaceCheck {
	lseq++
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), lseq) }
	ext := u("EXT")
	cyc := last(f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run) VALUES (?, ?, ?, 'BUY_SUBMITTED', 0)",
		f.em, f.ex, f.symbol))
	ord := last(f.exec("INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, exchange_order_id, state, quantity, limit_price) VALUES (?, ?, ?, 'buy', 'entry_buy', ?, ?, 'ACKED', '1', '100')",
		cyc, f.ex, f.em, u("cb"), ext))
	return PlaceCheck{ExchangeID: f.ex, CycleID: cyc, OrderID: ord, ExchangeOrderID: ext}
}

// seedProvenExit creates a REAL (dry_run=0) cycle with a FILLED entry buy of `bought` and a
// QUEUED exit_sell of `sellQty`, returning the PlaceCheck for that sell. This is the DB
// proof checkExitSell demands (PR20 correction: a sell is only risk-reducing when the
// database shows acquired inventory it closes).
func (f *lfix) seedProvenExit(bought, sellQty string) PlaceCheck {
	lseq++
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), lseq) }
	cyc := last(f.exec("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run) VALUES (?, ?, ?, 'BUY_FILLED', 0)", f.em, f.ex, f.symbol))
	f.exec("INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, order_type, quantity, limit_price, filled_quantity) VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'FILLED', 'limit', ?, '100', ?)",
		cyc, f.ex, f.em, u("pb"), bought, bought)
	coid := u("ps")
	ord := last(f.exec("INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, order_type, quantity, limit_price) VALUES (?, ?, ?, 'sell', 'exit_sell', ?, 'QUEUED', 'limit', ?, '100')",
		cyc, f.ex, f.em, coid, sellQty))
	return PlaceCheck{ExchangeID: f.ex, ExchangeMarketID: f.em, Symbol: f.symbol, CycleID: cyc, OrderID: ord,
		Side: "sell", Notional: dec(sellQty).Mul(dec("100")), BaseQty: dec(sellQty),
		LimitPrice: dec("100"), OrderType: "limit", LocalClientOrderID: coid}
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
	// A PROVEN exit sell (DB shows filled inventory it closes) is allowed under the kill switch.
	sell := f.seedProvenExit("0.5", "0.5")
	if d := f.g.CheckPlace(f.ctx, sell); !d.Allow {
		t.Errorf("sell should be allowed under kill switch (risk-reducing exit): %s", d.Reason)
	}
	// A cancel of a PROVEN target is allowed under the kill switch.
	if d := f.g.CheckCancel(f.ctx, f.cancelOf()); !d.Allow {
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
	cur, _ := f.g.openCycles(f.ctx)
	f.set("max_open_cycles", cur+1)
	f.seedLiveOpenCycle() // now cur+1 open
	if d := f.g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("max open cycles must block a new buy cycle")
	}
}

// TestDailyLimitsNoLongerBlockTrading (PR20 correction #1 — owner decision): the historical
// max_daily_orders / max_daily_quote columns must NEVER influence a trading decision. Even
// with the tightest possible values stored (0), a buy passes every remaining gate.
func TestDailyLimitsNoLongerBlockTrading(t *testing.T) {
	f := setupL(t)
	f.seedDailyOrder() // real same-day order exists — would have tripped the old cap
	f.set("max_daily_orders", 0)
	f.set("max_daily_quote", 0)
	if d := f.g.CheckPlace(f.ctx, f.buy("50", "0.5")); !d.Allow {
		t.Errorf("daily caps must not block trading anymore, got deny: %s", d.Reason)
	}
}

// --- PR20 corrections: fail-closed safety queries, exits, durable audit ----------------------

// TestSafetyQueryErrorsDeny (PR20 #8): ANY database error while evaluating a live safety
// condition denies the operation. A closed DB makes every safety query fail; both the place
// and cancel gates must deny, and each query helper must surface its error explicitly.
func TestSafetyQueryErrorsDeny(t *testing.T) {
	f := setupL(t)
	broken, err := sql.Open("mysql", os.Getenv("V3_TEST_MYSQL_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	broken.Close() // every query now errors
	g := NewGuard(broken, clock.NewSystem(), nil)

	if d := g.CheckPlace(f.ctx, f.buy("50", "0.5")); d.Allow {
		t.Error("CheckPlace must deny when safety queries error")
	}
	if d := g.CheckCancel(f.ctx, PlaceCheck{ExchangeID: f.ex}); d.Allow {
		t.Error("CheckCancel must deny when the credential query errors")
	}
	if d := g.AllowNewBuyCycle(f.ctx, f.ex, f.em); d.Allow {
		t.Error("AllowNewBuyCycle must deny when safety queries error")
	}
	// Every remaining safety query returns its error explicitly (no silent zero).
	if _, err := g.openCycles(f.ctx); err == nil {
		t.Error("openCycles must return the query error")
	}
	if _, err := g.unresolvedReconcile(f.ctx); err == nil {
		t.Error("unresolvedReconcile must return the query error")
	}
	if _, err := g.consecutiveFailures(f.ctx, f.ex); err == nil {
		t.Error("consecutiveFailures must return the query error")
	}
	if _, err := g.credentialsAvailable(f.ctx, f.ex); err == nil {
		t.Error("credentialsAvailable must return the query error")
	}
	if _, err := g.live(f.ctx, f.ex, f.em); err == nil {
		t.Error("live must return the query error")
	}
}

// TestExitSellInventoryProof (PR20 #8): the exit path enforces DB-proven risk reduction —
// oversell, duplicate active sells, missing inventory, wrong ownership, and payload/DB
// quantity mismatches are all denied; a within-inventory exit passes even with the kill
// switch engaged and entry caps exhausted.
func TestExitSellInventoryProof(t *testing.T) {
	f := setupL(t)
	// Entry limits fully hostile: kill switch on, zero open-cycle headroom.
	f.set("kill_switch", 1)
	f.set("max_open_cycles", 1)

	// Oversell: bought 0.5, trying to sell 0.7.
	if d := f.g.CheckPlace(f.ctx, f.seedProvenExit("0.5", "0.7")); d.Allow {
		t.Error("oversell must be denied")
	}
	// No filled inventory at all.
	if d := f.g.CheckPlace(f.ctx, f.seedProvenExit("0", "0.5")); d.Allow {
		t.Error("exit without filled inventory must be denied")
	}
	// Within inventory → allowed despite kill switch + exhausted entry caps.
	ok := f.seedProvenExit("0.5", "0.5")
	if d := f.g.CheckPlace(f.ctx, ok); !d.Allow {
		t.Errorf("proven exit must pass under kill switch/entry limits: %s", d.Reason)
	}
	// Duplicate: a second ACTIVE sell for the same cycle overshoots the inventory.
	lseq++
	f.exec("INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, quantity, limit_price) VALUES (?, ?, ?, 'sell', 'exit_sell', ?, 'ACKED', '0.5', '100')",
		ok.CycleID, f.ex, f.em, fmt.Sprintf("dup%d_%d", time.Now().UnixNano(), lseq))
	if d := f.g.CheckPlace(f.ctx, ok); d.Allow {
		t.Error("a duplicate active sell consuming the inventory must deny the second one")
	}
	// Payload quantity disagreeing with the registered order.
	bad := f.seedProvenExit("0.5", "0.5")
	bad.BaseQty = dec("0.4")
	if d := f.g.CheckPlace(f.ctx, bad); d.Allow {
		t.Error("payload/DB quantity mismatch must be denied")
	}
	// Ownership: order from another cycle.
	other := f.seedProvenExit("0.5", "0.5")
	stolen := f.seedProvenExit("0.5", "0.5")
	stolen.OrderID = other.OrderID // sell order belongs to a DIFFERENT cycle
	if d := f.g.CheckPlace(f.ctx, stolen); d.Allow {
		t.Error("order/cycle ownership mismatch must be denied")
	}
}

// TestAuditOutagePolicy (PR20 #8): an allowed real PLACE without a persistable audit row is
// DENIED (fail closed) — but a risk-reducing CANCEL still proceeds (a missing audit row is
// less dangerous than the inability to cancel), with the decision logged instead.
func TestAuditOutagePolicy(t *testing.T) {
	f := setupL(t)
	f.exec("RENAME TABLE live_audit TO live_audit_outage_test")
	restored := false
	restore := func() {
		if !restored {
			f.exec("RENAME TABLE live_audit_outage_test TO live_audit")
			restored = true
		}
	}
	defer restore()

	if d := f.g.CheckPlace(f.ctx, f.buy("50", "0.5")); d.Allow {
		t.Error("an allowed place without a durable audit must flip to DENY (fail closed)")
	}
	if d := f.g.CheckCancel(f.ctx, f.cancelOf()); !d.Allow {
		t.Errorf("a cancel must NOT be blocked by an audit outage: %s", d.Reason)
	}
	restore()
	// With the audit table back, the same place is allowed and audited.
	d := f.g.CheckPlace(f.ctx, f.buy("50", "0.5"))
	if !d.Allow {
		t.Fatalf("place after audit restore should pass: %s", d.Reason)
	}
	var n int
	f.db.QueryRow("SELECT COUNT(*) FROM live_audit WHERE decision='allow' AND action='place_buy'").Scan(&n)
	if n == 0 {
		t.Error("restored place must be durably audited")
	}
}
