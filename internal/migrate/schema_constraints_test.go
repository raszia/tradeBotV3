package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// TestPR2SchemaConstraintsExist (PR2 correction) verifies the added foreign keys, operational
// indexes, and financial CHECK constraints are present after migration.
func TestPR2SchemaConstraintsExist(t *testing.T) {
	db, ctx := predeployDB(t)
	if _, err := Run(ctx, db, FS); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, fk := range []string{"fk_signals_exchange", "fk_signals_config"} {
		if !constraintExists(t, db, "signals", fk, "FOREIGN KEY") {
			t.Errorf("missing FOREIGN KEY %s on signals", fk)
		}
	}
	for _, ix := range []struct{ tbl, name string }{
		{"comparison_events", "idx_cmp_exchange_symbol"},
		{"signals", "idx_signals_exchange_symbol"},
	} {
		if !indexExists(t, db, ix.tbl, ix.name) {
			t.Errorf("missing index %s on %s", ix.name, ix.tbl)
		}
	}
	for _, c := range []struct{ tbl, name string }{
		{"orders", "chk_orders_qty_nonneg"}, {"orders", "chk_orders_filled_nonneg"},
		{"orders", "chk_orders_filled_le_qty"}, {"orders", "chk_orders_price_nonneg"},
		{"orders", "chk_orders_fee_nonneg"},
		{"fills", "chk_fills_qty_nonneg"}, {"fills", "chk_fills_price_nonneg"}, {"fills", "chk_fills_fee_nonneg"},
		{"exchange_markets", "chk_em_tick_nonneg"}, {"exchange_markets", "chk_em_step_nonneg"},
		{"exchange_markets", "chk_em_minqty_nonneg"}, {"exchange_markets", "chk_em_minamt_nonneg"},
		{"wallet_balances_current", "chk_balcur_avail_nonneg"}, {"wallet_balance_history", "chk_balhist_total_nonneg"},
	} {
		if !constraintExists(t, db, c.tbl, c.name, "CHECK") {
			t.Errorf("missing CHECK %s on %s", c.name, c.tbl)
		}
	}
}

// TestPR2SchemaConstraintsEnforced (PR2 correction) verifies the FKs + CHECKs actually reject
// bad writes (and allow valid / NULL ones).
func TestPR2SchemaConstraintsEnforced(t *testing.T) {
	db, ctx := predeployDB(t)
	if _, err := Run(ctx, db, FS); err != nil {
		t.Fatalf("Run: %v", err)
	}
	n := 0
	uniq := func(p string) string { n++; return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), n) }
	bad := func(q string, a ...any) {
		if _, err := db.ExecContext(ctx, q, a...); err == nil {
			t.Errorf("constraint should REJECT: %s", q)
		}
	}
	ok := func(q string, a ...any) {
		if _, err := db.ExecContext(ctx, q, a...); err != nil {
			t.Errorf("should ACCEPT, got %v for: %s", err, q)
		}
	}

	// FK: signals.exchange_id / config_version must reference real rows; NULL is allowed.
	bad("INSERT INTO signals (exchange_id, canonical_symbol) VALUES (88888888, 'X/Y')")
	bad("INSERT INTO signals (canonical_symbol, config_version) VALUES ('X/Y', 88888888)")
	ok("INSERT INTO signals (canonical_symbol) VALUES ('X/Y')") // NULL exchange/config

	// Seed a real exchange (FK parent for balances / orders).
	r, err := db.ExecContext(ctx, "INSERT INTO exchanges (code, name, enabled) VALUES (?, 'c', 1)", uniq("cex"))
	if err != nil {
		t.Fatal(err)
	}
	exID, _ := r.LastInsertId()

	// CHECK: wallet balance must be non-negative.
	bad("INSERT INTO wallet_balances_current (exchange_id, asset, available, locked, total) VALUES (?, 'BTC', '-1', '0', '0')", exID)
	ok("INSERT INTO wallet_balances_current (exchange_id, asset, available, locked, total) VALUES (?, 'BTC', '5', '0', '5')", exID)

	// CHECK: exchange_markets tick/step may be 0 (no-snap sentinel) but not negative.
	bad("INSERT INTO exchange_markets (exchange_id, exchange_symbol, canonical_symbol, tick_size) VALUES (?, ?, ?, '-0.1')", exID, uniq("es"), uniq("M")+"/Z")
	ok("INSERT INTO exchange_markets (exchange_id, exchange_symbol, canonical_symbol, tick_size, step_size) VALUES (?, ?, ?, '0', '0')", exID, uniq("es"), uniq("M")+"/Z")

	// Seed exchange_market + cycle for the order CHECKs.
	cyc, em := seedConstraintChain(t, db, ctx, exID, uniq)
	ordCols := "(cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, quantity"
	bad("INSERT INTO orders "+ordCols+") VALUES (?, ?, ?, 'buy','entry_buy', ?, '-1')", cyc, exID, em, uniq("o"))                                           // negative qty
	bad("INSERT INTO orders "+ordCols+", filled_quantity) VALUES (?, ?, ?, 'buy','entry_buy', ?, '1', '2')", cyc, exID, em, uniq("o"))                      // filled > qty
	bad("INSERT INTO orders "+ordCols+", limit_price) VALUES (?, ?, ?, 'buy','entry_buy', ?, '1', '-5')", cyc, exID, em, uniq("o"))                         // negative price
	ok("INSERT INTO orders "+ordCols+", filled_quantity, limit_price) VALUES (?, ?, ?, 'buy','entry_buy', ?, '1', '0.5', '100')", cyc, exID, em, uniq("o")) // valid
}

// seedConstraintChain inserts the minimal exchange_market + cycle parents an order needs
// (market_id is nullable, so no assets/markets row is required).
func seedConstraintChain(t *testing.T, db *sql.DB, ctx context.Context, exID int64, uniq func(string) string) (cycleID, emID int64) {
	t.Helper()
	last := func(q string, a ...any) int64 {
		res, err := db.ExecContext(ctx, q, a...)
		if err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
		id, _ := res.LastInsertId()
		return id
	}
	emID = last("INSERT INTO exchange_markets (exchange_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?)",
		exID, uniq("es"), uniq("M")+"/Y")
	cycleID = last("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol) VALUES (?, ?, 'X/Y')",
		emID, exID)
	return cycleID, emID
}
