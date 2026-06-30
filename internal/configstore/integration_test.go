package configstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	"v3TradeBot/internal/migrate"
)

// TestConfigStoreIntegration exercises the full config load + version + audit path
// against a real MariaDB. Skipped unless V3_TEST_MYSQL_DSN points at a throwaway
// database. Seeded rows are left in place (intended for an ephemeral test DB).
// gatedConfigDB opens the throwaway MariaDB (skips when V3_TEST_MYSQL_DSN is unset),
// migrates it, and returns a ready db + context. Shared by the gated configstore tests.
func gatedConfigDB(t *testing.T) (*sql.DB, context.Context) {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the configstore integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := migrate.Run(ctx, db, migrate.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db, ctx
}

func TestConfigStoreIntegration(t *testing.T) {
	db, ctx := gatedConfigDB(t)

	exec := func(q string, args ...any) sql.Result {
		res, err := db.ExecContext(ctx, q, args...)
		if err != nil {
			t.Fatalf("seed exec %q: %v", q, err)
		}
		return res
	}
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }

	// Seed: exchange + market + symbol_config + exchange_config + retention. Every
	// uniquely-constrained value carries a per-run suffix so the test is REPEAT-SAFE
	// against a reused test DB (no collisions on exchanges.code / assets.symbol /
	// markets.canonical_symbol / exchange_markets / retention_settings.table_name).
	sfx := fmt.Sprintf("%d", time.Now().UnixNano())
	exCode := "cfgtest_" + sfx
	baseSym := "CFGB_" + sfx
	quoteSym := "CFGQ_" + sfx
	canonical := baseSym + "/" + quoteSym
	exSym := "CFGBCFGQ_" + sfx
	exID := last(exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'Cfg Test', 1)", exCode))
	baseID := last(exec("INSERT INTO assets (symbol, kind) VALUES (?,'crypto')", baseSym))
	quoteID := last(exec("INSERT INTO assets (symbol, kind) VALUES (?,'crypto')", quoteSym))
	mktID := last(exec("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", canonical, baseID, quoteID))
	emID := last(exec(`INSERT INTO exchange_markets
		(exchange_id, market_id, exchange_symbol, canonical_symbol, enabled_for_collection, enabled_for_signal, enabled_for_trading, enabled_for_sell_manage)
		VALUES (?, ?, ?, ?, 1, 1, 1, 1)`, exID, mktID, exSym, canonical))
	exec("INSERT INTO symbol_configs (exchange_market_id, min_spread_bps, buy_size, buy_size_unit, sell_offset_bps, reprice_interval_seconds, order_timeout_ms, max_retries, retry_backoff_ms) VALUES (?, 40, '0.5', 'quote', 20, 5, 3000, 3, 500)", emID)
	exec("INSERT INTO exchange_configs (exchange_id, max_concurrent_requests, request_timeout_ms) VALUES (?, 2, 5000)", exID)
	exec("INSERT INTO retention_settings (table_name, retention_days, enabled) VALUES (?, 7, 1)", "api_call_logs_"+exCode)

	s := New(db)

	// No active version yet -> snapshot loads with Version 0 (rule: no active config).
	snap, err := s.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	mc, ok := snap.Market(emID)
	if !ok || !mc.HasSymbolConfig || mc.MinSpreadBps != 40 || mc.ExchangeCode != exCode {
		t.Fatalf("loaded market = %+v ok=%v", mc, ok)
	}
	if !mc.EnabledForTrading {
		t.Error("market should be enabled_for_trading")
	}

	// Activate a version, then a versioned+audited write.
	if _, err := s.ActivateVersion(ctx, "admin", "initial"); err != nil {
		t.Fatalf("ActivateVersion: %v", err)
	}
	newVer, err := s.UpdateMinSpreadBps(ctx, emID, 60, "admin", "widen spread")
	if err != nil {
		t.Fatalf("UpdateMinSpreadBps: %v", err)
	}

	// Reload: the active version is the new one and min_spread is updated.
	snap2, err := s.LoadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap2.Version != newVer {
		t.Errorf("active version = %d, want %d", snap2.Version, newVer)
	}
	if mc2, _ := snap2.Market(emID); mc2.MinSpreadBps != 60 || mc2.SymbolConfigVersion != newVer {
		t.Errorf("updated market = %+v", mc2)
	}

	// The audit row records old/new without secrets.
	var oldV, newV, changedBy string
	err = db.QueryRowContext(ctx,
		"SELECT old_value, new_value, changed_by FROM config_change_audit WHERE entity_type='symbol_config' AND entity_id=? ORDER BY id DESC LIMIT 1", emID).
		Scan(&oldV, &newV, &changedBy)
	if err != nil {
		t.Fatalf("audit row: %v", err)
	}
	if oldV != "40" || newV != "60" || changedBy != "admin" {
		t.Errorf("audit = old=%s new=%s by=%s", oldV, newV, changedBy)
	}

	// Validation passes for the seeded market.
	if issues := ValidateSnapshot(snap2); len(issues) != 0 {
		t.Errorf("seeded config should validate: %v", issues)
	}
}

// TestLoadFeesScopesDefaultsByExchange (PR6 correction) seeds TWO exchanges each with a
// DIFFERENT default fee plus a market-specific override on one, then loads the snapshot and
// proves both defaults survive (no key-0 collision), FeeFor returns the right exchange's
// default, and a market override wins. Repeat-safe via per-run suffixes.
func TestLoadFeesScopesDefaultsByExchange(t *testing.T) {
	db, ctx := gatedConfigDB(t)
	sfx := fmt.Sprintf("%d", time.Now().UnixNano())
	exec := func(q string, a ...any) sql.Result {
		r, err := db.ExecContext(ctx, q, a...)
		if err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
		return r
	}
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	dec := decimal.RequireFromString

	exA := last(exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'Fee A', 1)", "feeA_"+sfx))
	exB := last(exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'Fee B', 1)", "feeB_"+sfx))
	// Two DIFFERENT exchange-wide DEFAULT fees (exchange_market_id NULL).
	exec("INSERT INTO exchange_fees (exchange_id, exchange_market_id, maker_fee, taker_fee) VALUES (?, NULL, '0.001', '0.002')", exA)
	exec("INSERT INTO exchange_fees (exchange_id, exchange_market_id, maker_fee, taker_fee) VALUES (?, NULL, '0.003', '0.004')", exB)
	// A market-specific override on exchange A.
	baseID := last(exec("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", "FEEB_"+sfx))
	quoteID := last(exec("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", "FEEQ_"+sfx))
	canonical := "FEEB_" + sfx + "/FEEQ_" + sfx
	mktID := last(exec("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", canonical, baseID, quoteID))
	emA := last(exec("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, ?)", exA, mktID, "FEEBFEEQ_"+sfx, canonical))
	exec("INSERT INTO exchange_fees (exchange_id, exchange_market_id, maker_fee, taker_fee) VALUES (?, ?, '0.005', '0.006')", exA, emA)

	snap, err := New(db).LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	// Both exchange defaults survive and are DISTINCT (neither overwrote the other).
	da, okA := snap.DefaultFeesByExchangeID[exA]
	dbf, okB := snap.DefaultFeesByExchangeID[exB]
	if !okA || !da.MakerFee.Equal(dec("0.001")) {
		t.Fatalf("exA default = %v ok=%v, want maker 0.001", da.MakerFee, okA)
	}
	if !okB || !dbf.MakerFee.Equal(dec("0.003")) {
		t.Fatalf("exB default = %v ok=%v, want maker 0.003 (NOT overwritten by exA)", dbf.MakerFee, okB)
	}

	// FeeFor returns the default for the CORRECT exchange.
	if f, ok := snap.FeeFor(exB, 0); !ok || !f.MakerFee.Equal(dec("0.003")) {
		t.Errorf("FeeFor(exB, default) = %v ok=%v, want 0.003", f.MakerFee, ok)
	}
	// Market-specific override wins over the exchange default.
	if f, ok := snap.FeeFor(exA, emA); !ok || !f.MakerFee.Equal(dec("0.005")) {
		t.Errorf("FeeFor(exA, override) = %v ok=%v, want override 0.005", f.MakerFee, ok)
	}
}

// TestUpdateMinSpreadBpsRejectsNegativeBeforeCommit (PR6 correction) — a negative
// min_spread_bps must be rejected BEFORE any DB mutation: no new config version, no
// symbol_config change, no audit row. A subsequent valid update still works.
func TestUpdateMinSpreadBpsRejectsNegativeBeforeCommit(t *testing.T) {
	db, ctx := gatedConfigDB(t)
	sfx := fmt.Sprintf("%d", time.Now().UnixNano())
	exec := func(q string, a ...any) sql.Result {
		r, err := db.ExecContext(ctx, q, a...)
		if err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
		return r
	}
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	count := func(q string, a ...any) int {
		var n int
		if err := db.QueryRowContext(ctx, q, a...).Scan(&n); err != nil {
			t.Fatalf("count %q: %v", q, err)
		}
		return n
	}

	exID := last(exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'Spread', 1)", "spread_"+sfx))
	baseID := last(exec("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", "SPB_"+sfx))
	quoteID := last(exec("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", "SPQ_"+sfx))
	canonical := "SPB_" + sfx + "/SPQ_" + sfx
	mktID := last(exec("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", canonical, baseID, quoteID))
	emID := last(exec("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, ?)", exID, mktID, "SPBSPQ_"+sfx, canonical))
	exec("INSERT INTO symbol_configs (exchange_market_id, min_spread_bps, buy_size, buy_size_unit) VALUES (?, 40, '0.5', 'quote')", emID)

	s := New(db)
	versionsBefore := count("SELECT COUNT(*) FROM config_versions")
	auditBefore := count("SELECT COUNT(*) FROM config_change_audit WHERE entity_type='symbol_config' AND entity_id=?", emID)

	// Invalid: negative spread must be rejected with a ValidationError, before any commit.
	_, err := s.UpdateMinSpreadBps(ctx, emID, -10, "admin", "bad value")
	if err == nil {
		t.Fatal("UpdateMinSpreadBps(-10) must return an error")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("err = %v, want *ValidationError", err)
	}

	// No version activated, no symbol_config mutation, no audit row.
	if v := count("SELECT COUNT(*) FROM config_versions"); v != versionsBefore {
		t.Errorf("config_versions changed %d -> %d on a rejected update", versionsBefore, v)
	}
	if sp := count("SELECT min_spread_bps FROM symbol_configs WHERE exchange_market_id=?", emID); sp != 40 {
		t.Errorf("min_spread_bps mutated to %d on a rejected update, want 40", sp)
	}
	if a := count("SELECT COUNT(*) FROM config_change_audit WHERE entity_type='symbol_config' AND entity_id=?", emID); a != auditBefore {
		t.Errorf("config_change_audit grew %d -> %d on a rejected update", auditBefore, a)
	}

	// Sanity: a VALID update still commits (the guard only blocks invalid values).
	if _, err := s.UpdateMinSpreadBps(ctx, emID, 55, "admin", "valid"); err != nil {
		t.Fatalf("valid UpdateMinSpreadBps: %v", err)
	}
	if sp := count("SELECT min_spread_bps FROM symbol_configs WHERE exchange_market_id=?", emID); sp != 55 {
		t.Errorf("valid update min_spread_bps = %d, want 55", sp)
	}
}

// TestActiveVersionRejectsMultipleActive (PR6 correction) — ActiveVersion must NOT silently
// pick one when several rows are active: zero -> ErrNoActiveVersion, one -> id, more than
// one -> ErrMultipleActiveVersions. It controls + restores the shared active state.
func TestActiveVersionRejectsMultipleActive(t *testing.T) {
	db, ctx := gatedConfigDB(t)
	s := New(db)

	// Snapshot the currently-active versions, then neutralize them so this test controls
	// the state. Restore on cleanup (the invariant is at most one active).
	var prior []int64
	rows, err := db.QueryContext(ctx, "SELECT id FROM config_versions WHERE status='active' ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		prior = append(prior, id)
	}
	rows.Close()
	if _, err := db.ExecContext(ctx, "UPDATE config_versions SET status='superseded' WHERE status='active'"); err != nil {
		t.Fatal(err)
	}

	var id1, id2 int64
	t.Cleanup(func() {
		db.ExecContext(ctx, "DELETE FROM config_versions WHERE id IN (?, ?)", id1, id2)
		if len(prior) > 0 {
			db.ExecContext(ctx, "UPDATE config_versions SET status='active' WHERE id=?", prior[0])
		}
	})

	// Zero active -> ErrNoActiveVersion.
	if _, err := s.ActiveVersion(ctx); !errors.Is(err, ErrNoActiveVersion) {
		t.Fatalf("zero active: err=%v, want ErrNoActiveVersion", err)
	}

	// Two active -> ErrMultipleActiveVersions (NOT a silently-picked latest).
	insertActive := func() int64 {
		r, err := db.ExecContext(ctx, "INSERT INTO config_versions (status, created_by) VALUES ('active','t')")
		if err != nil {
			t.Fatal(err)
		}
		id, _ := r.LastInsertId()
		return id
	}
	id1 = insertActive()
	id2 = insertActive()
	if _, err := s.ActiveVersion(ctx); !errors.Is(err, ErrMultipleActiveVersions) {
		t.Fatalf("two active: err=%v, want ErrMultipleActiveVersions", err)
	}

	// Exactly one active -> its id.
	if _, err := db.ExecContext(ctx, "UPDATE config_versions SET status='superseded' WHERE id=?", id2); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ActiveVersion(ctx); err != nil || got != id1 {
		t.Fatalf("one active: got=%d err=%v, want %d", got, err, id1)
	}
}
