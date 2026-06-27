package configstore

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"v3TradeBot/internal/migrate"
)

// TestConfigStoreIntegration exercises the full config load + version + audit path
// against a real MariaDB. Skipped unless V3_TEST_MYSQL_DSN points at a throwaway
// database. Seeded rows are left in place (intended for an ephemeral test DB).
func TestConfigStoreIntegration(t *testing.T) {
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the configstore integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := migrate.Run(ctx, db, migrate.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	exec := func(q string, args ...any) sql.Result {
		res, err := db.ExecContext(ctx, q, args...)
		if err != nil {
			t.Fatalf("seed exec %q: %v", q, err)
		}
		return res
	}
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }

	// Seed: exchange + market + symbol_config + exchange_config + retention. Use a
	// unique code so repeat runs don't collide on the unique(code) constraint.
	exCode := "cfgtest"
	exID := last(exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'Cfg Test', 1)", exCode))
	baseID := last(exec("INSERT INTO assets (symbol, kind) VALUES ('CFGB','crypto')"))
	quoteID := last(exec("INSERT INTO assets (symbol, kind) VALUES ('CFGQ','crypto')"))
	mktID := last(exec("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES ('CFGB/CFGQ', ?, ?, 'OTHER')", baseID, quoteID))
	emID := last(exec(`INSERT INTO exchange_markets
		(exchange_id, market_id, exchange_symbol, canonical_symbol, enabled_for_collection, enabled_for_signal, enabled_for_trading, enabled_for_sell_manage)
		VALUES (?, ?, 'CFGBCFGQ', 'CFGB/CFGQ', 1, 1, 1, 1)`, exID, mktID))
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
