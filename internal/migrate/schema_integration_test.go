package migrate

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// TestSchemaIntegration applies the migrations to a real MariaDB and asserts the
// PR2 schema is shaped correctly (tables, indexes, FKs, unique constraints, enum
// values, no plaintext credential columns, retention indexes, and the
// generated-column active-lock uniqueness). Skipped unless V3_TEST_MYSQL_DSN is
// set to a THROWAWAY database, e.g.:
//
//	V3_TEST_MYSQL_DSN='root:rootpw@tcp(127.0.0.1:13306)/v3tb_test' go test ./internal/migrate/... -run Schema
func TestSchemaIntegration(t *testing.T) {
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run schema integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// Fresh apply (idempotent if already applied) + no-op re-apply.
	if _, err := Run(ctx, db, FS); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	res2, err := Run(ctx, db, FS)
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if len(res2.Applied) != 0 {
		t.Fatalf("second Run applied %d migrations, want 0 (idempotent)", len(res2.Applied))
	}
	if err := EnsureCurrent(ctx, db, FS); err != nil {
		t.Fatalf("EnsureCurrent: %v", err)
	}

	t.Run("expected tables exist", func(t *testing.T) {
		for _, tbl := range expectedTables {
			if !tableExistsDB(t, db, tbl) {
				t.Errorf("missing table %q", tbl)
			}
		}
	})

	t.Run("foreign keys exist on core tables", func(t *testing.T) {
		// (table, fk constraint name) pairs that must exist.
		fks := [][2]string{
			{"markets", "fk_markets_base"},
			{"exchange_markets", "fk_exmarket_exchange"},
			{"exchange_credentials", "fk_credential_exchange"},
			{"symbol_configs", "fk_symbolcfg_market"},
			{"cycles", "fk_cycles_market"},
			{"cycle_state_events", "fk_cycle_events_cycle"},
			{"orders", "fk_orders_cycle"},
			{"order_events", "fk_order_events_order"},
			{"fills", "fk_fills_order"},
			{"symbol_locks", "fk_locks_cycle"},
			{"exchange_requests", "fk_exreq_exchange"},
			{"signals", "fk_signals_cycle"},
		}
		for _, fk := range fks {
			if !constraintExists(t, db, fk[0], fk[1], "FOREIGN KEY") {
				t.Errorf("missing FK %q on %q", fk[1], fk[0])
			}
		}
	})

	t.Run("high-volume log tables have NO foreign keys", func(t *testing.T) {
		// Intentional design (item 9): these are written async and pruned, so they
		// carry no FKs.
		for _, tbl := range []string{"api_call_logs", "app_logs", "comparison_events", "exchange_health_samples", "wallet_balance_history"} {
			if n := fkCount(t, db, tbl); n != 0 {
				t.Errorf("table %q has %d FKs, want 0 (high-volume design)", tbl, n)
			}
		}
	})

	t.Run("unique constraints exist", func(t *testing.T) {
		uniques := [][2]string{
			{"orders", "uq_orders_local_coid"},
			{"exchange_requests", "uq_exreq_idem"},
			{"symbol_locks", "uq_active_lock"},
			{"exchange_markets", "uq_exmarket"},
			{"wallet_balances_current", "uq_balance_current"},
			{"exchange_credentials", "uq_credential"},
		}
		for _, u := range uniques {
			if !indexExists(t, db, u[0], u[1]) {
				t.Errorf("missing unique index %q on %q", u[1], u[0])
			}
		}
	})

	t.Run("queue claim/retry indexes exist", func(t *testing.T) {
		for _, idx := range []string{"idx_exreq_claim", "idx_exreq_retry", "idx_exreq_inflight"} {
			if !indexExists(t, db, "exchange_requests", idx) {
				t.Errorf("missing index %q on exchange_requests", idx)
			}
		}
	})

	t.Run("high-volume tables have a created_at retention index", func(t *testing.T) {
		// index name -> table; each indexes created_at for batched retention deletes.
		retention := map[string]string{
			"api_call_logs":           "idx_apilog_created",
			"app_logs":                "idx_applog_created",
			"comparison_events":       "idx_cmp_created",
			"exchange_health_samples": "idx_healthsample_created",
			"wallet_balance_history":  "idx_balhist_created",
		}
		for tbl, idx := range retention {
			if !indexExists(t, db, tbl, idx) {
				t.Errorf("missing retention index %q on %q", idx, tbl)
			}
		}
	})

	t.Run("exchange_requests status enum values", func(t *testing.T) {
		got := columnType(t, db, "exchange_requests", "status")
		want := "enum('QUEUED','CLAIMED','IN_FLIGHT','SUCCEEDED','FAILED','RETRY_SCHEDULED','DEAD')"
		if got != want {
			t.Errorf("status column type =\n  %s\nwant\n  %s", got, want)
		}
	})

	t.Run("exchange_credentials has no plaintext secret columns", func(t *testing.T) {
		for _, col := range []string{"api_key", "api_secret", "passphrase"} {
			if columnExists(t, db, "exchange_credentials", col) {
				t.Errorf("plaintext column %q must not exist on exchange_credentials", col)
			}
		}
		for _, col := range []string{"encrypted_api_key", "encrypted_api_secret", "encrypted_passphrase", "encryption_algorithm", "key_version"} {
			if !columnExists(t, db, "exchange_credentials", col) {
				t.Errorf("expected column %q on exchange_credentials", col)
			}
		}
	})

	t.Run("generated active_key enforces one ACTIVE lock per scope+symbol", func(t *testing.T) {
		assertActiveLockUniqueness(t, db)
	})
}

// --- introspection helpers (all scoped to the connection's DATABASE()) ---

func tableExistsDB(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var n int
	mustScan(t, db, &n,
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`, name)
	return n > 0
}

func columnExists(t *testing.T, db *sql.DB, table, col string) bool {
	t.Helper()
	var n int
	mustScan(t, db, &n,
		`SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
		table, col)
	return n > 0
}

func columnType(t *testing.T, db *sql.DB, table, col string) string {
	t.Helper()
	var s string
	mustScan(t, db, &s,
		`SELECT column_type FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
		table, col)
	return s
}

func indexExists(t *testing.T, db *sql.DB, table, index string) bool {
	t.Helper()
	var n int
	mustScan(t, db, &n,
		`SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?`,
		table, index)
	return n > 0
}

func constraintExists(t *testing.T, db *sql.DB, table, name, ctype string) bool {
	t.Helper()
	var n int
	mustScan(t, db, &n,
		`SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema = DATABASE() AND table_name = ? AND constraint_name = ? AND constraint_type = ?`,
		table, name, ctype)
	return n > 0
}

func fkCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	mustScan(t, db, &n,
		`SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema = DATABASE() AND table_name = ? AND constraint_type = 'FOREIGN KEY'`,
		table)
	return n
}

func mustScan(t *testing.T, db *sql.DB, dst any, query string, args ...any) {
	t.Helper()
	if err := db.QueryRow(query, args...).Scan(dst); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
}

// assertActiveLockUniqueness inserts a minimal valid object graph and verifies
// that a second ACTIVE symbol_locks row for the same scope+symbol is rejected by
// the unique generated-column index. Everything runs in a transaction that is
// rolled back, so the test leaves no residue.
func assertActiveLockUniqueness(t *testing.T, db *sql.DB) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck // always roll back; this test only probes a constraint

	exec := func(q string, args ...any) sql.Result {
		res, err := tx.Exec(q, args...)
		if err != nil {
			t.Fatalf("setup exec failed: %q: %v", q, err)
		}
		return res
	}
	lastID := func(r sql.Result) int64 {
		id, err := r.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	baseID := lastID(exec(`INSERT INTO assets (symbol, kind) VALUES ('TSTBASE','crypto')`))
	quoteID := lastID(exec(`INSERT INTO assets (symbol, kind) VALUES ('TSTQUOTE','crypto')`))
	marketID := lastID(exec(
		`INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES ('TSTBASE/TSTQUOTE', ?, ?, 'OTHER')`,
		baseID, quoteID))
	exID := lastID(exec(`INSERT INTO exchanges (code, name) VALUES ('tstex','Test Exchange')`))
	emID := lastID(exec(
		`INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, 'TSTBASETSTQUOTE', 'TSTBASE/TSTQUOTE')`,
		exID, marketID))
	cycID := lastID(exec(
		`INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol) VALUES (?, ?, 'TSTBASE/TSTQUOTE')`,
		emID, exID))

	// First ACTIVE lock: must succeed.
	if _, err := tx.Exec(
		`INSERT INTO symbol_locks (scope, canonical_symbol, cycle_id, expires_at) VALUES ('tstex','TSTBASE/TSTQUOTE', ?, NOW(6) + INTERVAL 1 HOUR)`,
		cycID); err != nil {
		t.Fatalf("first ACTIVE lock should insert: %v", err)
	}
	// Second ACTIVE lock for the same scope+symbol: must be rejected.
	_, err = tx.Exec(
		`INSERT INTO symbol_locks (scope, canonical_symbol, cycle_id, expires_at) VALUES ('tstex','TSTBASE/TSTQUOTE', ?, NOW(6) + INTERVAL 1 HOUR)`,
		cycID)
	if err == nil {
		t.Fatal("second ACTIVE lock for the same scope+symbol must violate uq_active_lock")
	}
}
