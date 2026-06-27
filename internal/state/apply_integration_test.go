package state

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"v3TradeBot/internal/migrate"
)

// TestApplyTransitionIntegration exercises ApplyCycleTransition against a real
// MariaDB so the actual UPDATE/INSERT SQL is validated against the PR2 schema
// (sqlmock cannot do that). Skipped unless V3_TEST_MYSQL_DSN points at a
// throwaway database. All work happens in a transaction that is rolled back, so
// the database is left unchanged.
//
//	V3_TEST_MYSQL_DSN='root:rootpw@tcp(127.0.0.1:13306)/v3tb_test' go test ./internal/state/... -run Integration
func TestApplyTransitionIntegration(t *testing.T) {
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the state integration test")
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
	if _, err := migrate.Run(ctx, db, migrate.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //nolint:errcheck // always roll back: leave the DB clean

	// Minimal valid object graph for a cycle.
	exec := func(q string, args ...any) sql.Result {
		res, err := tx.ExecContext(ctx, q, args...)
		if err != nil {
			t.Fatalf("setup exec %q: %v", q, err)
		}
		return res
	}
	lastID := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }

	baseID := lastID(exec(`INSERT INTO assets (symbol, kind) VALUES ('ITSTB','crypto')`))
	quoteID := lastID(exec(`INSERT INTO assets (symbol, kind) VALUES ('ITSTQ','crypto')`))
	marketID := lastID(exec(`INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES ('ITSTB/ITSTQ', ?, ?, 'OTHER')`, baseID, quoteID))
	exID := lastID(exec(`INSERT INTO exchanges (code, name) VALUES ('itstex','Integ Test')`))
	emID := lastID(exec(`INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, 'ITSTBITSTQ', 'ITSTB/ITSTQ')`, exID, marketID))
	cycID := lastID(exec(`INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol) VALUES (?, ?, 'ITSTB/ITSTQ')`, emID, exID))

	// New cycle starts at state NEW, version 0.
	var st string
	var ver int64
	if err := tx.QueryRowContext(ctx, "SELECT state, version FROM cycles WHERE id=?", cycID).Scan(&st, &ver); err != nil {
		t.Fatal(err)
	}
	if st != string(CycleNew) || ver != 0 {
		t.Fatalf("new cycle = (%s,%d), want (NEW,0)", st, ver)
	}

	// Apply NEW -> SIGNAL_DETECTED.
	res, err := ApplyCycleTransition(ctx, tx, CycleTransition{
		CycleID: cycID, From: CycleNew, To: CycleSignalDetected, Version: 0,
		EventType: "signal_detected", Reason: "integration test",
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if res.NewVersion != 1 || res.Replayed {
		t.Fatalf("result = %+v, want version 1 fresh", res)
	}

	// Row advanced.
	if err := tx.QueryRowContext(ctx, "SELECT state, version FROM cycles WHERE id=?", cycID).Scan(&st, &ver); err != nil {
		t.Fatal(err)
	}
	if st != string(CycleSignalDetected) || ver != 1 {
		t.Fatalf("after apply = (%s,%d), want (SIGNAL_DETECTED,1)", st, ver)
	}

	// Event row written with correct from/to/version.
	var fromState, toState string
	var evVer int64
	if err := tx.QueryRowContext(ctx,
		"SELECT from_state, to_state, version FROM cycle_state_events WHERE cycle_id=? ORDER BY id DESC LIMIT 1", cycID).
		Scan(&fromState, &toState, &evVer); err != nil {
		t.Fatalf("event row: %v", err)
	}
	if fromState != "NEW" || toState != "SIGNAL_DETECTED" || evVer != 1 {
		t.Fatalf("event = (%s->%s v%d), want (NEW->SIGNAL_DETECTED v1)", fromState, toState, evVer)
	}

	// Replaying the same transition (stale from/version) is a safe no-op and does
	// NOT write a second event.
	res2, err := ApplyCycleTransition(ctx, tx, CycleTransition{
		CycleID: cycID, From: CycleNew, To: CycleSignalDetected, Version: 0,
	})
	if err != nil {
		t.Fatalf("replay apply: %v", err)
	}
	if !res2.Replayed || res2.NewVersion != 1 {
		t.Fatalf("replay result = %+v, want replayed at version 1", res2)
	}
	var eventCount int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM cycle_state_events WHERE cycle_id=?", cycID).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 1 {
		t.Fatalf("event count = %d, want 1 (replay must not duplicate)", eventCount)
	}

	// A genuinely stale version (wrong observed version) is rejected.
	_, err = ApplyCycleTransition(ctx, tx, CycleTransition{
		CycleID: cycID, From: CycleSignalDetected, To: CycleBuyRequestQueued, Version: 99,
	})
	if err == nil {
		t.Fatal("expected stale-version error for wrong observed version")
	}
}
