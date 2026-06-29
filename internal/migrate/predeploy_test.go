package migrate

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func predeployDB(t *testing.T) (*sql.DB, context.Context) {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the predeploy migration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return db, ctx
}

// TestServicesRefusePendingMigrations (PR27) proves the fail-fast every service relies on:
// EnsureCurrent (called by service.RunWithDB at startup) returns an error when a migration
// is pending, so a service started against a schema behind the code refuses to run.
func TestServicesRefusePendingMigrations(t *testing.T) {
	db, ctx := predeployDB(t)
	if _, err := Run(ctx, db, FS); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Up to date -> EnsureCurrent passes.
	if err := EnsureCurrent(ctx, db, FS); err != nil {
		t.Fatalf("EnsureCurrent after Run should pass: %v", err)
	}
	// Simulate a code update that adds a migration the DB hasn't applied: forget the latest
	// applied row, so one migration is now pending.
	var maxV int
	if err := db.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_migrations").Scan(&maxV); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM schema_migrations WHERE version=?", maxV); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = Run(ctx, db, FS) }) // re-apply so the shared DB is left current

	err := EnsureCurrent(ctx, db, FS)
	if err == nil {
		t.Fatal("EnsureCurrent must FAIL when a migration is pending (services must refuse to start)")
	}
	if !strings.Contains(err.Error(), "pending") {
		t.Errorf("error should mention pending migrations, got: %v", err)
	}
}

// TestKillSwitchDefaultsEngaged (PR27) proves the SAFE default: an operator inserting the
// live_controls singleton with no explicit kill_switch gets it ENGAGED (=1).
func TestKillSwitchDefaultsEngaged(t *testing.T) {
	db, ctx := predeployDB(t)
	if _, err := Run(ctx, db, FS); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Use a transaction so we don't disturb the shared singleton for other gated tests.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DELETE FROM live_controls WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO live_controls (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	var kill int
	if err := tx.QueryRowContext(ctx, "SELECT kill_switch FROM live_controls WHERE id=1").Scan(&kill); err != nil {
		t.Fatal(err)
	}
	if kill != 1 {
		t.Errorf("kill_switch default = %d, want 1 (engaged by default — safe)", kill)
	}
}
