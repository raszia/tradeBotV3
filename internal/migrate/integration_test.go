package migrate

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// TestRunIntegration applies the embedded migrations against a real MariaDB and
// verifies idempotency. It is skipped unless V3_TEST_MYSQL_DSN points at a
// throwaway database, e.g.:
//
//	V3_TEST_MYSQL_DSN='root:root@tcp(127.0.0.1:3306)/v3test?multiStatements=false' go test ./internal/migrate/...
//
// The target database is mutated (tables created), so point it at a scratch DB.
func TestRunIntegration(t *testing.T) {
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run migration integration test")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// First run applies migration 001.
	res, err := Run(ctx, db, FS)
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if res.EndVersion < 1 {
		t.Fatalf("expected EndVersion >= 1, got %d", res.EndVersion)
	}

	// app_meta must now exist.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	exists, err := tableExists(ctx, conn, "app_meta")
	if err != nil || !exists {
		t.Fatalf("app_meta exists=%v err=%v", exists, err)
	}

	// Second run is a no-op (idempotent).
	res2, err := Run(ctx, db, FS)
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if len(res2.Applied) != 0 {
		t.Fatalf("second Run applied %d migrations, want 0", len(res2.Applied))
	}

	// Status / EnsureCurrent should now report clean.
	applied, pending, err := Status(ctx, db, FS)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending = %d, want 0", len(pending))
	}
	if len(applied) < 1 {
		t.Fatalf("applied = %d, want >= 1", len(applied))
	}
	if err := EnsureCurrent(ctx, db, FS); err != nil {
		t.Fatalf("EnsureCurrent: %v", err)
	}
}
