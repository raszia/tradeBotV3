package migrate

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestChecksumStable(t *testing.T) {
	a := checksum([]byte("CREATE TABLE x (id INT);"))
	b := checksum([]byte("CREATE TABLE x (id INT);"))
	if a != b {
		t.Fatal("checksum not stable for identical input")
	}
	if a == checksum([]byte("CREATE TABLE x (id INT);  ")) {
		t.Fatal("checksum should be sensitive to trailing whitespace (raw bytes)")
	}
	if len(a) != 64 {
		t.Fatalf("checksum length = %d, want 64 hex chars", len(a))
	}
}

func TestParseFilename(t *testing.T) {
	type want struct {
		version int
		name    string
		err     bool
	}
	cases := map[string]want{
		"001_app_meta.sql":   {1, "app_meta", false},
		"042_add_orders.sql": {42, "add_orders", false},
		"007_a_b_c.sql":      {7, "a_b_c", false},
		"bad.sql":            {0, "", true},
		"_noversion.sql":     {0, "", true},
		"12.sql":             {0, "", true}, // no underscore
		"00x_bad.sql":        {0, "", true},
		"003_.sql":           {0, "", true}, // empty name
	}
	for fn, w := range cases {
		v, n, err := parseFilename(fn)
		if w.err {
			if err == nil {
				t.Errorf("parseFilename(%q): expected error", fn)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseFilename(%q): unexpected error %v", fn, err)
			continue
		}
		if v != w.version || n != w.name {
			t.Errorf("parseFilename(%q) = (%d,%q), want (%d,%q)", fn, v, n, w.version, w.name)
		}
	}
}

func TestParseKind(t *testing.T) {
	cases := map[string]Kind{
		"-- migrate:ddl\nCREATE TABLE x();":          KindDDL,
		"-- migrate:dml\nUPDATE x SET a=1;":          KindDML,
		"--migrate:dml\nUPDATE x;":                   KindDML,
		"CREATE TABLE x();":                          KindDDL, // default
		"-- some comment\n-- migrate:DML\nUPDATE x;": KindDML, // case-insensitive
		// Directive-looking text inside a string is not a comment line, so default.
		"INSERT INTO t VALUES ('migrate:dml');": KindDDL,
	}
	for sql, want := range cases {
		if got := parseKind([]byte(sql)); got != want {
			t.Errorf("parseKind(%q) = %v, want %v", sql, got, want)
		}
	}
}

func TestSplitStatements(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"simple", "A; B; C", []string{"A", "B", "C"}},
		{"trailing semicolon", "A; B;", []string{"A", "B"}},
		{"no trailing", "A", []string{"A"}},
		{"empty between", "A;; B", []string{"A", "B"}},
		{"line comment", "A; -- B; C\nD", []string{"A", "D"}},
		{"hash comment", "A; # ignored;\nD", []string{"A", "D"}},
		{"block comment", "A /* ; still A ; */ ; B", []string{"A", "B"}},
		{"semicolon in single quote", "INSERT INTO t VALUES ('a;b'); SELECT 1", []string{"INSERT INTO t VALUES ('a;b')", "SELECT 1"}},
		{"semicolon in backtick", "SELECT `a;b` FROM t; SELECT 2", []string{"SELECT `a;b` FROM t", "SELECT 2"}},
		{"escaped quote", `INSERT INTO t VALUES ('a\';b'); SELECT 3`, []string{`INSERT INTO t VALUES ('a\';b')`, "SELECT 3"}},
	}
	for _, c := range cases {
		got := splitStatements(c.in)
		if len(got) != len(c.want) {
			t.Errorf("%s: got %d stmts %q, want %d %q", c.name, len(got), got, len(c.want), c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("%s: stmt[%d] = %q, want %q", c.name, i, got[i], c.want[i])
			}
		}
	}
}

func TestParseSortsAndDetectsDuplicates(t *testing.T) {
	fsys := fstest.MapFS{
		"migrations/002_b.sql": {Data: []byte("-- migrate:ddl\nCREATE TABLE b();")},
		"migrations/001_a.sql": {Data: []byte("-- migrate:ddl\nCREATE TABLE a();")},
		"migrations/notes.txt": {Data: []byte("ignored")},
	}
	migs, err := parse(fsys)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(migs) != 2 {
		t.Fatalf("got %d migrations, want 2 (non-sql ignored)", len(migs))
	}
	if migs[0].Version != 1 || migs[1].Version != 2 {
		t.Fatalf("not sorted: %d then %d", migs[0].Version, migs[1].Version)
	}

	dup := fstest.MapFS{
		"migrations/001_a.sql":     {Data: []byte("CREATE TABLE a();")},
		"migrations/001_again.sql": {Data: []byte("CREATE TABLE c();")},
	}
	if _, err := parse(dup); err == nil {
		t.Fatal("expected duplicate-version error")
	}
}

func TestPlan(t *testing.T) {
	migs := []Migration{
		{Version: 1, Name: "a", Checksum: "csA"},
		{Version: 2, Name: "b", Checksum: "csB"},
		{Version: 3, Name: "c", Checksum: "csC"},
	}

	// Nothing applied -> all pending.
	todo, err := plan(migs, map[int]string{})
	if err != nil || len(todo) != 3 {
		t.Fatalf("plan(empty): todo=%d err=%v", len(todo), err)
	}

	// Some applied -> remainder.
	todo, err = plan(migs, map[int]string{1: "csA"})
	if err != nil || len(todo) != 2 || todo[0].Version != 2 {
		t.Fatalf("plan(partial): todo=%v err=%v", todo, err)
	}

	// All applied -> nothing.
	todo, err = plan(migs, map[int]string{1: "csA", 2: "csB", 3: "csC"})
	if err != nil || len(todo) != 0 {
		t.Fatalf("plan(all): todo=%d err=%v", len(todo), err)
	}

	// Checksum mismatch on an applied version -> hard stop.
	_, err = plan(migs, map[int]string{2: "DIFFERENT"})
	var mismatch *ChecksumMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected ChecksumMismatchError, got %v", err)
	}
	if mismatch.Version != 2 {
		t.Fatalf("mismatch version = %d, want 2", mismatch.Version)
	}
}

// TestRunFailsWhenLockNotAcquired verifies that if GET_LOCK does not return 1
// (another migration holds the lock), Run aborts before touching the schema.
func TestRunFailsWhenLockNotAcquired(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	// GET_LOCK returns 0 (timeout). No schema_migrations work should follow.
	mock.ExpectQuery("SELECT GET_LOCK").
		WillReturnRows(sqlmock.NewRows([]string{"l"}).AddRow(int64(0)))

	_, err = Run(context.Background(), mockDB, FS)
	if err == nil {
		t.Fatal("expected error when advisory lock not acquired")
	}
	// Embedded migrations parsed fine; failure must be the lock.
	if got := err.Error(); !strings.Contains(got, "advisory lock") {
		t.Fatalf("error = %q, want advisory lock failure", got)
	}
}
