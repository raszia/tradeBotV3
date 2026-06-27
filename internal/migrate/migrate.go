// Package migrate is the in-code database migration runner. All schema and
// reference-data changes ship as embedded NNN_name.sql files and are applied by
// the `migrate` binary — there is NO external/manual SQL setup.
//
// Safety model (see PROJECT_ARCHITECTURE.md "Migration rules"):
//
//   - A MariaDB session advisory lock (GET_LOCK) serializes concurrent runs so
//     two `migrate` invocations starting together cannot double-apply. The lock
//     is held on a SINGLE pinned *sql.Conn; all work runs on that same conn,
//     otherwise the lock (which is session-scoped) would protect nothing.
//   - schema_migrations records every applied version with its file checksum.
//     Editing an already-applied migration changes its checksum and is a HARD
//     STOP — you must add a new NNN file instead.
//   - DDL auto-commits in MariaDB and therefore CANNOT be wrapped in a real
//     transaction. So each migration declares its kind:
//   - "-- migrate:dml"  → statements + the schema_migrations row run inside one
//     transaction (truly atomic).
//   - "-- migrate:ddl"  → statements run directly (each auto-commits); the
//     version row is written only AFTER all statements
//     succeed. DDL migrations must be ONE idempotent change
//     (CREATE TABLE IF NOT EXISTS, ...) so re-running after a
//     mid-file failure is safe. Default when no directive given.
//   - Never mix dangerous DDL and data changes in the same file.
//
// Service binaries do NOT auto-migrate; they call EnsureCurrent on startup and
// fail fast if the schema is behind the code.
package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	// advisoryLockName is the GET_LOCK name guarding migration runs.
	advisoryLockName = "v3tradebot_migrate"
	// advisoryLockTimeoutSec bounds how long we wait for the lock before failing.
	advisoryLockTimeoutSec = 30
	// migrationsDir is the embedded directory holding NNN_name.sql files.
	migrationsDir = "migrations"
)

// Kind distinguishes transactional DML migrations from auto-committing DDL.
type Kind string

const (
	KindDDL Kind = "ddl"
	KindDML Kind = "dml"
)

// Migration is a single parsed migration file.
type Migration struct {
	Version  int
	Name     string
	Kind     Kind
	SQL      string
	Checksum string // hex(sha256(raw file bytes))
}

// Result summarises a Run.
type Result struct {
	StartVersion int
	EndVersion   int
	Applied      []Migration
}

// ChecksumMismatchError is returned when a file's checksum differs from what was
// recorded when its version was applied. This is a hard stop: an applied
// migration was edited, which is forbidden.
type ChecksumMismatchError struct {
	Version int
	Name    string
	Applied string
	File    string
}

func (e *ChecksumMismatchError) Error() string {
	return fmt.Sprintf(
		"migrate: checksum mismatch for version %d (%s): recorded=%s file=%s — never edit an applied migration; add a new NNN file instead",
		e.Version, e.Name, e.Applied, e.File)
}

// Run applies all pending migrations in version order under the advisory lock.
// It is idempotent: a second concurrent caller blocks on the lock then finds
// nothing pending.
func Run(ctx context.Context, db *sql.DB, fsys fs.FS) (Result, error) {
	migs, err := parse(fsys)
	if err != nil {
		return Result{}, err
	}

	// Pin one connection for the whole run; the advisory lock lives on it.
	conn, err := db.Conn(ctx)
	if err != nil {
		return Result{}, err
	}
	defer conn.Close()

	if err := acquireLock(ctx, conn); err != nil {
		return Result{}, err
	}
	defer releaseLock(ctx, conn)

	if err := ensureSchemaMigrations(ctx, conn); err != nil {
		return Result{}, err
	}
	applied, err := loadApplied(ctx, conn)
	if err != nil {
		return Result{}, err
	}

	res := Result{StartVersion: maxVersion(applied)}
	res.EndVersion = res.StartVersion

	todo, err := plan(migs, applied)
	if err != nil {
		return res, err
	}
	for _, m := range todo {
		if err := applyOne(ctx, conn, m); err != nil {
			return res, fmt.Errorf("migrate: apply %03d_%s: %w", m.Version, m.Name, err)
		}
		res.Applied = append(res.Applied, m)
		res.EndVersion = m.Version
	}
	return res, nil
}

// Status reports applied vs pending migrations without mutating anything. On a
// brand-new database (no schema_migrations table yet) every migration is
// reported pending — it does not create the table, keeping read-only callers
// (services) side-effect free.
func Status(ctx context.Context, db *sql.DB, fsys fs.FS) (appliedList, pending []Migration, err error) {
	migs, err := parse(fsys)
	if err != nil {
		return nil, nil, err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	applied := map[int]string{}
	exists, err := tableExists(ctx, conn, "schema_migrations")
	if err != nil {
		return nil, nil, err
	}
	if exists {
		applied, err = loadApplied(ctx, conn)
		if err != nil {
			return nil, nil, err
		}
	}

	for _, m := range migs {
		if _, ok := applied[m.Version]; ok {
			appliedList = append(appliedList, m)
		} else {
			pending = append(pending, m)
		}
	}
	return appliedList, pending, nil
}

// EnsureCurrent returns an error if any migration is pending. Service binaries
// call this on startup and refuse to run against a schema that is behind the
// code, so a freshly deployed binary can never operate on an old schema (and
// services never silently mutate the schema themselves).
func EnsureCurrent(ctx context.Context, db *sql.DB, fsys fs.FS) error {
	_, pending, err := Status(ctx, db, fsys)
	if err != nil {
		return fmt.Errorf("migrate: status check: %w", err)
	}
	if len(pending) > 0 {
		names := make([]string, len(pending))
		for i, m := range pending {
			names[i] = fmt.Sprintf("%03d_%s", m.Version, m.Name)
		}
		return fmt.Errorf("migrate: %d pending migration(s): %s — run the `migrate` binary before starting this service",
			len(pending), strings.Join(names, ", "))
	}
	return nil
}

// plan is the pure decision core: given all migrations and the set already
// applied (version → checksum), return those still to apply in order. It is the
// place the checksum hard-stop lives, kept free of DB I/O so it is exhaustively
// unit-testable.
func plan(migs []Migration, applied map[int]string) ([]Migration, error) {
	var todo []Migration
	for _, m := range migs {
		if cs, ok := applied[m.Version]; ok {
			if cs != m.Checksum {
				return nil, &ChecksumMismatchError{Version: m.Version, Name: m.Name, Applied: cs, File: m.Checksum}
			}
			continue // already applied, checksum matches
		}
		todo = append(todo, m)
	}
	return todo, nil
}

// applyOne applies a single migration, honoring the DDL/DML transaction rules.
func applyOne(ctx context.Context, conn *sql.Conn, m Migration) error {
	stmts := splitStatements(m.SQL)
	const insertVersion = `INSERT INTO schema_migrations (version, name, checksum, applied_by) VALUES (?, ?, ?, ?)`
	tag := appliedByTag()

	switch m.Kind {
	case KindDML:
		// DML is transactional: statements and the version row commit together.
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for _, s := range stmts {
			if _, err := tx.ExecContext(ctx, s); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, insertVersion, m.Version, m.Name, m.Checksum, tag); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()

	default: // KindDDL
		// DDL auto-commits per statement in MariaDB, so we cannot make this
		// atomic. The version row is recorded ONLY after every statement
		// succeeds; idempotent DDL makes a re-run after a partial failure safe.
		for _, s := range stmts {
			if _, err := conn.ExecContext(ctx, s); err != nil {
				return err
			}
		}
		_, err := conn.ExecContext(ctx, insertVersion, m.Version, m.Name, m.Checksum, tag)
		return err
	}
}

// acquireLock takes the session advisory lock on the pinned connection.
func acquireLock(ctx context.Context, conn *sql.Conn) error {
	var got sql.NullInt64
	err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", advisoryLockName, advisoryLockTimeoutSec).Scan(&got)
	if err != nil {
		return fmt.Errorf("migrate: acquire advisory lock: %w", err)
	}
	if !got.Valid || got.Int64 != 1 {
		return fmt.Errorf("migrate: could not acquire advisory lock %q within %ds (another migration running?)",
			advisoryLockName, advisoryLockTimeoutSec)
	}
	return nil
}

// releaseLock best-effort releases the advisory lock. Uses DO so no result rows
// are produced. Failure to release is non-fatal: the lock auto-releases when the
// pinned connection closes.
func releaseLock(ctx context.Context, conn *sql.Conn) {
	_, _ = conn.ExecContext(ctx, "DO RELEASE_LOCK(?)", advisoryLockName)
}

const schemaMigrationsDDL = `CREATE TABLE IF NOT EXISTS schema_migrations (
  version    INT NOT NULL PRIMARY KEY,
  name       VARCHAR(255) NOT NULL,
  checksum   CHAR(64) NOT NULL,
  applied_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  applied_by VARCHAR(191) NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

func ensureSchemaMigrations(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, schemaMigrationsDDL)
	return err
}

func loadApplied(ctx context.Context, conn *sql.Conn) (map[int]string, error) {
	rows, err := conn.QueryContext(ctx, "SELECT version, checksum FROM schema_migrations")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int]string{}
	for rows.Next() {
		var v int
		var cs string
		if err := rows.Scan(&v, &cs); err != nil {
			return nil, err
		}
		out[v] = cs
	}
	return out, rows.Err()
}

func tableExists(ctx context.Context, conn *sql.Conn, name string) (bool, error) {
	var n int
	err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		name).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// parse reads, validates, and sorts all embedded migration files.
func parse(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("migrate: read %s dir: %w", migrationsDir, err)
	}
	var migs []Migration
	seen := map[int]string{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		version, name, err := parseFilename(e.Name())
		if err != nil {
			return nil, err
		}
		if prev, ok := seen[version]; ok {
			return nil, fmt.Errorf("migrate: duplicate version %d: %q and %q", version, prev, e.Name())
		}
		seen[version] = e.Name()

		raw, err := fs.ReadFile(fsys, migrationsDir+"/"+e.Name())
		if err != nil {
			return nil, err
		}
		migs = append(migs, Migration{
			Version:  version,
			Name:     name,
			Kind:     parseKind(raw),
			SQL:      string(raw),
			Checksum: checksum(raw),
		})
	}
	sort.Slice(migs, func(i, j int) bool { return migs[i].Version < migs[j].Version })
	return migs, nil
}

// parseFilename splits "NNN_name.sql" into its numeric version and name.
func parseFilename(fn string) (int, string, error) {
	base := strings.TrimSuffix(fn, ".sql")
	idx := strings.IndexByte(base, '_')
	if idx <= 0 {
		return 0, "", fmt.Errorf("migrate: bad migration filename %q (want NNN_name.sql)", fn)
	}
	version, err := strconv.Atoi(base[:idx])
	if err != nil {
		return 0, "", fmt.Errorf("migrate: bad version number in %q: %w", fn, err)
	}
	name := base[idx+1:]
	if name == "" {
		return 0, "", fmt.Errorf("migrate: empty name in %q", fn)
	}
	return version, name, nil
}

// parseKind reads the "-- migrate:ddl|dml" directive from leading comment lines.
// Defaults to DDL (the conservative choice: no transaction wrapping, idempotent
// statements expected).
func parseKind(raw []byte) Kind {
	for _, line := range strings.Split(string(raw), "\n") {
		t := strings.TrimSpace(line)
		if !strings.HasPrefix(t, "--") {
			continue
		}
		l := strings.ToLower(t)
		if strings.Contains(l, "migrate:dml") {
			return KindDML
		}
		if strings.Contains(l, "migrate:ddl") {
			return KindDDL
		}
	}
	return KindDDL
}

func checksum(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func maxVersion(applied map[int]string) int {
	max := 0
	for v := range applied {
		if v > max {
			max = v
		}
	}
	return max
}

func appliedByTag() string {
	host, _ := os.Hostname()
	return fmt.Sprintf("%s/%d", host, os.Getpid())
}

// splitStatements splits a migration file into individual SQL statements on
// top-level semicolons, correctly ignoring ';' that appears inside single/double
// quotes, backtick identifiers, line comments (-- or #), and block comments
// (/* */), plus backslash escapes inside strings. Stored programs / DELIMITER
// are intentionally unsupported (migrations must not contain them).
func splitStatements(sqlText string) []string {
	var stmts []string
	var b strings.Builder
	runes := []rune(sqlText)
	n := len(runes)

	inSingle, inDouble, inBacktick := false, false, false

	for i := 0; i < n; i++ {
		c := runes[i]

		// Backslash escape inside a quoted string: copy the escaped pair.
		if (inSingle || inDouble) && c == '\\' && i+1 < n {
			b.WriteRune(c)
			b.WriteRune(runes[i+1])
			i++
			continue
		}

		if !inSingle && !inDouble && !inBacktick {
			// line comment: -- ... or # ...
			if c == '-' && i+1 < n && runes[i+1] == '-' {
				for i < n && runes[i] != '\n' {
					i++
				}
				continue
			}
			if c == '#' {
				for i < n && runes[i] != '\n' {
					i++
				}
				continue
			}
			// block comment: /* ... */
			if c == '/' && i+1 < n && runes[i+1] == '*' {
				i += 2
				for i+1 < n && !(runes[i] == '*' && runes[i+1] == '/') {
					i++
				}
				i++ // land on '/', loop's i++ moves past it
				continue
			}
			// statement terminator
			if c == ';' {
				addStmt(&stmts, b.String())
				b.Reset()
				continue
			}
		}

		switch c {
		case '\'':
			if !inDouble && !inBacktick {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBacktick {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBacktick = !inBacktick
			}
		}
		b.WriteRune(c)
	}
	addStmt(&stmts, b.String())
	return stmts
}

func addStmt(stmts *[]string, s string) {
	if t := strings.TrimSpace(s); t != "" {
		*stmts = append(*stmts, t)
	}
}
