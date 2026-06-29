package migrate

import (
	"errors"
	"strings"
	"testing"
)

// TestCheckAppliedIntegrity (PR1 correction) unit-tests the pure verification core that
// EnsureCurrent/Status rely on: a matching applied set passes; an edited (checksum-changed)
// applied migration is a ChecksumMismatchError; an applied version the binary does not know
// is an UnknownAppliedVersionError. No DB needed.
func TestCheckAppliedIntegrity(t *testing.T) {
	migs := []Migration{{Version: 1, Name: "a", Checksum: "cs1"}, {Version: 2, Name: "b", Checksum: "cs2"}}

	if err := checkAppliedIntegrity(migs, map[int]string{1: "cs1", 2: "cs2"}); err != nil {
		t.Errorf("matching applied set should pass, got %v", err)
	}

	var cm *ChecksumMismatchError
	if err := checkAppliedIntegrity(migs, map[int]string{1: "WRONG", 2: "cs2"}); !errors.As(err, &cm) {
		t.Fatalf("edited migration: err = %v, want ChecksumMismatchError", err)
	} else if cm.Version != 1 {
		t.Errorf("mismatch version = %d, want 1", cm.Version)
	}

	var uk *UnknownAppliedVersionError
	if err := checkAppliedIntegrity(migs, map[int]string{1: "cs1", 5: "cs5"}); !errors.As(err, &uk) {
		t.Fatalf("unknown applied version: err = %v, want UnknownAppliedVersionError", err)
	} else if uk.Version != 5 || uk.MaxKnown != 2 {
		t.Errorf("unknown = v%d (maxKnown %d), want v5 (maxKnown 2)", uk.Version, uk.MaxKnown)
	}
}

// TestEnsureCurrentFailsOnChecksumMismatch (PR1 correction) — the blocking case: an applied
// migration whose file later changed (checksum differs) must make EnsureCurrent (hence
// service startup, which calls it) refuse to run. Status reports it too.
func TestEnsureCurrentFailsOnChecksumMismatch(t *testing.T) {
	db, ctx := predeployDB(t)
	if _, err := Run(ctx, db, FS); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := EnsureCurrent(ctx, db, FS); err != nil {
		t.Fatalf("clean EnsureCurrent should pass: %v", err)
	}

	var orig string
	if err := db.QueryRowContext(ctx, "SELECT checksum FROM schema_migrations WHERE version=1").Scan(&orig); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE schema_migrations SET checksum=? WHERE version=1", strings.Repeat("0", 64)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.ExecContext(ctx, "UPDATE schema_migrations SET checksum=? WHERE version=1", orig) })

	var cm *ChecksumMismatchError
	if err := EnsureCurrent(ctx, db, FS); !errors.As(err, &cm) {
		t.Fatalf("EnsureCurrent err = %v, want ChecksumMismatchError", err)
	} else if cm.Version != 1 {
		t.Errorf("mismatch version = %d, want 1", cm.Version)
	}

	// Status reports the same mismatch (exposes enough information for EnsureCurrent to fail).
	if _, _, serr := Status(ctx, db, FS); !errors.As(serr, &cm) {
		t.Errorf("Status err = %v, want ChecksumMismatchError", serr)
	}
}

// TestEnsureCurrentFailsOnUnknownAppliedVersion (PR1 correction) — an applied version the
// binary does not know (DB schema newer than the code) must make EnsureCurrent refuse.
func TestEnsureCurrentFailsOnUnknownAppliedVersion(t *testing.T) {
	db, ctx := predeployDB(t)
	if _, err := Run(ctx, db, FS); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"INSERT INTO schema_migrations (version, name, checksum) VALUES (99999, 'from_a_newer_binary', ?)",
		checksum([]byte("future"))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM schema_migrations WHERE version=99999") })

	var uk *UnknownAppliedVersionError
	if err := EnsureCurrent(ctx, db, FS); !errors.As(err, &uk) {
		t.Fatalf("EnsureCurrent err = %v, want UnknownAppliedVersionError", err)
	} else if uk.Version != 99999 {
		t.Errorf("unknown applied version = %d, want 99999", uk.Version)
	}
	if !strings.Contains(uk.Error(), "newer") {
		t.Errorf("error should say the schema is newer/unrecognized: %v", uk)
	}
}
