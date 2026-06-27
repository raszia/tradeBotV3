package symbollock

import (
	"context"
	"database/sql"
	"errors"

	"github.com/go-sql-driver/mysql"
)

// This file implements the symbol lock: ACQUIRE (PR9 — the cross-process
// one-active-intent gate) plus the reconciler's (PR12) load + CONSERVATIVE release.

// ErrSymbolLocked is returned by Acquire when the scope already has an ACTIVE lock
// (a duplicate on the generated UNIQUE active_key). The caller's transaction should
// roll back so no orphan cycle is created.
var ErrSymbolLocked = errors.New("symbollock: scope already locked")

// Acquire inserts an ACTIVE lock for (scope, canonicalSymbol) owned by cycleID,
// within the caller's transaction tx. The unique-when-active generated column
// (active_key) guarantees at most one ACTIVE lock per scope across all processes:
// a duplicate maps to ErrSymbolLocked. The lease is leaseSeconds from the DB clock
// (NOW(6)), so the database clock is authoritative.
//
// This MUST run in the SAME transaction that creates the cycle (and order/request),
// so either the whole intent commits or nothing does (no lock without a cycle).
func Acquire(ctx context.Context, tx *sql.Tx, scope, canonicalSymbol string, cycleID int64, leaseSeconds int) (int64, error) {
	if leaseSeconds <= 0 {
		leaseSeconds = 600
	}
	res, err := tx.ExecContext(ctx,
		"INSERT INTO symbol_locks (scope, canonical_symbol, cycle_id, state, expires_at) "+
			"VALUES (?, ?, ?, 'ACTIVE', NOW(6) + INTERVAL ? SECOND)",
		scope, canonicalSymbol, cycleID, leaseSeconds)
	if err != nil {
		var myErr *mysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1062 { // duplicate active_key
			return 0, ErrSymbolLocked
		}
		return 0, err
	}
	return res.LastInsertId()
}

// Lock is a row of symbol_locks.
type Lock struct {
	ID              int64
	Scope           string
	CanonicalSymbol string
	CycleID         int64
	State           string // ACTIVE | RELEASED | STALE
}

// LoadActive returns all ACTIVE locks.
func LoadActive(ctx context.Context, db *sql.DB) ([]Lock, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT id, scope, canonical_symbol, cycle_id, state FROM symbol_locks WHERE state = 'ACTIVE'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Lock
	for rows.Next() {
		var l Lock
		if err := rows.Scan(&l.ID, &l.Scope, &l.CanonicalSymbol, &l.CycleID, &l.State); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// ActiveByCycle returns the ACTIVE lock owned by a cycle, ok=false if none.
func ActiveByCycle(ctx context.Context, tx *sql.Tx, cycleID int64) (Lock, bool, error) {
	var l Lock
	err := tx.QueryRowContext(ctx,
		"SELECT id, scope, canonical_symbol, cycle_id, state FROM symbol_locks WHERE cycle_id = ? AND state = 'ACTIVE' LIMIT 1",
		cycleID).Scan(&l.ID, &l.Scope, &l.CanonicalSymbol, &l.CycleID, &l.State)
	if err == sql.ErrNoRows {
		return Lock{}, false, nil
	}
	if err != nil {
		return Lock{}, false, err
	}
	return l, true, nil
}

// Release moves an ACTIVE lock to RELEASED within tx. Callers must only do this
// when the owning cycle is positively safe/terminal (PR12 rule #8) — releasing a
// lock for a live/uncertain cycle would let a second signal trade the same symbol
// concurrently.
func Release(ctx context.Context, tx *sql.Tx, lockID int64) error {
	_, err := tx.ExecContext(ctx,
		"UPDATE symbol_locks SET state='RELEASED', released_at=NOW(6), updated_at=NOW(6) WHERE id=? AND state='ACTIVE'",
		lockID)
	return err
}
