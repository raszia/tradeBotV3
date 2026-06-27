package symbollock

import (
	"context"
	"database/sql"
)

// This file implements the parts of the symbol lock the reconciler (PR12) needs:
// loading active locks and CONSERVATIVELY releasing one. Lock ACQUISITION (the
// transactional acquire-lock + create-cycle + enqueue path) is implemented in PR9.

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
