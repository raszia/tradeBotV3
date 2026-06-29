package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// Result reports the outcome of an applied transition.
type Result struct {
	// NewVersion is the row's version after the transition (or the current
	// version if the transition was a replay no-op).
	NewVersion int64
	// Replayed is true only when this EXACT transition was already applied — the row
	// is in the target state at version expected+1 and the recorded event at that
	// version is this same from->to. The call is then a safe idempotent no-op and NO
	// duplicate event row is written. A row that merely happens to share the target
	// state at a DIFFERENT (higher) version is a stale transition, not a replay.
	Replayed bool
}

// CycleTransition describes one cycle state change. EventType and Reason are
// recorded on the cycle_state_events row; Payload (optional JSON) captures extra
// context. Version is the cycle version the caller observed (optimistic
// concurrency token).
type CycleTransition struct {
	CycleID   int64
	From      CycleState
	To        CycleState
	Version   int64
	EventType string
	Reason    string
	Payload   json.RawMessage
}

// OrderTransition is the order-state analogue of CycleTransition.
type OrderTransition struct {
	OrderID   int64
	From      OrderState
	To        OrderState
	Version   int64
	EventType string
	Reason    string
	Payload   json.RawMessage
}

// ApplyCycleTransition validates from->to and atomically (within the caller's
// transaction tx):
//
//  1. updates cycles guarded by (id, state, version) — optimistic concurrency,
//  2. inserts a cycle_state_events row with from/to/new-version.
//
// It MUST be called inside the SAME transaction as any related writes (e.g. the
// enqueue of an exchange_request) so "registered before sent" and lifecycle
// determinism hold all-or-nothing. On a zero-row update it re-reads the row to
// distinguish replay / stale version / state mismatch / missing row.
func ApplyCycleTransition(ctx context.Context, tx *sql.Tx, t CycleTransition) (Result, error) {
	if err := ValidateCycleTransition(t.From, t.To); err != nil {
		return Result{}, err
	}
	return applyTransition(ctx, tx, transitionSpec{
		table:          "cycles",
		idCol:          "id",
		eventTable:     "cycle_state_events",
		eventParentCol: "cycle_id",
		id:             t.CycleID,
		from:           string(t.From),
		to:             string(t.To),
		version:        t.Version,
		eventType:      defaultEventType(t.EventType, string(t.To)),
		message:        t.Reason,
		payload:        t.Payload,
	})
}

// ApplyOrderTransition is the order analogue of ApplyCycleTransition.
func ApplyOrderTransition(ctx context.Context, tx *sql.Tx, t OrderTransition) (Result, error) {
	if err := ValidateOrderTransition(t.From, t.To); err != nil {
		return Result{}, err
	}
	return applyTransition(ctx, tx, transitionSpec{
		table:          "orders",
		idCol:          "id",
		eventTable:     "order_events",
		eventParentCol: "order_id",
		id:             t.OrderID,
		from:           string(t.From),
		to:             string(t.To),
		version:        t.Version,
		eventType:      defaultEventType(t.EventType, string(t.To)),
		message:        t.Reason,
		payload:        t.Payload,
	})
}

// transitionSpec carries the per-entity SQL identifiers and the transition data.
// Table/column names are compile-time constants supplied by the Apply* wrappers
// (never user input), so interpolating them into the SQL is safe.
type transitionSpec struct {
	table          string
	idCol          string
	eventTable     string
	eventParentCol string
	id             int64
	from           string
	to             string
	version        int64
	eventType      string
	message        string
	payload        json.RawMessage
}

func applyTransition(ctx context.Context, tx *sql.Tx, s transitionSpec) (Result, error) {
	// Optimistic-concurrency update: only succeeds if the row is still in the
	// expected state at the expected version. version is bumped so concurrent
	// writers can detect the change.
	updateSQL := fmt.Sprintf(
		"UPDATE %s SET state = ?, version = version + 1, updated_at = NOW(6) WHERE %s = ? AND state = ? AND version = ?",
		s.table, s.idCol)
	res, err := tx.ExecContext(ctx, updateSQL, s.to, s.id, s.from, s.version)
	if err != nil {
		return Result{}, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return Result{}, err
	}

	if affected == 1 {
		newVersion := s.version + 1
		// Event insert is in the SAME tx as the update: both commit or both roll
		// back, so a transition is never recorded without its state change and
		// vice versa. UNIQUE(parent, version) additionally blocks duplicate events.
		if err := insertEvent(ctx, tx, s, newVersion); err != nil {
			return Result{}, err
		}
		return Result{NewVersion: newVersion}, nil
	}

	// Zero rows updated: figure out why by re-reading the current row.
	curState, curVersion, found, err := readRowState(ctx, tx, s.table, s.idCol, s.id)
	if err != nil {
		return Result{}, err
	}
	switch {
	case !found:
		return Result{}, ErrUnknownRow
	case curState == s.to && curVersion == s.version+1:
		// EXACT already-applied transition: the row is in the target state AND its
		// version is precisely expected+1, i.e. this transition (and only this one)
		// advanced the row off the version the caller observed. As a stronger guard,
		// confirm the recorded event at that version is THIS from->to — not a
		// coincidental same-target reached by a different path. Only then is it a true
		// idempotent replay no-op (we do NOT write a second event for the same version).
		matched, err := replayEventMatches(ctx, tx, s, curVersion)
		if err != nil {
			return Result{}, err
		}
		if !matched {
			return Result{}, ErrStaleVersion
		}
		return Result{NewVersion: curVersion, Replayed: true}, nil
	case curVersion != s.version:
		// The row has moved on from the version the caller observed. This INCLUDES a
		// row already in the target state but at a version higher than expected+1: that
		// is a STALE transition (a late/duplicate caller acting on old knowledge), NOT a
		// replay, and is rejected — e.g. SELL_REPRICE_PENDING->SELL_SUBMITTED with
		// expected_version=10 against a row already at SELL_SUBMITTED version 20.
		return Result{}, ErrStaleVersion
	default:
		// curVersion == s.version but the state is not `from` (and not the exact
		// replay): the caller's expected `from` is wrong / inconsistent.
		return Result{}, ErrStateMismatch
	}
}

// replayEventMatches confirms the state-event recorded at the given version is exactly the
// transition being replayed (same parent, version, from_state, to_state). It is the optional
// stronger replay guard: a row that merely happens to be in the target state at version
// expected+1 is only a TRUE replay if the event history shows THIS from->to produced that
// version. Returns false when the event at that version recorded a different transition.
func replayEventMatches(ctx context.Context, tx *sql.Tx, s transitionSpec, version int64) (bool, error) {
	q := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s WHERE %s = ? AND version = ? AND from_state = ? AND to_state = ?",
		s.eventTable, s.eventParentCol)
	var n int
	if err := tx.QueryRowContext(ctx, q, s.id, version, s.from, s.to).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

func insertEvent(ctx context.Context, tx *sql.Tx, s transitionSpec, version int64) error {
	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s, event_type, from_state, to_state, version, message, payload_json, created_at) "+
			"VALUES (?, ?, ?, ?, ?, ?, ?, NOW(6))",
		s.eventTable, s.eventParentCol)

	var message any
	if s.message != "" {
		message = s.message
	}
	var payload any
	if len(s.payload) > 0 {
		payload = []byte(s.payload)
	}
	_, err := tx.ExecContext(ctx, insertSQL, s.id, s.eventType, s.from, s.to, version, message, payload)
	return err
}

func readRowState(ctx context.Context, tx *sql.Tx, table, idCol string, id int64) (state string, version int64, found bool, err error) {
	q := fmt.Sprintf("SELECT state, version FROM %s WHERE %s = ?", table, idCol)
	err = tx.QueryRowContext(ctx, q, id).Scan(&state, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, false, nil
	}
	if err != nil {
		return "", 0, false, err
	}
	return state, version, true, nil
}

// defaultEventType falls back to the target state name when no explicit event
// type is supplied, so an event always has a meaningful type.
func defaultEventType(eventType, fallback string) string {
	if eventType != "" {
		return eventType
	}
	return fallback
}
