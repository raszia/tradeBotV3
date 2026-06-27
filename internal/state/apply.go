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
	// Replayed is true when the row was already in the target state, so the
	// transition was a safe no-op and NO duplicate event row was written. This is
	// how crash/duplicate-event replay is handled idempotently.
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
	case curState == s.to:
		// Already in the target state: treat as an idempotent replay no-op. We do
		// NOT write another event (that would duplicate the version'd history).
		return Result{NewVersion: curVersion, Replayed: true}, nil
	case curVersion != s.version:
		// Someone else advanced the row.
		return Result{}, ErrStaleVersion
	default:
		// Version matches but the state is neither `from` nor `to`: the caller's
		// expected `from` is wrong / inconsistent.
		return Result{}, ErrStateMismatch
	}
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
