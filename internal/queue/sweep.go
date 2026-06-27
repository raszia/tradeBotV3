package queue

import (
	"context"
	"database/sql"

	"v3TradeBot/internal/state"
)

// StuckResult summarises a SweepStuck run.
type StuckResult struct {
	RequeuedReadOnly int
	DeadMutating     int
}

// SweepStuck recovers requests that have been IN_FLIGHT longer than their timeout
// (+ graceSeconds) — i.e. the executor likely crashed after marking IN_FLIGHT.
// Recovery is CONSERVATIVE (rule #6):
//
//   - READ-ONLY requests are idempotent → re-queued (RETRY_SCHEDULED).
//   - MUTATING requests (PLACE/CANCEL) are NEVER blindly re-sent: their send
//     outcome is unknown, so the request is moved to DEAD and the owning order is
//     pushed to NEEDS_RECONCILE (in the same transaction) for the reconciler /
//     operator to resolve.
func (q *Queue) SweepStuck(ctx context.Context, graceSeconds int) (StuckResult, error) {
	rows, err := q.db.QueryContext(ctx, `
		SELECT id, request_type, order_id
		FROM exchange_requests
		WHERE status = 'IN_FLIGHT' AND inflight_at IS NOT NULL
		  AND inflight_at < (NOW(6) - INTERVAL (timeout_ms/1000 + ?) SECOND)`, graceSeconds)
	if err != nil {
		return StuckResult{}, err
	}
	type stuck struct {
		id      int64
		typ     RequestType
		orderID sql.NullInt64
	}
	var found []stuck
	for rows.Next() {
		var s stuck
		var t string
		if err := rows.Scan(&s.id, &t, &s.orderID); err != nil {
			rows.Close()
			return StuckResult{}, err
		}
		s.typ = RequestType(t)
		found = append(found, s)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return StuckResult{}, err
	}
	rows.Close()

	var res StuckResult
	for _, s := range found {
		if s.typ.IsMutating() {
			if err := q.deadMutatingStuck(ctx, s.id, s.orderID); err != nil {
				return res, err
			}
			res.DeadMutating++
			continue
		}
		// Read-only: safe to re-queue.
		if _, err := q.ScheduleRetry(ctx, s.id, "stuck IN_FLIGHT read-only re-queued"); err != nil {
			return res, err
		}
		res.RequeuedReadOnly++
	}
	return res, nil
}

// deadMutatingStuck marks a stuck mutating request DEAD and pushes its order to
// NEEDS_RECONCILE in one transaction. It NEVER re-sends.
func (q *Queue) deadMutatingStuck(ctx context.Context, requestID int64, orderID sql.NullInt64) error {
	return q.withTx(ctx, func(tx *sql.Tx) error {
		const cause = "stuck IN_FLIGHT: ambiguous mutating send outcome — needs reconcile"
		if orderID.Valid {
			var curState string
			var version int64
			if err := tx.QueryRowContext(ctx,
				"SELECT state, version FROM orders WHERE id=?", orderID.Int64).Scan(&curState, &version); err != nil {
				if err != sql.ErrNoRows {
					return err
				}
			} else {
				from := state.OrderState(curState)
				// Only transition if it can enter NEEDS_RECONCILE (non-terminal,
				// not already there). Otherwise leave the order as-is.
				if err := state.ValidateOrderTransition(from, state.OrderNeedsReconcile); err == nil {
					if _, err := state.ApplyOrderTransition(ctx, tx, state.OrderTransition{
						OrderID: orderID.Int64, From: from, To: state.OrderNeedsReconcile, Version: version,
						EventType: "stuck_inflight_reconcile", Reason: cause,
					}); err != nil {
						return err
					}
				}
			}
		}
		return q.MarkDead(ctx, tx, requestID, cause)
	})
}

func (q *Queue) withTx(ctx context.Context, fn func(*sql.Tx) error) (retErr error) {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if retErr != nil {
			_ = tx.Rollback()
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
