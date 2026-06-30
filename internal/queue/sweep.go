package queue

import (
	"context"
	"database/sql"

	"v3TradeBot/internal/state"
)

// StuckResult summarises a SweepStuck run.
type StuckResult struct {
	RequeuedClaimed  int // stale CLAIMED (never sent) reset to QUEUED
	RequeuedReadOnly int // stale read-only IN_FLIGHT rescheduled
	DeadMutating     int // stale mutating IN_FLIGHT dead-lettered + order NEEDS_RECONCILE
}

// SweepStuck recovers requests left behind by a crashed executor. Recovery is
// CONSERVATIVE (rule #6):
//
//   - Stale CLAIMED requests (claimed but never marked IN_FLIGHT → NEVER sent to the
//     exchange) are simply re-queued (status→QUEUED, claim cleared) so another executor
//     can take them. This is always safe — including for mutating PLACE/CANCEL — because
//     IN_FLIGHT (not CLAIMED) is the pre-send boundary.
//   - Stale IN_FLIGHT READ-ONLY requests are idempotent → re-queued (RETRY_SCHEDULED).
//   - Stale IN_FLIGHT MUTATING requests (PLACE/CANCEL) are NEVER blindly re-sent: their
//     send outcome is unknown, so the request is moved to DEAD and the owning order is
//     pushed to NEEDS_RECONCILE (same tx) for the reconciler / operator to resolve.
//
// "Stale" = older than its timeout_ms + graceSeconds.
func (q *Queue) SweepStuck(ctx context.Context, graceSeconds int) (StuckResult, error) {
	var res StuckResult

	// 1. Recover stale CLAIMED requests (pre-send → safe to requeue).
	requeuedClaimed, err := q.requeueStaleClaimed(ctx, graceSeconds)
	if err != nil {
		return res, err
	}
	res.RequeuedClaimed = requeuedClaimed

	// 2. Recover stale IN_FLIGHT requests.
	rows, err := q.db.QueryContext(ctx, `
		SELECT id, request_type, order_id
		FROM exchange_requests
		WHERE status = 'IN_FLIGHT' AND inflight_at IS NOT NULL
		  AND inflight_at < (NOW(6) - INTERVAL (timeout_ms/1000 + ?) SECOND)`, graceSeconds)
	if err != nil {
		return res, err
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
			return res, err
		}
		s.typ = RequestType(t)
		found = append(found, s)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return res, err
	}
	rows.Close()

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

// requeueStaleClaimed resets CLAIMED requests whose claim is older than their
// timeout_ms + graceSeconds back to QUEUED, clearing claimed_by/claimed_at. A CLAIMED
// request has NOT been sent to the exchange (IN_FLIGHT is the pre-send marker), so
// requeuing is ALWAYS safe — even for mutating PLACE/CANCEL — and needs no
// NEEDS_RECONCILE. Without this, a crash between Claim and MarkInFlight would strand the
// request in CLAIMED forever (and hold a per-exchange concurrency slot). Returns the count.
func (q *Queue) requeueStaleClaimed(ctx context.Context, graceSeconds int) (int, error) {
	res, err := q.db.ExecContext(ctx, `
		UPDATE exchange_requests
		SET status='QUEUED', claimed_by=NULL, claimed_at=NULL, updated_at=NOW(6)
		WHERE status='CLAIMED' AND claimed_at IS NOT NULL
		  AND claimed_at < (NOW(6) - INTERVAL (timeout_ms/1000 + ?) SECOND)`, graceSeconds)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
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
