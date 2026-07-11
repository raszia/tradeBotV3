package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"v3TradeBot/internal/clock"
)

// ErrDuplicateIdempotencyKey is returned by Enqueue when a request with the same
// idempotency_key already exists. The UNIQUE constraint enforces this at the DB
// level; Enqueue surfaces it so callers (the trade-engine) treat a duplicate as a
// rejected request rather than a second send (rule #8).
var ErrDuplicateIdempotencyKey = errors.New("queue: duplicate idempotency key")

// ErrRequestNotClaimed is returned by MarkInFlight when the guarded CLAIMED→IN_FLIGHT
// update matches zero rows (the request is not — or no longer — CLAIMED). It is
// safety-critical: the executor MUST treat this as "do not send to the exchange".
var ErrRequestNotClaimed = errors.New("queue: request not in CLAIMED state")

// ErrRequestNotActive is returned by the terminal Mark* methods when their guarded
// update matches zero rows — the request already moved to a terminal status
// (DEAD/FAILED/SUCCEEDED) or away from CLAIMED/IN_FLIGHT. Refusing the update stops a
// late worker from clobbering a newer status (e.g. overwriting DEAD with SUCCEEDED).
var ErrRequestNotActive = errors.New("queue: request not in a markable (CLAIMED/IN_FLIGHT) state")

// Backoff parameters for retry scheduling.
const (
	retryBaseDelay = 500 * time.Millisecond
	retryMaxDelay  = 60 * time.Second
)

// Queue is the DB-backed exchange-request queue.
type Queue struct {
	db    *sql.DB
	clock clock.Clock
}

// New builds a Queue. A nil clock uses the system clock.
func New(db *sql.DB, clk clock.Clock) *Queue {
	if clk == nil {
		clk = clock.NewSystem()
	}
	return &Queue{db: db, clock: clk}
}

// Enqueue inserts a request as QUEUED inside the caller's transaction (so it is
// atomic with the cycle/order writes that justify it — rule: registered before
// sent). A duplicate idempotency_key returns ErrDuplicateIdempotencyKey.
func (q *Queue) Enqueue(ctx context.Context, tx *sql.Tx, r Request) (int64, error) {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO exchange_requests
		  (exchange_id, symbol, cycle_id, order_id, request_type, priority, status,
		   payload, timeout_ms, max_retries, idempotency_key)
		VALUES (?, ?, ?, ?, ?, ?, 'QUEUED', ?, ?, ?, ?)`,
		r.ExchangeID, nullStr(r.Symbol), r.CycleID, r.OrderID, string(r.Type), r.Priority,
		payloadOrEmpty(r.Payload), defaultTimeout(r.TimeoutMS), defaultMaxRetries(r.MaxRetries), r.IdempotencyKey)
	if err != nil {
		var myErr *mysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1062 { // duplicate key
			return 0, ErrDuplicateIdempotencyKey
		}
		return 0, err
	}
	return res.LastInsertId()
}

// Claim atomically moves up to `limit` eligible requests for one exchange from
// QUEUED / (RETRY_SCHEDULED && due) to CLAIMED, respecting priority, the
// per-exchange concurrency limit, the exchange's enabled flag, and the allowed
// request types. It returns the claimed requests.
//
// Cross-process safety (rule #3): the count + select + update run on a single
// pinned connection guarded by a per-exchange advisory lock (GET_LOCK), so the
// per-exchange limit holds even with multiple executor processes. FOR UPDATE SKIP
// LOCKED additionally prevents two claimers ever touching the same row.
// EnqueueScheduled inserts a request that is NOT claimable until delay has elapsed,
// by writing it as RETRY_SCHEDULED with next_retry_at = now + delay (the claim query
// only picks up RETRY_SCHEDULED rows whose next_retry_at <= NOW). This models the
// simulated-IOC wait (place → wait → cancel → status) and sell repricing intervals as
// queued work, so an executor worker is never blocked sleeping. Runs in the caller's
// tx for atomicity with the state transition that triggers it. A duplicate
// idempotency_key returns ErrDuplicateIdempotencyKey.
//
// RETRY_SCHEDULED convention (kept deliberately — no separate SCHEDULED enum value):
// the status RETRY_SCHEDULED is overloaded and disambiguated by retry_count:
//
//   - retry_count == 0  →  a SCHEDULED NEXT STEP (this method): a planned future
//     request (e.g. the simulated-IOC cancel/status), NOT a failure.
//   - retry_count  > 0  →  an ACTUAL RETRY (ScheduleRetry): the request previously
//     ran and is being retried after a transient error, with backoff.
//
// Dashboard/reporting MUST use retry_count to tell the two apart so scheduled
// simulated-IOC / reprice steps are not surfaced as failed retries. EnqueueScheduled
// always leaves retry_count at its default 0.
func (q *Queue) EnqueueScheduled(ctx context.Context, tx *sql.Tx, r Request, delay time.Duration) (int64, error) {
	if delay < 0 {
		delay = 0
	}
	nextAt := q.clock.Now().Add(delay).UTC()
	res, err := tx.ExecContext(ctx, `
		INSERT INTO exchange_requests
		  (exchange_id, symbol, cycle_id, order_id, request_type, priority, status,
		   payload, timeout_ms, max_retries, idempotency_key, next_retry_at)
		VALUES (?, ?, ?, ?, ?, ?, 'RETRY_SCHEDULED', ?, ?, ?, ?, ?)`,
		r.ExchangeID, nullStr(r.Symbol), r.CycleID, r.OrderID, string(r.Type), r.Priority,
		payloadOrEmpty(r.Payload), defaultTimeout(r.TimeoutMS), defaultMaxRetries(r.MaxRetries), r.IdempotencyKey, nextAt)
	if err != nil {
		var myErr *mysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1062 {
			return 0, ErrDuplicateIdempotencyKey
		}
		return 0, err
	}
	return res.LastInsertId()
}

// dryRun scopes the claim to the EXECUTION MODE: nil = no filter; &true = only requests
// whose owning cycle has dry_run=1 (a dry-run executor); &false = only dry_run=0 (a live
// executor). Requests with no cycle_id (e.g. bare balance polls) are claimable in any mode.
// This keeps a dry-run executor from ever claiming a real cycle's request and vice-versa —
// the first of two guards (the second is the executor's pre-send check).
func (q *Queue) Claim(ctx context.Context, exchangeID int64, claimedBy string, limit int, allowed []RequestType, dryRun *bool) ([]Claimed, error) {
	if limit <= 0 || len(allowed) == 0 {
		return nil, nil
	}
	// The mode predicate (applied to BOTH the in-flight count and the select, so a dry-run
	// and a live executor sharing an exchange never contend on each other's concurrency slots).
	dryClause := ""
	var dryArg []any
	if dryRun != nil {
		// A cycle whose dry_run matches this executor's mode, OR a CYCLE-LESS request that is
		// READ-ONLY. A mutating (PLACE/CANCEL) request is NEVER claimable without a mode-matching
		// cycle: it could not otherwise be classified dry-run vs live nor recovered (PR19 round 3
		// #6; the DB CHECK also makes a cycle-less mutating row impossible).
		dryClause = " AND (EXISTS (SELECT 1 FROM cycles c WHERE c.id = er.cycle_id AND c.dry_run = ?) OR (er.cycle_id IS NULL AND er.request_type NOT IN ('PLACE_ORDER','CANCEL_ORDER')))"
		dryArg = []any{boolToInt(*dryRun)}
	}
	conn, err := q.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	lockName := fmt.Sprintf("v3tb_exreq_claim_%d", exchangeID)
	got, err := acquireLock(ctx, conn, lockName, 5)
	if err != nil {
		return nil, err
	}
	if !got {
		// Another claimer holds the per-exchange lock; skip this round (not an error).
		return nil, nil
	}
	defer releaseLock(ctx, conn, lockName)

	var claimedIDs []int64
	err = withConnTx(ctx, conn, func(tx *sql.Tx) error {
		// In-flight count = CLAIMED + IN_FLIGHT (both occupy a concurrency slot), scoped to
		// the same execution mode so dry-run and live executors don't consume each other's slots.
		var inFlight int
		countArgs := append([]any{exchangeID}, dryArg...)
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM exchange_requests er WHERE er.exchange_id = ? AND er.status IN ('CLAIMED','IN_FLIGHT')"+dryClause,
			countArgs...).Scan(&inFlight); err != nil {
			return err
		}
		slots := limit - inFlight
		if slots <= 0 {
			return nil
		}

		typeList, typeArgs := inClause(allowed)
		selectSQL := fmt.Sprintf(`
			SELECT er.id
			FROM exchange_requests er
			WHERE er.exchange_id = ?
			  AND er.request_type IN (%s)
			  AND (er.status = 'QUEUED' OR (er.status = 'RETRY_SCHEDULED' AND er.next_retry_at <= NOW(6)))
			  AND EXISTS (SELECT 1 FROM exchanges e WHERE e.id = er.exchange_id AND e.enabled = 1)%s
			ORDER BY er.priority ASC, er.id ASC
			LIMIT ?
			FOR UPDATE SKIP LOCKED`, typeList, dryClause)
		args := append([]any{exchangeID}, typeArgs...)
		args = append(args, dryArg...)
		args = append(args, slots)

		rows, err := tx.QueryContext(ctx, selectSQL, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			claimedIDs = append(claimedIDs, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(claimedIDs) == 0 {
			return nil
		}

		idList, idArgs := int64InClause(claimedIDs)
		updSQL := fmt.Sprintf(
			"UPDATE exchange_requests SET status='CLAIMED', claimed_by=?, claimed_at=NOW(6), updated_at=NOW(6) WHERE id IN (%s)", idList)
		updArgs := append([]any{claimedBy}, idArgs...)
		_, err = tx.ExecContext(ctx, updSQL, updArgs...)
		return err
	})
	if err != nil {
		return nil, err
	}
	if len(claimedIDs) == 0 {
		return nil, nil
	}
	return q.loadClaimed(ctx, claimedIDs)
}

// loadClaimed re-reads full claimed rows (with the exchange code).
func (q *Queue) loadClaimed(ctx context.Context, ids []int64) ([]Claimed, error) {
	idList, idArgs := int64InClause(ids)
	query := fmt.Sprintf(`
		SELECT er.id, er.exchange_id, e.code, er.symbol, er.cycle_id, er.order_id, er.request_type,
		       er.priority, er.payload, er.timeout_ms, er.retry_count, er.max_retries, er.idempotency_key
		FROM exchange_requests er
		JOIN exchanges e ON e.id = er.exchange_id
		WHERE er.id IN (%s)
		ORDER BY er.priority ASC, er.id ASC`, idList)
	rows, err := q.db.QueryContext(ctx, query, idArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Claimed
	for rows.Next() {
		var (
			c       Claimed
			symbol  sql.NullString
			payload []byte
			reqType string
		)
		if err := rows.Scan(&c.ID, &c.ExchangeID, &c.ExchangeCode, &symbol, &c.CycleID, &c.OrderID,
			&reqType, &c.Priority, &payload, &c.TimeoutMS, &c.RetryCount, &c.MaxRetries, &c.IdempotencyKey); err != nil {
			return nil, err
		}
		c.Symbol = symbol.String
		c.Type = RequestType(reqType)
		c.Payload = json.RawMessage(payload)
		out = append(out, c)
	}
	return out, rows.Err()
}

// MarkInFlight moves a CLAIMED request to IN_FLIGHT and stamps inflight_at. The
// executor MUST call this (and have it committed) BEFORE sending a mutating
// request, so a crash leaves a recoverable IN_FLIGHT marker (rule #6).
//
// It is GUARDED on status='CLAIMED' and verifies exactly one row changed: if the row
// is no longer CLAIMED (already swept, requeued, or never claimed) it returns
// ErrRequestNotClaimed and the executor must NOT send to the exchange — preventing a
// stale/duplicate mutating send.
func (q *Queue) MarkInFlight(ctx context.Context, id int64) error {
	res, err := q.db.ExecContext(ctx,
		"UPDATE exchange_requests SET status='IN_FLIGHT', inflight_at=NOW(6), updated_at=NOW(6) WHERE id=? AND status='CLAIMED'",
		id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: id=%d", ErrRequestNotClaimed, id)
	}
	return nil
}

// MarkSucceeded moves a request to SUCCEEDED within the caller's tx (so it can be
// atomic with an order-state transition — rule #9). GUARDED: only a request still in
// CLAIMED (read-only success) or IN_FLIGHT (mutating success) is advanced; a row that
// already moved to a CONFLICTING terminal status (e.g. DEAD/FAILED) yields
// ErrRequestNotActive and the tx rolls back. Re-applying the SAME status is an
// idempotent no-op (safe for crash-recovery reprocessing).
func (q *Queue) MarkSucceeded(ctx context.Context, tx *sql.Tx, id int64, response json.RawMessage) error {
	return q.markTerminal(ctx, tx, id, "SUCCEEDED",
		"UPDATE exchange_requests SET status='SUCCEEDED', response=?, last_error=NULL, updated_at=NOW(6) WHERE id=? AND status IN ('CLAIMED','IN_FLIGHT')",
		payloadOrNil(response), id)
}

// MarkFailed moves a request to FAILED (a definitive, non-retryable failure — pre-send
// from CLAIMED, or a definite exchange rejection from IN_FLIGHT) within the caller's tx.
// Guarded the same way as MarkSucceeded (conflicting status → ErrRequestNotActive;
// same-status → idempotent no-op).
func (q *Queue) MarkFailed(ctx context.Context, tx *sql.Tx, id int64, cause string) error {
	return q.markTerminal(ctx, tx, id, "FAILED",
		"UPDATE exchange_requests SET status='FAILED', last_error=?, updated_at=NOW(6) WHERE id=? AND status IN ('CLAIMED','IN_FLIGHT')",
		nullStr(cause), id)
}

// MarkDead moves a request to DEAD (exhausted/abandoned; e.g. an ambiguous mutating
// outcome) within the caller's tx. The owning order should be pushed to NEEDS_RECONCILE
// in the SAME tx by the caller. Guarded the same way (conflicting status →
// ErrRequestNotActive; re-marking an already-DEAD row → idempotent no-op).
func (q *Queue) MarkDead(ctx context.Context, tx *sql.Tx, id int64, cause string) error {
	return q.markTerminal(ctx, tx, id, "DEAD",
		"UPDATE exchange_requests SET status='DEAD', last_error=?, updated_at=NOW(6) WHERE id=? AND status IN ('CLAIMED','IN_FLIGHT')",
		nullStr(cause), id)
}

// markTerminal runs a guarded terminal UPDATE (WHERE status IN ('CLAIMED','IN_FLIGHT'))
// and disambiguates a zero-row result by re-reading the current status in the SAME tx:
//
//   - exactly one row changed   → applied (nil);
//   - already in `target`       → idempotent no-op (nil) — safe re-processing, no double work;
//   - any OTHER status          → ErrRequestNotActive — a late worker must NOT overwrite a
//     newer/conflicting terminal status (e.g. SUCCEEDED clobbering DEAD).
func (q *Queue) markTerminal(ctx context.Context, tx *sql.Tx, id int64, target, update string, args ...any) error {
	res, err := tx.ExecContext(ctx, update, args...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	var cur string
	switch err := tx.QueryRowContext(ctx, "SELECT status FROM exchange_requests WHERE id=?", id).Scan(&cur); {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("%w: id=%d (no such request)", ErrRequestNotActive, id)
	case err != nil:
		return err
	}
	if cur == target {
		return nil // already in the target terminal status — idempotent no-op
	}
	return fmt.Errorf("%w: id=%d is %s, refusing to set %s", ErrRequestNotActive, id, cur, target)
}

// ScheduleRetry bumps retry_count and either schedules a backoff retry
// (RETRY_SCHEDULED) or, if max_retries is exhausted, moves the request to DEAD.
// Intended for READ-ONLY requests (and pre-send mutating failures). Returns the
// resulting status. Backoff is exponential, capped.
func (q *Queue) ScheduleRetry(ctx context.Context, id int64, cause string) (string, error) {
	var retryCount, maxRetries int
	var reqType string
	var orderID sql.NullInt64
	if err := q.db.QueryRowContext(ctx,
		"SELECT retry_count, max_retries, request_type, order_id FROM exchange_requests WHERE id=?", id).Scan(&retryCount, &maxRetries, &reqType, &orderID); err != nil {
		return "", err
	}
	// DEFENSE-IN-DEPTH (PR26): a MUTATING request must NEVER be blindly retried — once it
	// has been claimed we cannot prove it was not sent. Dead-letter it + push its order to
	// NEEDS_RECONCILE instead. No current caller passes a mutating request here (all four
	// call sites are read-only/GET_*); this guards a future caller mistake from re-sending.
	if RequestType(reqType).IsMutating() {
		if err := q.deadMutatingStuck(ctx, id, orderID); err != nil {
			return "", err
		}
		return string(StatusDead), nil
	}
	next := retryCount + 1
	if next > maxRetries {
		_, err := q.db.ExecContext(ctx,
			"UPDATE exchange_requests SET status='DEAD', retry_count=?, last_error=?, updated_at=NOW(6) WHERE id=?",
			next, nullStr(cause), id)
		return string(StatusDead), err
	}
	delay := backoff(retryCount)
	nextAt := q.clock.Now().Add(delay).UTC()
	_, err := q.db.ExecContext(ctx,
		"UPDATE exchange_requests SET status='RETRY_SCHEDULED', retry_count=?, next_retry_at=?, last_error=?, updated_at=NOW(6) WHERE id=?",
		next, nextAt, nullStr(cause), id)
	return string(StatusRetryScheduled), err
}

// backoff returns the delay before the (retryCount+1)-th attempt: base*2^n capped.
func backoff(retryCount int) time.Duration {
	d := retryBaseDelay << retryCount
	if d > retryMaxDelay || d <= 0 { // d<=0 guards overflow on large counts
		return retryMaxDelay
	}
	return d
}

// --- small helpers ---

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func inClause(types []RequestType) (string, []any) {
	placeholders := make([]string, len(types))
	args := make([]any, len(types))
	for i, t := range types {
		placeholders[i] = "?"
		args[i] = string(t)
	}
	return strings.Join(placeholders, ","), args
}

func int64InClause(ids []int64) (string, []any) {
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	return strings.Join(placeholders, ","), args
}

func acquireLock(ctx context.Context, conn *sql.Conn, name string, timeoutSec int) (bool, error) {
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", name, timeoutSec).Scan(&got); err != nil {
		return false, err
	}
	return got.Valid && got.Int64 == 1, nil
}

func releaseLock(ctx context.Context, conn *sql.Conn, name string) {
	_, _ = conn.ExecContext(ctx, "DO RELEASE_LOCK(?)", name)
}

func withConnTx(ctx context.Context, conn *sql.Conn, fn func(*sql.Tx) error) (retErr error) {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			_ = tx.Rollback()
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func payloadOrEmpty(p json.RawMessage) any {
	if len(p) == 0 {
		return "{}"
	}
	return []byte(p)
}

func payloadOrNil(p json.RawMessage) any {
	if len(p) == 0 {
		return nil
	}
	return []byte(p)
}

func defaultTimeout(ms int) int {
	if ms <= 0 {
		return 10000
	}
	return ms
}

func defaultMaxRetries(n int) int {
	if n < 0 {
		return 0
	}
	if n == 0 {
		return 5
	}
	return n
}
