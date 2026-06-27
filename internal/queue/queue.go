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
func (q *Queue) Claim(ctx context.Context, exchangeID int64, claimedBy string, limit int, allowed []RequestType) ([]Claimed, error) {
	if limit <= 0 || len(allowed) == 0 {
		return nil, nil
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
		// In-flight count = CLAIMED + IN_FLIGHT (both occupy a concurrency slot).
		var inFlight int
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM exchange_requests WHERE exchange_id = ? AND status IN ('CLAIMED','IN_FLIGHT')",
			exchangeID).Scan(&inFlight); err != nil {
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
			  AND EXISTS (SELECT 1 FROM exchanges e WHERE e.id = er.exchange_id AND e.enabled = 1)
			ORDER BY er.priority ASC, er.id ASC
			LIMIT ?
			FOR UPDATE SKIP LOCKED`, typeList)
		args := append([]any{exchangeID}, typeArgs...)
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
func (q *Queue) MarkInFlight(ctx context.Context, id int64) error {
	_, err := q.db.ExecContext(ctx,
		"UPDATE exchange_requests SET status='IN_FLIGHT', inflight_at=NOW(6), updated_at=NOW(6) WHERE id=? AND status='CLAIMED'",
		id)
	return err
}

// MarkSucceeded moves a request to SUCCEEDED within the caller's tx (so it can be
// atomic with an order-state transition — rule #9).
func (q *Queue) MarkSucceeded(ctx context.Context, tx *sql.Tx, id int64, response json.RawMessage) error {
	_, err := tx.ExecContext(ctx,
		"UPDATE exchange_requests SET status='SUCCEEDED', response=?, last_error=NULL, updated_at=NOW(6) WHERE id=?",
		payloadOrNil(response), id)
	return err
}

// MarkFailed moves a request to FAILED (a definitive, non-retryable failure)
// within the caller's tx.
func (q *Queue) MarkFailed(ctx context.Context, tx *sql.Tx, id int64, cause string) error {
	_, err := tx.ExecContext(ctx,
		"UPDATE exchange_requests SET status='FAILED', last_error=?, updated_at=NOW(6) WHERE id=?",
		nullStr(cause), id)
	return err
}

// MarkDead moves a request to DEAD (exhausted/abandoned; e.g. an ambiguous
// mutating outcome) within the caller's tx. The owning order should be pushed to
// NEEDS_RECONCILE in the SAME tx by the caller.
func (q *Queue) MarkDead(ctx context.Context, tx *sql.Tx, id int64, cause string) error {
	_, err := tx.ExecContext(ctx,
		"UPDATE exchange_requests SET status='DEAD', last_error=?, updated_at=NOW(6) WHERE id=?",
		nullStr(cause), id)
	return err
}

// ScheduleRetry bumps retry_count and either schedules a backoff retry
// (RETRY_SCHEDULED) or, if max_retries is exhausted, moves the request to DEAD.
// Intended for READ-ONLY requests (and pre-send mutating failures). Returns the
// resulting status. Backoff is exponential, capped.
func (q *Queue) ScheduleRetry(ctx context.Context, id int64, cause string) (string, error) {
	var retryCount, maxRetries int
	if err := q.db.QueryRowContext(ctx,
		"SELECT retry_count, max_retries FROM exchange_requests WHERE id=?", id).Scan(&retryCount, &maxRetries); err != nil {
		return "", err
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
