package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"

	"v3TradeBot/internal/clock"
)

func newQ(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *Queue) {
	t.Helper()
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mockDB.Close() })
	return mockDB, mock, New(mockDB, clock.NewManual(time.Unix(1_700_000_000, 0).UTC()))
}

func TestRequestTypeIsMutating(t *testing.T) {
	if !TypePlaceOrder.IsMutating() || !TypeCancelOrder.IsMutating() {
		t.Error("place/cancel must be mutating")
	}
	for _, ro := range ReadOnlyTypes {
		if ro.IsMutating() {
			t.Errorf("%s must be read-only", ro)
		}
	}
}

func TestEnqueueSuccess(t *testing.T) {
	mockDB, mock, q := newQ(t)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO exchange_requests").
		WillReturnResult(sqlmock.NewResult(42, 1))
	mock.ExpectCommit()

	tx, _ := mockDB.Begin()
	id, err := q.Enqueue(context.Background(), tx, Request{
		ExchangeID: 1, Type: TypePlaceOrder, CycleID: ptr64(7), OrderID: ptr64(9), IdempotencyKey: "k1", Payload: json.RawMessage(`{"a":1}`),
	})
	if err != nil || id != 42 {
		t.Fatalf("Enqueue = %d, %v", id, err)
	}
	_ = tx.Commit()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEnqueueDuplicateIdempotencyKeyRejected(t *testing.T) {
	mockDB, mock, q := newQ(t)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO exchange_requests").
		WillReturnError(&mysql.MySQLError{Number: 1062, Message: "Duplicate entry"})

	tx, _ := mockDB.Begin()
	_, err := q.Enqueue(context.Background(), tx, Request{ExchangeID: 1, Type: TypePlaceOrder, CycleID: ptr64(7), OrderID: ptr64(9), IdempotencyKey: "dup"})
	if !errors.Is(err, ErrDuplicateIdempotencyKey) {
		t.Fatalf("expected ErrDuplicateIdempotencyKey, got %v", err)
	}
}

func TestMarkInFlight(t *testing.T) {
	mockDB, mock, q := newQ(t)
	mock.ExpectExec("UPDATE exchange_requests SET status='IN_FLIGHT'").
		WithArgs(int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := q.MarkInFlight(context.Background(), 7); err != nil {
		t.Fatal(err)
	}
	_ = mockDB
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMarkSucceededFailedDead(t *testing.T) {
	mockDB, mock, q := newQ(t)
	ctx := context.Background()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE exchange_requests SET status='SUCCEEDED'").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE exchange_requests SET status='FAILED'").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE exchange_requests SET status='DEAD'").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, _ := mockDB.Begin()
	if err := q.MarkSucceeded(ctx, tx, 1, json.RawMessage(`{"ok":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := q.MarkFailed(ctx, tx, 2, "rejected"); err != nil {
		t.Fatal(err)
	}
	if err := q.MarkDead(ctx, tx, 3, "ambiguous"); err != nil {
		t.Fatal(err)
	}
	_ = tx.Commit()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestScheduleRetryIncrementsThenDead(t *testing.T) {
	mockDB, mock, q := newQ(t)
	ctx := context.Background()
	_ = mockDB

	// A READ-ONLY request (GET_BALANCE): retry_count 0, max 5 -> RETRY_SCHEDULED.
	cols := []string{"retry_count", "max_retries", "request_type", "order_id"}
	mock.ExpectQuery("SELECT retry_count, max_retries, request_type, order_id FROM exchange_requests").
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows(cols).AddRow(0, 5, "GET_BALANCE", nil))
	mock.ExpectExec("UPDATE exchange_requests SET status='RETRY_SCHEDULED'").
		WithArgs(1, sqlmock.AnyArg(), "boom", int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	st, err := q.ScheduleRetry(ctx, 1, "boom")
	if err != nil || st != string(StatusRetryScheduled) {
		t.Fatalf("ScheduleRetry = %q, %v", st, err)
	}

	// retry_count 5, max 5 -> exhausted -> DEAD.
	mock.ExpectQuery("SELECT retry_count, max_retries, request_type, order_id FROM exchange_requests").
		WithArgs(int64(2)).
		WillReturnRows(sqlmock.NewRows(cols).AddRow(5, 5, "GET_BALANCE", nil))
	mock.ExpectExec("UPDATE exchange_requests SET status='DEAD'").
		WithArgs(6, "boom", int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	st, err = q.ScheduleRetry(ctx, 2, "boom")
	if err != nil || st != string(StatusDead) {
		t.Fatalf("ScheduleRetry(exhausted) = %q, %v", st, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestBackoffExponentialCapped(t *testing.T) {
	if backoff(0) != retryBaseDelay {
		t.Errorf("backoff(0) = %v", backoff(0))
	}
	if backoff(1) != 2*retryBaseDelay {
		t.Errorf("backoff(1) = %v", backoff(1))
	}
	if backoff(100) != retryMaxDelay {
		t.Errorf("backoff(100) should cap at %v, got %v", retryMaxDelay, backoff(100))
	}
}

func ptr64(v int64) *int64 { return &v }
