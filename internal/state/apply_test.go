package state

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"v3TradeBot/internal/db"
)

func newMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mockDB.Close() })
	return mockDB, mock
}

func TestApplyCycleTransitionSuccessWritesEvent(t *testing.T) {
	mockDB, mock := newMock(t)

	mock.ExpectBegin()
	// CAS update guarded by (id, state, version).
	mock.ExpectExec("UPDATE cycles SET state").
		WithArgs("SIGNAL_DETECTED", int64(42), "NEW", int64(3)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// Event row carries from/to/new-version/message/payload — asserted exactly.
	mock.ExpectExec("INSERT INTO cycle_state_events").
		WithArgs(int64(42), "signal", "NEW", "SIGNAL_DETECTED", int64(4), "buy signal", []byte(`{"k":1}`)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, err := mockDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	res, err := ApplyCycleTransition(context.Background(), tx, CycleTransition{
		CycleID: 42, From: CycleNew, To: CycleSignalDetected, Version: 3,
		EventType: "signal", Reason: "buy signal", Payload: []byte(`{"k":1}`),
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if res.NewVersion != 4 || res.Replayed {
		t.Fatalf("result = %+v, want {NewVersion:4 Replayed:false}", res)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}

func TestApplyCycleTransitionInvalidDoesNotTouchDB(t *testing.T) {
	mockDB, mock := newMock(t)
	mock.ExpectBegin() // tx is created, but no exec must happen

	tx, err := mockDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	_, err = ApplyCycleTransition(context.Background(), tx, CycleTransition{
		CycleID: 1, From: CycleNew, To: CycleClosed, Version: 0, // NEW->CLOSED is illegal
	})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("err = %v, want ErrInvalidTransition", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet (no exec should have run): %v", err)
	}
}

// TestApplyCycleTransitionReplayExactVersion: a row already in the target state at EXACTLY
// expected_version+1, whose recorded event is this same transition, is an idempotent replay
// no-op (no second event written). expected=3 -> the exact replay is at version 4.
func TestApplyCycleTransitionReplayExactVersion(t *testing.T) {
	mockDB, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE cycles SET state").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT state, version FROM cycles").
		WithArgs(int64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"state", "version"}).AddRow("SIGNAL_DETECTED", int64(4)))
	// The event at version 4 records THIS transition (NEW->SIGNAL_DETECTED).
	mock.ExpectQuery("SELECT COUNT").
		WithArgs(int64(42), int64(4), "NEW", "SIGNAL_DETECTED").
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))

	tx, _ := mockDB.Begin()
	res, err := ApplyCycleTransition(context.Background(), tx, CycleTransition{
		CycleID: 42, From: CycleNew, To: CycleSignalDetected, Version: 3,
	})
	if err != nil {
		t.Fatalf("exact replay should be a no-op success, got %v", err)
	}
	if !res.Replayed || res.NewVersion != 4 {
		t.Fatalf("result = %+v, want replayed at version 4", res)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet (no event insert on replay): %v", err)
	}
}

// TestApplyCycleTransitionStaleTargetHigherVersion: a row in the target state but at a version
// HIGHER than expected_version+1 is a STALE transition, not a replay -> ErrStaleVersion.
// expected=3 (replay would only be at v4), but the row is already at v10.
func TestApplyCycleTransitionStaleTargetHigherVersion(t *testing.T) {
	mockDB, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE cycles SET state").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT state, version FROM cycles").
		WithArgs(int64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"state", "version"}).AddRow("SIGNAL_DETECTED", int64(10)))
	// No event-match query must run: the strict version check rejects before it.

	tx, _ := mockDB.Begin()
	_, err := ApplyCycleTransition(context.Background(), tx, CycleTransition{
		CycleID: 42, From: CycleNew, To: CycleSignalDetected, Version: 3,
	})
	if !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("err = %v, want ErrStaleVersion (stale target at higher version, not replay)", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}

func TestApplyCycleTransitionStaleVersion(t *testing.T) {
	mockDB, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE cycles SET state").WillReturnResult(sqlmock.NewResult(0, 0))
	// Still in `from`, but at a different version than the caller observed.
	mock.ExpectQuery("SELECT state, version FROM cycles").
		WillReturnRows(sqlmock.NewRows([]string{"state", "version"}).AddRow("NEW", int64(99)))

	tx, _ := mockDB.Begin()
	_, err := ApplyCycleTransition(context.Background(), tx, CycleTransition{
		CycleID: 42, From: CycleNew, To: CycleSignalDetected, Version: 3,
	})
	if !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("err = %v, want ErrStaleVersion", err)
	}
}

func TestApplyCycleTransitionStateMismatch(t *testing.T) {
	mockDB, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE cycles SET state").WillReturnResult(sqlmock.NewResult(0, 0))
	// Version matches but the state is neither `from` nor `to`.
	mock.ExpectQuery("SELECT state, version FROM cycles").
		WillReturnRows(sqlmock.NewRows([]string{"state", "version"}).AddRow("BUY_FILLED", int64(3)))

	tx, _ := mockDB.Begin()
	_, err := ApplyCycleTransition(context.Background(), tx, CycleTransition{
		CycleID: 42, From: CycleNew, To: CycleSignalDetected, Version: 3,
	})
	if !errors.Is(err, ErrStateMismatch) {
		t.Fatalf("err = %v, want ErrStateMismatch", err)
	}
}

func TestApplyCycleTransitionUnknownRow(t *testing.T) {
	mockDB, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE cycles SET state").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT state, version FROM cycles").
		WillReturnRows(sqlmock.NewRows([]string{"state", "version"})) // no rows

	tx, _ := mockDB.Begin()
	_, err := ApplyCycleTransition(context.Background(), tx, CycleTransition{
		CycleID: 999, From: CycleNew, To: CycleSignalDetected, Version: 3,
	})
	if !errors.Is(err, ErrUnknownRow) {
		t.Fatalf("err = %v, want ErrUnknownRow", err)
	}
}

func TestApplyRollsBackWhenEventInsertFails(t *testing.T) {
	mockDB, mock := newMock(t)
	store := db.NewFromDB(mockDB)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE cycles SET state").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO cycle_state_events").WillReturnError(errors.New("insert boom"))
	mock.ExpectRollback()

	err := store.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, e := ApplyCycleTransition(context.Background(), tx, CycleTransition{
			CycleID: 1, From: CycleNew, To: CycleSignalDetected, Version: 0,
		})
		return e
	})
	if err == nil {
		t.Fatal("expected WithTx to return the event-insert error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet (must roll back): %v", err)
	}
}

func TestApplyRollsBackWhenUpdateFails(t *testing.T) {
	mockDB, mock := newMock(t)
	store := db.NewFromDB(mockDB)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE cycles SET state").WillReturnError(errors.New("update boom"))
	mock.ExpectRollback()

	err := store.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, e := ApplyCycleTransition(context.Background(), tx, CycleTransition{
			CycleID: 1, From: CycleNew, To: CycleSignalDetected, Version: 0,
		})
		return e
	})
	if err == nil {
		t.Fatal("expected WithTx to return the update error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet (must roll back): %v", err)
	}
}

func TestApplyOrderTransitionSuccess(t *testing.T) {
	mockDB, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE orders SET state").
		WithArgs("REGISTERED", int64(7), "NEW", int64(0)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO order_events").
		WithArgs(int64(7), "register", "NEW", "REGISTERED", int64(1), nil, nil).
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, _ := mockDB.Begin()
	res, err := ApplyOrderTransition(context.Background(), tx, OrderTransition{
		OrderID: 7, From: OrderNew, To: OrderRegistered, Version: 0, EventType: "register",
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if res.NewVersion != 1 || res.Replayed {
		t.Fatalf("result = %+v", res)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}

// TestApplyOrderTransitionReplayExactVersion: order replay is accepted only at exactly
// expected_version+1 with a matching event. expected=3 -> exact replay at version 4.
func TestApplyOrderTransitionReplayExactVersion(t *testing.T) {
	mockDB, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE orders SET state").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT state, version FROM orders").
		WithArgs(int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"state", "version"}).AddRow("REGISTERED", int64(4)))
	mock.ExpectQuery("SELECT COUNT").
		WithArgs(int64(7), int64(4), "NEW", "REGISTERED").
		WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1))

	tx, _ := mockDB.Begin()
	res, err := ApplyOrderTransition(context.Background(), tx, OrderTransition{
		OrderID: 7, From: OrderNew, To: OrderRegistered, Version: 3,
	})
	if err != nil {
		t.Fatalf("exact order replay should succeed, got %v", err)
	}
	if !res.Replayed || res.NewVersion != 4 {
		t.Fatalf("result = %+v, want replayed at version 4", res)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet (no event insert on replay): %v", err)
	}
}

// TestApplyOrderTransitionStaleTargetHigherVersion: an order in the target state but at a
// version higher than expected_version+1 is stale, not a replay -> ErrStaleVersion.
func TestApplyOrderTransitionStaleTargetHigherVersion(t *testing.T) {
	mockDB, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE orders SET state").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT state, version FROM orders").
		WithArgs(int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"state", "version"}).AddRow("REGISTERED", int64(10)))

	tx, _ := mockDB.Begin()
	_, err := ApplyOrderTransition(context.Background(), tx, OrderTransition{
		OrderID: 7, From: OrderNew, To: OrderRegistered, Version: 3,
	})
	if !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("err = %v, want ErrStaleVersion (stale target at higher version)", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}
