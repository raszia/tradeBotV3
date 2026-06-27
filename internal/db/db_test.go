package db

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestOrDefaultHelpers(t *testing.T) {
	if got := orDefaultInt(0, 25); got != 25 {
		t.Errorf("orDefaultInt(0,25) = %d", got)
	}
	if got := orDefaultInt(5, 25); got != 5 {
		t.Errorf("orDefaultInt(5,25) = %d", got)
	}
	if got := orDefaultDur(0, 30*time.Minute); got != 30*time.Minute {
		t.Errorf("orDefaultDur(0,..) = %v", got)
	}
	if got := orDefaultDur(90, time.Minute); got != 90*time.Second {
		t.Errorf("orDefaultDur(90,..) = %v", got)
	}
}

func TestWithTxCommitsOnSuccess(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	store := NewFromDB(mockDB)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO t").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err = store.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, execErr := tx.ExecContext(context.Background(), "INSERT INTO t VALUES (1)")
		return execErr
	})
	if err != nil {
		t.Fatalf("WithTx returned error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestWithTxRollsBackOnError(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	store := NewFromDB(mockDB)

	wantErr := errors.New("boom")
	mock.ExpectBegin()
	mock.ExpectRollback()

	err = store.WithTx(context.Background(), func(tx *sql.Tx) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("WithTx error = %v, want %v", err, wantErr)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestWithTxRollsBackOnPanic(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	store := NewFromDB(mockDB)

	mock.ExpectBegin()
	mock.ExpectRollback()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic to propagate out of WithTx")
		}
		// The transaction must have been rolled back before the panic propagated.
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations after panic: %v", err)
		}
	}()

	_ = store.WithTx(context.Background(), func(tx *sql.Tx) error {
		panic("fn blew up")
	})
}
