package collector

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestDBHealthRecorderSuccess(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	mock.ExpectQuery("SELECT id FROM exchanges WHERE code").
		WithArgs("nobitex").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(7)))
	mock.ExpectExec("INSERT INTO exchange_health_current").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO exchange_health_samples").
		WillReturnResult(sqlmock.NewResult(1, 1))

	r := NewDBHealthRecorder(mockDB)
	r.RecordSuccess(context.Background(), "nobitex", 25*time.Millisecond)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDBHealthRecorderFailureAndIDCaching(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	// resolveID runs only once thanks to caching, even across two records.
	mock.ExpectQuery("SELECT id FROM exchanges WHERE code").
		WithArgs("wallex").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(3)))
	mock.ExpectExec("INSERT INTO exchange_health_current").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO exchange_health_samples").WillReturnResult(sqlmock.NewResult(1, 1))
	// second call: no SELECT (cached), just the two inserts.
	mock.ExpectExec("INSERT INTO exchange_health_current").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO exchange_health_samples").WillReturnResult(sqlmock.NewResult(1, 1))

	r := NewDBHealthRecorder(mockDB)
	r.RecordFailure(context.Background(), "wallex", errors.New("boom"))
	r.RecordFailure(context.Background(), "wallex", errors.New("boom again"))

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDBHealthRecorderUnknownExchangeSkips(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	// Unknown code: SELECT returns no rows -> recorder writes nothing further.
	mock.ExpectQuery("SELECT id FROM exchanges WHERE code").
		WithArgs("ghost").
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	r := NewDBHealthRecorder(mockDB)
	r.RecordSuccess(context.Background(), "ghost", time.Millisecond)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
