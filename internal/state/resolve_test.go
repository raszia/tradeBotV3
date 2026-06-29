package state

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestApplyCycleResolutionSuccess(t *testing.T) {
	mockDB, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE cycles SET state").
		WithArgs("CANCELLED", int64(7), "NEEDS_RECONCILE", int64(5)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO cycle_state_events").
		WithArgs(int64(7), "operator_resolution", "NEEDS_RECONCILE", "CANCELLED", int64(6), "zero exposure", nil).
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, _ := mockDB.Begin()
	res, err := ApplyCycleResolution(context.Background(), tx, CycleTransition{
		CycleID: 7, From: CycleNeedsReconcile, To: CycleCancelled, Version: 5, Reason: "zero exposure",
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if res.NewVersion != 6 {
		t.Fatalf("new version = %d, want 6", res.NewVersion)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}

func TestApplyCycleResolutionRejectsNonReconcileFrom(t *testing.T) {
	mockDB, mock := newMock(t)
	mock.ExpectBegin() // no exec must run
	tx, _ := mockDB.Begin()
	_, err := ApplyCycleResolution(context.Background(), tx, CycleTransition{
		CycleID: 1, From: CycleBuyFilled, To: CycleCancelled, Version: 0, // not from NEEDS_RECONCILE
	})
	if !errors.Is(err, ErrNotReconcileResolution) {
		t.Fatalf("err = %v, want ErrNotReconcileResolution", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet (no exec): %v", err)
	}
}

func TestApplyCycleResolutionRejectsIllegalTarget(t *testing.T) {
	mockDB, mock := newMock(t)
	mock.ExpectBegin() // no exec must run
	tx, _ := mockDB.Begin()
	// NEEDS_RECONCILE -> BUY_SUBMITTED is NOT a permitted resolution target.
	_, err := ApplyCycleResolution(context.Background(), tx, CycleTransition{
		CycleID: 1, From: CycleNeedsReconcile, To: CycleBuySubmitted, Version: 0,
	})
	if !errors.Is(err, ErrNotReconcileResolution) {
		t.Fatalf("err = %v, want ErrNotReconcileResolution", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet (no exec): %v", err)
	}
}

func TestApplyOrderResolutionSuccess(t *testing.T) {
	mockDB, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE orders SET state").
		WithArgs("FILLED", int64(3), "NEEDS_RECONCILE", int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO order_events").
		WithArgs(int64(3), "operator_resolution", "NEEDS_RECONCILE", "FILLED", int64(3), "confirmed fill", nil).
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, _ := mockDB.Begin()
	if _, err := ApplyOrderResolution(context.Background(), tx, OrderTransition{
		OrderID: 3, From: OrderNeedsReconcile, To: OrderFilled, Version: 2, Reason: "confirmed fill",
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}

func TestApplyOrderResolutionRejectsNonReconcileFrom(t *testing.T) {
	mockDB, mock := newMock(t)
	mock.ExpectBegin()
	tx, _ := mockDB.Begin()
	if _, err := ApplyOrderResolution(context.Background(), tx, OrderTransition{
		OrderID: 1, From: OrderSubmitted, To: OrderFilled, Version: 0,
	}); !errors.Is(err, ErrNotReconcileResolution) {
		t.Fatalf("err = %v, want ErrNotReconcileResolution", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet (no exec): %v", err)
	}
}

func TestResolutionTargetWhitelists(t *testing.T) {
	// Spot-check the operator-exit whitelists so a future edit can't silently widen them.
	if !IsCycleResolutionTarget(CycleClosed) || !IsCycleResolutionTarget(CycleCancelled) || !IsCycleResolutionTarget(CycleBuyFilled) {
		t.Error("expected CLOSED/CANCELLED/BUY_FILLED to be cycle resolution targets")
	}
	if IsCycleResolutionTarget(CycleBuySubmitted) || IsCycleResolutionTarget(CycleNeedsReconcile) {
		t.Error("BUY_SUBMITTED / NEEDS_RECONCILE must NOT be cycle resolution targets")
	}
	if !IsOrderResolutionTarget(OrderFilled) || !IsOrderResolutionTarget(OrderCancelled) {
		t.Error("expected FILLED/CANCELLED to be order resolution targets")
	}
	if IsOrderResolutionTarget(OrderSubmitted) || IsOrderResolutionTarget(OrderNeedsReconcile) {
		t.Error("SUBMITTED / NEEDS_RECONCILE must NOT be order resolution targets")
	}
}
