package executor

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/queue"
)

func TestIsRetryable(t *testing.T) {
	retryable := []error{
		execution.ErrRateLimited, execution.ErrAckTimeout,
		context.DeadlineExceeded, context.Canceled,
		&exchanges.NormalizedAPIError{Category: exchanges.CatServer},
		&exchanges.NormalizedAPIError{Category: exchanges.CatRateLimit},
		&exchanges.NormalizedAPIError{Retryable: true},
	}
	for _, e := range retryable {
		if !isRetryable(e) {
			t.Errorf("isRetryable(%v) = false, want true", e)
		}
	}
	notRetryable := []error{
		errors.New("plain"),
		execution.ErrInsufficientBalance,
		&exchanges.NormalizedAPIError{Category: exchanges.CatBadRequest},
	}
	for _, e := range notRetryable {
		if isRetryable(e) {
			t.Errorf("isRetryable(%v) = true, want false", e)
		}
	}
}

func TestIsDefiniteRejection(t *testing.T) {
	definite := []error{
		execution.ErrInsufficientBalance,
		execution.ErrAuthFailed,
		&exchanges.NormalizedAPIError{Category: exchanges.CatBadRequest},
		&exchanges.NormalizedAPIError{Category: exchanges.CatAuth},
	}
	for _, e := range definite {
		if !isDefiniteRejection(e) {
			t.Errorf("isDefiniteRejection(%v) = false, want true", e)
		}
	}
	ambiguous := []error{
		errors.New("timeout"),
		execution.ErrAckTimeout,
		&exchanges.NormalizedAPIError{Category: exchanges.CatServer},
		&exchanges.NormalizedAPIError{Category: exchanges.CatNetwork},
		&exchanges.NormalizedAPIError{Category: exchanges.CatTimeout},
	}
	for _, e := range ambiguous {
		if isDefiniteRejection(e) {
			t.Errorf("isDefiniteRejection(%v) = true (must be ambiguous), want false", e)
		}
	}
}

func TestAllowedTypesGating(t *testing.T) {
	// Live execution OFF -> only read-only types are claimed (mutating never sent).
	off := New(nil, nil, nil, nil, Config{AllowLiveExecution: false})
	if !sameTypes(off.allowedTypes(), queue.ReadOnlyTypes) {
		t.Errorf("live off allowedTypes = %v, want read-only only", off.allowedTypes())
	}
	for _, ty := range off.allowedTypes() {
		if ty.IsMutating() {
			t.Errorf("read-only-only set contains mutating type %s", ty)
		}
	}
	// Live execution ON -> all types.
	on := New(nil, nil, nil, nil, Config{AllowLiveExecution: true})
	if len(on.allowedTypes()) != len(queue.AllTypes) {
		t.Errorf("live on allowedTypes = %v, want all", on.allowedTypes())
	}
}

// TestNoDirectSendMethod guards rule #1: the executor exposes no method that
// sends an order directly — its only exported method is Run.
func TestNoDirectSendMethod(t *testing.T) {
	typ := reflect.TypeOf(&Executor{})
	exported := []string{}
	for i := 0; i < typ.NumMethod(); i++ {
		exported = append(exported, typ.Method(i).Name)
	}
	if len(exported) != 1 || exported[0] != "Run" {
		t.Fatalf("executor exported methods = %v; want only [Run] (no direct-send path)", exported)
	}
}

func sameTypes(a, b []queue.RequestType) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
