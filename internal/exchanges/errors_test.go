package exchanges

import (
	"errors"
	"testing"

	"v3TradeBot/internal/execution"
)

func TestUnsupportedWrapsSentinel(t *testing.T) {
	err := Unsupported("nobitex", "SubscribeOrderUpdates")
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Unsupported should wrap ErrUnsupported, got %v", err)
	}
	if got := err.Error(); got == "" {
		t.Error("empty error message")
	}
}

func TestNormalizedAPIErrorUnwrapsToExecutionSentinel(t *testing.T) {
	apiErr := &NormalizedAPIError{
		Exchange:   "wallex",
		Op:         "PlaceOrder",
		StatusCode: 429,
		Category:   CatRateLimit,
		Retryable:  true,
		Message:    "too many requests",
		Err:        execution.ErrRateLimited,
	}
	// errors.Is must see through to the wrapped execution sentinel.
	if !errors.Is(apiErr, execution.ErrRateLimited) {
		t.Error("NormalizedAPIError should unwrap to execution.ErrRateLimited")
	}
	if apiErr.Error() == "" {
		t.Error("empty error string")
	}
}

func TestNormalizedAPIErrorNoWrap(t *testing.T) {
	apiErr := &NormalizedAPIError{Exchange: "x", Op: "GetOrder", StatusCode: 500, Category: CatServer}
	// No wrapped sentinel: errors.Is against a sentinel is false, but it is still
	// a usable error.
	if errors.Is(apiErr, execution.ErrRateLimited) {
		t.Error("should not match an unrelated sentinel")
	}
}
