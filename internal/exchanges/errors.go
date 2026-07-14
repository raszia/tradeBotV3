package exchanges

import (
	"errors"
	"fmt"
)

// ErrUnsupported is returned (wrapped) when an adapter is asked to perform an
// operation its venue does not support. Callers branch with errors.Is.
var ErrUnsupported = errors.New("exchanges: operation not supported by this exchange")

// Unsupported builds a wrapped ErrUnsupported naming the exchange and operation.
func Unsupported(exchange, op string) error {
	return fmt.Errorf("%w: %s does not support %s", ErrUnsupported, exchange, op)
}

// ErrorCategory classifies a venue error for retry/health decisions.
type ErrorCategory string

const (
	CatUnknown             ErrorCategory = "unknown"
	CatNetwork             ErrorCategory = "network"
	CatTimeout             ErrorCategory = "timeout"
	CatAuth                ErrorCategory = "auth"
	CatRateLimit           ErrorCategory = "rate_limit"
	CatNotFound            ErrorCategory = "not_found"
	CatInsufficientBalance ErrorCategory = "insufficient_balance"
	CatBadRequest          ErrorCategory = "bad_request"
	CatServer              ErrorCategory = "server"
)

// NormalizedAPIError is the structured, exchange-agnostic error an adapter
// produces for an HTTP/venue failure. It wraps a sentinel from internal/execution
// (e.g. ErrRateLimited) where the failure is classifiable, so callers can use
// errors.Is on either this type or the sentinel. Message/Raw must already be
// secret-masked before construction.
type NormalizedAPIError struct {
	Exchange   string
	Op         string
	StatusCode int
	Code       string // venue-specific error code, if any
	Message    string
	Category   ErrorCategory
	Retryable  bool
	Err        error // wrapped sentinel (execution.Err*), if classified
	// RateLimit carries structured throttle metadata when Category == CatRateLimit
	// (PR20 correction #6): the venue-provided wait, where the signal came from, and —
	// per exchange + operation, from a documented contract only — whether the request
	// was DEFINITELY rejected before execution. nil for non-rate-limit errors.
	RateLimit *RateLimitInfo
}

func (e *NormalizedAPIError) Error() string {
	return fmt.Sprintf("exchange %s op=%s status=%d code=%s category=%s retryable=%t: %s",
		e.Exchange, e.Op, e.StatusCode, e.Code, e.Category, e.Retryable, e.Message)
}

// Unwrap exposes the wrapped sentinel so errors.Is(err, execution.ErrRateLimited)
// works on a *NormalizedAPIError.
func (e *NormalizedAPIError) Unwrap() error { return e.Err }
