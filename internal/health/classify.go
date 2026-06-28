// Package health is the exchange health monitor (PR14). It runs READ-ONLY probes
// (public market-data, and authenticated read-only probes when credentials exist),
// classifies the result into a small fixed vocabulary, and records it in MariaDB
// (exchange_health_current + exchange_health_samples). It makes NO trading calls
// (no PlaceOrder/CancelOrder), creates no cycles/orders, and mutates no trading
// queue — the Monitor only ever invokes caller-supplied read-only probe funcs.
//
// The Recorder here is the canonical, reusable health-reporting helper other
// components (collector/executor/balance-sync/reconciler) can adopt to report health
// consistently.
package health

import (
	"context"
	"encoding/json"
	"errors"

	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
)

// Status is the normalized exchange-health vocabulary (no adapter-invented names).
type Status string

const (
	StatusHealthy     Status = "HEALTHY"
	StatusDegraded    Status = "DEGRADED"     // reachable but erroring
	StatusUnavailable Status = "UNAVAILABLE"  // cannot reach / server down / timeout
	StatusAuthFailed  Status = "AUTH_FAILED"  // authentication rejected
	StatusRateLimited Status = "RATE_LIMITED" // throttled by the venue
	StatusUnknown     Status = "UNKNOWN"      // no probe / could not determine
)

// Category is the normalized error-category vocabulary.
type Category string

const (
	CatNone            Category = ""
	CatTimeout         Category = "timeout"
	CatNetwork         Category = "network"
	Cat5xx             Category = "exchange_5xx"
	Cat4xx             Category = "exchange_4xx"
	CatAuth            Category = "auth"
	CatRateLimit       Category = "rate_limit"
	CatUnsupported     Category = "unsupported"
	CatInvalidResponse Category = "invalid_response"
	CatUnknown         Category = "unknown"
)

// Classify maps a probe error to a normalized (Status, Category). nil → HEALTHY.
// context.Canceled is treated as a shutdown signal, not a health verdict (UNKNOWN);
// the Monitor does not record it.
func Classify(err error) (Status, Category) {
	if err == nil {
		return StatusHealthy, CatNone
	}
	switch {
	case errors.Is(err, context.Canceled):
		return StatusUnknown, CatUnknown
	case errors.Is(err, execution.ErrAuthFailed):
		return StatusAuthFailed, CatAuth
	case errors.Is(err, execution.ErrRateLimited):
		return StatusRateLimited, CatRateLimit
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, execution.ErrAckTimeout), errors.Is(err, execution.ErrFillTimeout):
		return StatusUnavailable, CatTimeout
	case errors.Is(err, exchanges.ErrUnsupported):
		return StatusDegraded, CatUnsupported
	}
	// Malformed venue response.
	var synErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &synErr) || errors.As(err, &typeErr) {
		return StatusDegraded, CatInvalidResponse
	}
	// Normalized HTTP-level error from an adapter.
	var apiErr *exchanges.NormalizedAPIError
	if errors.As(err, &apiErr) {
		switch apiErr.Category {
		case exchanges.CatAuth:
			return StatusAuthFailed, CatAuth
		case exchanges.CatRateLimit:
			return StatusRateLimited, CatRateLimit
		case exchanges.CatTimeout:
			return StatusUnavailable, CatTimeout
		case exchanges.CatNetwork:
			return StatusUnavailable, CatNetwork
		case exchanges.CatServer:
			return StatusUnavailable, Cat5xx
		case exchanges.CatBadRequest, exchanges.CatNotFound, exchanges.CatInsufficientBalance:
			return StatusDegraded, Cat4xx
		}
		return StatusDegraded, CatUnknown
	}
	return StatusDegraded, CatUnknown
}
