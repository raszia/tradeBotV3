// Package queue is the database-backed priority queue for exchange requests. The
// trade-engine ENQUEUES requests (inside its cycle-creation transaction); the
// order-executor CLAIMS and sends them. This decouples decision-making from API
// calls and guarantees a request is persisted before it is ever sent.
//
// Why the DB and not Redis: queue state is AUTHORITATIVE recovery state (what we
// were about to send / have sent). It must survive a Redis flush/restart, so it
// lives in MariaDB, never in Redis.
package queue

import (
	"encoding/json"

	"v3TradeBot/internal/state"
)

// RequestType classifies an exchange request. MUTATING requests change exchange
// state (place/cancel) and get a CONSERVATIVE retry policy; read-only requests
// are idempotent and may be retried freely.
type RequestType string

const (
	TypePlaceOrder    RequestType = "PLACE_ORDER"
	TypeCancelOrder   RequestType = "CANCEL_ORDER"
	TypeGetOrder      RequestType = "GET_ORDER"
	TypeGetOpenOrders RequestType = "GET_OPEN_ORDERS"
	TypeGetBalance    RequestType = "GET_BALANCE"
)

// IsMutating reports whether sending this request changes exchange state. The
// distinction drives retry/recovery policy (rule #6/#7): a mutating request whose
// send outcome is unknown is NEVER blindly re-sent.
func (t RequestType) IsMutating() bool {
	return t == TypePlaceOrder || t == TypeCancelOrder
}

// ReadOnlyTypes is the set of non-mutating request types.
var ReadOnlyTypes = []RequestType{TypeGetOrder, TypeGetOpenOrders, TypeGetBalance}

// AllTypes is every request type.
var AllTypes = []RequestType{TypePlaceOrder, TypeCancelOrder, TypeGetOrder, TypeGetOpenOrders, TypeGetBalance}

// Statuses reuse the fixed system-wide enum from internal/state so schema, queue,
// executor, and dashboard share one vocabulary (rule #5).
const (
	StatusQueued         = state.RequestQueued
	StatusClaimed        = state.RequestClaimed
	StatusInFlight       = state.RequestInFlight
	StatusSucceeded      = state.RequestSucceeded
	StatusFailed         = state.RequestFailed
	StatusRetryScheduled = state.RequestRetryScheduled
	StatusDead           = state.RequestDead
)

// Request is the input to Enqueue.
type Request struct {
	ExchangeID     int64
	Symbol         string
	CycleID        *int64
	OrderID        *int64
	Type           RequestType
	Priority       int16 // lower = more urgent
	Payload        json.RawMessage
	TimeoutMS      int
	MaxRetries     int
	IdempotencyKey string // internal idempotency key; UNIQUE in the DB
}

// Claimed is a request the executor has claimed for processing.
type Claimed struct {
	ID             int64
	ExchangeID     int64
	ExchangeCode   string
	Symbol         string
	CycleID        *int64
	OrderID        *int64
	Type           RequestType
	Priority       int16
	Payload        json.RawMessage
	TimeoutMS      int
	RetryCount     int
	MaxRetries     int
	IdempotencyKey string
}

// Timeout returns the request's send timeout, defaulting to 10s.
func (c Claimed) Timeout() int {
	if c.TimeoutMS <= 0 {
		return 10000
	}
	return c.TimeoutMS
}
