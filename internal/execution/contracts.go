// Package execution holds the normalized, exchange-agnostic order-execution
// contracts that flow between the order-executor and the concrete exchange
// adapters in internal/exchanges. It is a redesign of the sibling system's
// execution types: prices/amounts are quote-native (no misleading *IRT names),
// and order type / time-in-force are plain fields set by the (owner-defined)
// trading logic — this layer imposes NO strategy (no forced IOC/market/post-only).
package execution

import (
	"errors"
	"time"

	"github.com/shopspring/decimal"
)

// Side of an order.
const (
	SideBuy  = "buy"
	SideSell = "sell"
)

// Common order types and times-in-force. These are NOT exhaustive and NOT
// imposed: OrderRequest.OrderType / TimeInForce carry whatever the owner's logic
// chooses; adapters translate them to the venue's native parameters (and return
// ErrUnsupported via internal/exchanges when a venue can't honor a value).
const (
	OrderTypeLimit  = "limit"
	OrderTypeMarket = "market"

	TIFGTC = "GTC" // good-till-cancelled
	TIFIOC = "IOC" // immediate-or-cancel
	TIFFOK = "FOK" // fill-or-kill
)

// NormalizedOrderState is the exchange-reported status of an order, normalized
// to one vocabulary across all venues. It is distinct from the database
// lifecycle (internal/state.OrderState): the order-status processor (PR10) maps
// these onto state transitions.
type NormalizedOrderState string

const (
	StateNew               NormalizedOrderState = "new"                // accepted, no fill yet
	StateOpen              NormalizedOrderState = "open"               // resting on the book
	StatePartiallyFilled   NormalizedOrderState = "partially_filled"   // partially filled, still open
	StateFilled            NormalizedOrderState = "filled"             // fully filled
	StateCanceled          NormalizedOrderState = "canceled"           // cancelled, no fill
	StatePartiallyCanceled NormalizedOrderState = "partially_canceled" // cancelled after a partial fill
	StateRejected          NormalizedOrderState = "rejected"           // rejected by the venue
	StateExpired           NormalizedOrderState = "expired"            // expired (e.g. IOC/FOK remainder)
	StateUnknown           NormalizedOrderState = "unknown"            // could not be determined
)

// OrderRequest is the input to PlaceOrder. ClientOrderID is the system's
// internal idempotency key; it is sent to the venue ONLY when the adapter's
// capabilities report SupportsClientOrderID (otherwise it is kept internally and
// recovery is conservative — see PROJECT_ARCHITECTURE.md §17).
type OrderRequest struct {
	ClientOrderID string
	Symbol        string          // canonical BASE/QUOTE
	Side          string          // SideBuy | SideSell
	Quantity      decimal.Decimal // base units
	LimitPrice    decimal.Decimal // quote currency; snapped to the venue's PriceTick by the caller
	OrderType     string          // owner-set, e.g. OrderTypeLimit
	TimeInForce   string          // owner-set, e.g. TIFGTC; may be empty
}

// OrderAck is the venue's acknowledgement of an order submission. Fills may
// continue after the ack and must be observed via GetOrder / order-update events.
type OrderAck struct {
	ExchangeOrderID string
	ClientOrderID   string
	Symbol          string
	Side            string
	RequestedQty    decimal.Decimal
	LimitPrice      decimal.Decimal
	Status          NormalizedOrderState
	RawStatus       string
	FilledQty       decimal.Decimal // filled as of ack time
	AvgPrice        decimal.Decimal // VWAP of the filled portion (quote)
	ExecutedQuote   decimal.Decimal // quote spent/received so far
	Fee             decimal.Decimal // fee amount in FeeAsset
	FeeAsset        string
	Active          bool // still working on the venue
	AckedAt         time.Time
	Raw             string // raw venue response (already secret-masked before logging)
}

// OrderStatus is the current state of one order as reported by the venue.
type OrderStatus struct {
	ExchangeOrderID string
	ClientOrderID   string
	Symbol          string
	Side            string
	Status          NormalizedOrderState
	IntendedQty     decimal.Decimal
	FilledQty       decimal.Decimal
	RemainingQty    decimal.Decimal
	AvgPrice        decimal.Decimal // quote
	ExecutedQuote   decimal.Decimal
	Fee             decimal.Decimal
	FeeAsset        string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	Raw             string
}

// Fill is one trade fill.
type Fill struct {
	ExchangeOrderID string
	ClientOrderID   string
	ExchangeFillID  string
	Symbol          string
	Side            string
	Quantity        decimal.Decimal
	Price           decimal.Decimal // quote
	QuoteAmount     decimal.Decimal
	Fee             decimal.Decimal
	FeeAsset        string
	FilledAt        time.Time
}

// Sentinel errors that adapters return where applicable; the order-executor and
// reconciler branch on them (use errors.Is). Adapters wrap a NormalizedAPIError
// (internal/exchanges) where a richer HTTP-level error is useful.
var (
	ErrOrderUnknown        = errors.New("execution: order unknown to exchange")
	ErrInsufficientBalance = errors.New("execution: insufficient balance on exchange")
	ErrRateLimited         = errors.New("execution: exchange rate limited request")
	ErrAuthFailed          = errors.New("execution: exchange authentication failed")
	ErrAckTimeout          = errors.New("execution: order ack timed out")
	ErrFillTimeout         = errors.New("execution: order fill timed out")
)
