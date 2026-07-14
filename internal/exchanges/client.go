package exchanges

import (
	"context"
	"net/http"
	"time"

	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/execution"
)

// Credentials holds DECRYPTED API credentials for a private client. PR4 does not
// read these from the environment or implement decryption; they are supplied via
// a CredentialProvider whose real implementation (later PR) reads the
// AES-encrypted rows from exchange_credentials and decrypts them with the master
// key. Credentials must be masked in all logs/outputs (see mask.go).
type Credentials struct {
	APIKey     string
	APISecret  string
	Passphrase string
}

// CredentialProvider supplies credentials for an exchange on demand. Abstracting
// the source keeps private clients independent of how credentials are stored.
type CredentialProvider interface {
	Credentials(ctx context.Context, exchange string) (Credentials, error)
}

// PreparedMutation is an immutable, fully-prepared order/cancel whose ONLY remaining step is
// the actual network send. All definitely-pre-send work (credentials, auth/token, symbol
// normalization, payload + HTTP-request construction) has already happened. The executor marks
// the queue row IN_FLIGHT only immediately before calling Send, so a crash DURING preparation
// (e.g. a Bitpin token refresh) leaves the request CLAIMED/never-sent, never a false ambiguity
// (PR20 correction #2).
type PreparedMutation interface {
	// Send performs the single remaining network mutation (the order/cancel HTTP request).
	Send(ctx context.Context) (execution.OrderAck, error)
}

// MutationPreparer is implemented by private adapters that split preparation from the send.
// A preparation error is definitely-not-sent (wrap with execution.NotSent/NotSentPermanent); a
// rate limit observed while preparing (e.g. a Bitpin auth 429 or an auth 200 with
// X-RateLimit-Remaining:0) must FAIL preparation with a rate-limited ErrNotSent so the order
// endpoint is never called in the same invocation (PR20 correction #3). Adapters that do not
// implement this fall back to PlaceOrder/CancelOrder.
type MutationPreparer interface {
	PreparePlace(ctx context.Context, req execution.OrderRequest) (PreparedMutation, error)
	PrepareCancel(ctx context.Context, exchangeOrderID string) (PreparedMutation, error)
}

// StaticCredentialProvider is a trivial provider for tests and bootstrap. It is
// NOT how production credentials are supplied (those come from the encrypted DB).
type StaticCredentialProvider struct{ Creds Credentials }

// Credentials implements CredentialProvider.
func (p StaticCredentialProvider) Credentials(context.Context, string) (Credentials, error) {
	return p.Creds, nil
}

// ClientConfig is the per-exchange construction config. It carries connection
// settings only — never secrets (those come from a CredentialProvider). In
// production it is built from DB-backed exchange config (PR6); tests build it
// directly and inject HTTPClient/BaseURL to point at a fake server.
type ClientConfig struct {
	Code           string            // canonical exchange code, e.g. "nobitex"
	BaseURL        string            // REST base; override to point at a test server
	WSURL          string            // WebSocket base; override for tests
	Symbols        map[string]string // canonical BASE/QUOTE -> venue-native symbol
	ProxyURL       string
	LocalIP        string
	ClientTimeout  time.Duration
	RequestTimeout time.Duration
	HTTPClient     *http.Client       // optional injected client (tests); else built from the above
	Creds          CredentialProvider // required for private clients
	// RateLimitSink receives throttle signals seen on SUCCESSFUL responses (PR20 correction
	// #6) — exhausted-quota headers on an HTTP 200 must pause FUTURE requests without
	// affecting the successful result. Optional; nil disables the observation.
	RateLimitSink RateLimitSink
}

// Capabilities describes what an exchange adapter supports. Adapters return a
// typed ErrUnsupported (see Unsupported) for any operation whose flag is false,
// so the rest of the system can branch on capability rather than guessing.
type Capabilities struct {
	MarketMetadata  bool // GetMarkets
	OrderBookREST   bool // GetOrderBook
	OrderBookWS     bool // SubscribeOrderBook
	BalanceFetch    bool // GetBalances
	PlaceOrder      bool
	CancelByOrderID bool // CancelOrder by exchange order id
	FetchByOrderID  bool // GetOrder by exchange order id
	FetchOpenOrders bool // GetOpenOrders
	RecentFills     bool // GetRecentFills (where implemented)
	OrderUpdatesWS  bool // SubscribeOrderUpdates over WebSocket
	OrderStatusPoll bool // order status available via polling
	// ClientOrderID: the venue accepts a client-provided order id ON PLACEMENT. This is
	// DISTINCT from being able to look an order up by that id — see LookupByClientOrderID.
	ClientOrderID bool
	// LookupByClientOrderID: GetOrder can resolve an order BY its client order id. Only set
	// this when the adapter's GetOrder genuinely accepts a client id (e.g. Wallex, which keys
	// orders by client_id). An adapter that accepts a client id on placement but whose GetOrder
	// takes only the exchange order id must leave this FALSE — the ambiguous-place recovery must
	// never pass a client id into an endpoint that expects an exchange order id, and must never
	// treat a "not found" from such a venue as proof the order was not placed.
	LookupByClientOrderID bool
	// ReliableNotFound: a "not found" from GetOrder is a TRUTHFUL negative — the venue has no
	// eventual-consistency window in which an accepted order is briefly invisible. Almost always
	// FALSE for real venues (an order may be accepted but not yet queryable), so a "not found"
	// is ambiguous and must NOT be treated as proof of non-placement. Only when this is true may
	// the recovery, after bounded read-only retries still find nothing, classify the order as
	// provably-not-placed and fail it cleanly.
	ReliableNotFound bool
}

// ClientOrderLookup is an OPTIONAL interface a PrivateClient implements when it can resolve an
// order BY its client order id through a read-only API. This is what the ambiguous-place recovery
// and the reconciler use when the exchange order id is unknown — NEVER GetOrder (which takes an
// exchange order id on most venues). An adapter should implement it ONLY when the lookup uses a
// RELIABLE identifier (the venue's own client-order-id field, unique per user), so a recovered
// order is never the wrong one. `Capabilities.LookupByClientOrderID` must be true iff this is
// implemented. ErrOrderUnknown means "not found (yet)" — never proof the order was not placed.
type ClientOrderLookup interface {
	GetOrderByClientOrderID(ctx context.Context, clientOrderID string) (execution.OrderStatus, error)
}

// ClientOrderIDNormalizer is an OPTIONAL interface a PrivateClient may implement when it
// transforms the local client order id before sending it (truncation, length limits, encoding,
// prefixing). ClientOrderIDForSend returns the EXACT identifier the adapter will transmit for a
// given local id, so the order-executor can persist that value (client_order_id_sent) BEFORE the
// network call and recover the order by exactly what the venue received. Adapters that send the
// id verbatim need not implement it (the executor then persists the local id unchanged).
// Implementations MUST be deterministic and idempotent (ClientOrderIDForSend(ClientOrderIDForSend(x))
// == ClientOrderIDForSend(x)) so the adapter's own PlaceOrder re-normalization is a no-op.
type ClientOrderIDNormalizer interface {
	ClientOrderIDForSend(local string) string
}

// PublicClient is the read-only market-data surface. It requires NO credentials,
// so the collector and regime module can use it without ever touching private
// keys (rule #8: public and private clients are separated).
type PublicClient interface {
	Name() string
	Capabilities() Capabilities

	// GetMarkets returns the venue's tradable markets and their rules.
	GetMarkets(ctx context.Context) ([]NormalizedMarket, error)
	// GetOrderBook fetches a REST order-book snapshot for a canonical symbol.
	GetOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error)
	// SubscribeOrderBook streams normalized order-book updates for the given
	// canonical symbols until ctx is cancelled. Returns ErrUnsupported when the
	// venue has no order-book WebSocket.
	SubscribeOrderBook(ctx context.Context, symbols []string) (<-chan domain.OrderBook, error)
}

// PrivateClient is the authenticated trading/account surface. Only the
// order-executor, reconciler and balance-sync use it; it needs credentials.
type PrivateClient interface {
	Name() string
	Capabilities() Capabilities

	// GetBalances returns balances for all assets the account holds.
	GetBalances(ctx context.Context) ([]domain.Balance, error)
	// PlaceOrder submits an order. Order type / time-in-force come from the
	// request (owner-defined); this layer imposes no strategy.
	PlaceOrder(ctx context.Context, req execution.OrderRequest) (execution.OrderAck, error)
	// CancelOrder is a best-effort cancel by exchange order id.
	CancelOrder(ctx context.Context, exchangeOrderID string) error
	// GetOrder returns the current state of one order.
	GetOrder(ctx context.Context, exchangeOrderID string) (execution.OrderStatus, error)
	// GetOpenOrders returns open orders, optionally filtered by canonical symbol
	// (empty symbol = all).
	GetOpenOrders(ctx context.Context, symbol string) ([]execution.OrderStatus, error)
	// SubscribeOrderUpdates streams normalized order events until ctx is
	// cancelled. Returns ErrUnsupported for polling-only venues.
	SubscribeOrderUpdates(ctx context.Context) (<-chan execution.NormalizedOrderEvent, error)
}
