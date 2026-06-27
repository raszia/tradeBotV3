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
	ClientOrderID   bool // venue accepts a client-provided order id
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
