// Package simexec is the SIMULATED exchange client for dry-run trading mode (PR19). It
// implements exchanges.PrivateClient with NO network I/O whatsoever — it never reaches
// a real exchange, so a dry-run system exercises the full internal lifecycle (signal →
// cycle → lock → queue → executor → order processing → sell → close → reconcile) with
// deterministic, configurable simulated outcomes and zero real exposure.
//
// It is wired ONLY when the bootstrap execution mode is "dry_run"; the safe default
// ("off") wires no clients at all.
package simexec

import (
	"context"
	"fmt"
	"sync"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
)

// Scenario selects the simulated outcome of an order's lifecycle.
type Scenario string

const (
	FullFill     Scenario = "full_fill"     // buy/sell fully fills
	PartialFill  Scenario = "partial_fill"  // half fills, remainder cancelled
	ZeroFill     Scenario = "zero_fill"     // nothing fills (clean no-fill)
	Ambiguous    Scenario = "ambiguous"     // GetOrder is unknown -> NEEDS_RECONCILE
	Rejected     Scenario = "rejected"      // PlaceOrder is definitively rejected
	PlaceTimeout Scenario = "place_timeout" // PlaceOrder ambiguous (maybe placed)
	CancelRace   Scenario = "cancel_race"   // cancel raced a FULL fill (status=filled)
)

// Client is a simulated exchanges.PrivateClient. It records each placed order so
// GetOrder can return a consistent status. Safe for concurrent use.
type Client struct {
	code     string
	scenario Scenario
	mu       sync.Mutex
	orders   map[string]execution.OrderRequest // exchange_order_id -> original request
}

// New builds a simulated client for an exchange code with a default scenario.
func New(code string, scenario Scenario) *Client {
	if scenario == "" {
		scenario = FullFill
	}
	return &Client{code: code, scenario: scenario, orders: map[string]execution.OrderRequest{}}
}

func (c *Client) Name() string { return c.code }

// Capabilities advertises client-order-id + fetch-by-order-id so the executor and
// reconciler take their normal (non-blind) paths.
func (c *Client) Capabilities() exchanges.Capabilities {
	return exchanges.Capabilities{PlaceOrder: true, ClientOrderID: true, FetchByOrderID: true}
}

// GetBalances returns a simulated, generous balance (read-only; never real).
func (c *Client) GetBalances(context.Context) ([]domain.Balance, error) {
	return []domain.Balance{{Asset: "USDT", Available: decimal.NewFromInt(1_000_000), Total: decimal.NewFromInt(1_000_000)}}, nil
}

// PlaceOrder simulates a submission. A Rejected scenario returns a definite rejection
// (no order placed); a PlaceTimeout returns an ambiguous ack timeout. Otherwise it
// records the order and returns an ack with a synthetic exchange order id.
func (c *Client) PlaceOrder(_ context.Context, req execution.OrderRequest) (execution.OrderAck, error) {
	switch c.scenario {
	case Rejected:
		return execution.OrderAck{}, &exchanges.NormalizedAPIError{Exchange: c.code, Op: "PlaceOrder", Category: exchanges.CatBadRequest, Message: "simulated rejection"}
	case PlaceTimeout:
		return execution.OrderAck{}, execution.ErrAckTimeout
	}
	exoid := "SIM-" + req.ClientOrderID
	if exoid == "SIM-" {
		c.mu.Lock()
		exoid = fmt.Sprintf("SIM-%s-%d", c.code, len(c.orders)+1)
		c.mu.Unlock()
	}
	c.mu.Lock()
	c.orders[exoid] = req
	c.mu.Unlock()
	return execution.OrderAck{
		ExchangeOrderID: exoid, ClientOrderID: req.ClientOrderID, Symbol: req.Symbol, Side: req.Side,
		RequestedQty: req.Quantity, LimitPrice: req.LimitPrice, Status: execution.StateOpen,
	}, nil
}

// CancelOrder always succeeds in simulation (the real outcome is decided by GetOrder).
func (c *Client) CancelOrder(context.Context, string) error { return nil }

// GetOrder returns the simulated final status per the scenario, using the recorded
// requested quantity + limit price for fill amounts/prices.
func (c *Client) GetOrder(_ context.Context, exchangeOrderID string) (execution.OrderStatus, error) {
	c.mu.Lock()
	req, ok := c.orders[exchangeOrderID]
	c.mu.Unlock()
	if !ok {
		return execution.OrderStatus{}, execution.ErrOrderUnknown
	}
	st := execution.OrderStatus{
		ExchangeOrderID: exchangeOrderID, ClientOrderID: req.ClientOrderID, Symbol: req.Symbol, Side: req.Side,
		IntendedQty: req.Quantity, AvgPrice: req.LimitPrice, Liquidity: "maker",
	}
	half := req.Quantity.Div(decimal.NewFromInt(2))
	switch c.scenario {
	case FullFill, CancelRace:
		st.Status = execution.StateFilled
		st.FilledQty = req.Quantity
		st.RemainingQty = decimal.Zero
		st.ExecutedQuote = req.Quantity.Mul(req.LimitPrice)
	case PartialFill:
		st.Status = execution.StateCanceled // remainder cancelled after a partial fill
		st.FilledQty = half
		st.RemainingQty = req.Quantity.Sub(half)
		st.ExecutedQuote = half.Mul(req.LimitPrice)
	case ZeroFill:
		st.Status = execution.StateCanceled
		st.FilledQty = decimal.Zero
		st.RemainingQty = req.Quantity
	case Ambiguous:
		return execution.OrderStatus{}, execution.ErrOrderUnknown
	default:
		st.Status = execution.StateFilled
		st.FilledQty = req.Quantity
		st.ExecutedQuote = req.Quantity.Mul(req.LimitPrice)
	}
	return st, nil
}

// GetOpenOrders returns nothing (simulation resolves via GetOrder).
func (c *Client) GetOpenOrders(context.Context, string) ([]execution.OrderStatus, error) {
	return nil, nil
}

// SubscribeOrderUpdates is unsupported (the dry-run flow is poll-based like the venues).
func (c *Client) SubscribeOrderUpdates(context.Context) (<-chan execution.NormalizedOrderEvent, error) {
	return nil, exchanges.Unsupported(c.code, "SubscribeOrderUpdates")
}

// Compile-time assertion: a simulated client satisfies the full private interface.
var _ exchanges.PrivateClient = (*Client)(nil)
