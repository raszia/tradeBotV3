package exchanges

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/execution"
)

// FakePrivateClient is an in-memory PrivateClient for tests (and, later, the
// dry-run mode). It records placed orders, supports cancel/get/open-orders, and
// can be told to auto-fill on placement. It makes NO network calls, so it is
// safe in automated tests (rule #3). It is NOT a real adapter.
//
// Capability flags default to "fully capable" but can be overridden to simulate
// venues that lack a capability (e.g. SupportsClientOrderID=false) so callers can
// test the conservative-recovery paths.
type FakePrivateClient struct {
	code string

	mu       sync.Mutex
	seq      int64
	orders   map[string]*execution.OrderStatus
	balances []domain.Balance
	updates  chan execution.NormalizedOrderEvent

	// AutoFill, when true, marks every placed order fully filled at its limit
	// price immediately.
	AutoFill bool
	// Caps overrides the reported capabilities (zero value = a sensible default).
	Caps *Capabilities
}

// NewFakePrivateClient builds a fake for the given exchange code.
func NewFakePrivateClient(code string) *FakePrivateClient {
	return &FakePrivateClient{
		code:    code,
		orders:  map[string]*execution.OrderStatus{},
		updates: make(chan execution.NormalizedOrderEvent, 64),
	}
}

func (f *FakePrivateClient) Name() string { return f.code }

func (f *FakePrivateClient) Capabilities() Capabilities {
	if f.Caps != nil {
		return *f.Caps
	}
	return Capabilities{
		BalanceFetch: true, PlaceOrder: true, CancelByOrderID: true,
		FetchByOrderID: true, FetchOpenOrders: true, RecentFills: true,
		OrderUpdatesWS: true, OrderStatusPoll: true, ClientOrderID: true,
	}
}

// SetBalances replaces the fake's balances.
func (f *FakePrivateClient) SetBalances(b []domain.Balance) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.balances = b
}

func (f *FakePrivateClient) GetBalances(context.Context) ([]domain.Balance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]domain.Balance, len(f.balances))
	copy(out, f.balances)
	return out, nil
}

func (f *FakePrivateClient) PlaceOrder(_ context.Context, req execution.OrderRequest) (execution.OrderAck, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	id := fmt.Sprintf("%s-%d", f.code, f.seq)

	status := execution.StateOpen
	filled := decimal.Zero
	avg := decimal.Zero
	if f.AutoFill {
		status = execution.StateFilled
		filled = req.Quantity
		avg = req.LimitPrice
	}
	now := time.Now()
	st := &execution.OrderStatus{
		ExchangeOrderID: id,
		ClientOrderID:   req.ClientOrderID,
		Symbol:          req.Symbol,
		Side:            req.Side,
		Status:          status,
		IntendedQty:     req.Quantity,
		FilledQty:       filled,
		RemainingQty:    req.Quantity.Sub(filled),
		AvgPrice:        avg,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	f.orders[id] = st
	ack := execution.OrderAck{
		ExchangeOrderID: id,
		ClientOrderID:   req.ClientOrderID,
		Symbol:          req.Symbol,
		Side:            req.Side,
		RequestedQty:    req.Quantity,
		LimitPrice:      req.LimitPrice,
		Status:          status,
		FilledQty:       filled,
		AvgPrice:        avg,
		Active:          status != execution.StateFilled,
		AckedAt:         now,
	}
	f.emit(execution.EventFromStatus(f.code, *st))
	return ack, nil
}

func (f *FakePrivateClient) CancelOrder(_ context.Context, exchangeOrderID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.orders[exchangeOrderID]
	if !ok {
		return execution.ErrOrderUnknown
	}
	if st.Status == execution.StateFilled {
		return nil // nothing to cancel
	}
	if st.FilledQty.IsPositive() {
		st.Status = execution.StatePartiallyCanceled
	} else {
		st.Status = execution.StateCanceled
	}
	st.UpdatedAt = time.Now()
	f.emit(execution.EventFromStatus(f.code, *st))
	return nil
}

func (f *FakePrivateClient) GetOrder(_ context.Context, exchangeOrderID string) (execution.OrderStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.orders[exchangeOrderID]
	if !ok {
		return execution.OrderStatus{}, execution.ErrOrderUnknown
	}
	return *st, nil
}

func (f *FakePrivateClient) GetOpenOrders(_ context.Context, symbol string) ([]execution.OrderStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []execution.OrderStatus
	for _, st := range f.orders {
		open := st.Status == execution.StateOpen || st.Status == execution.StateNew || st.Status == execution.StatePartiallyFilled
		if !open {
			continue
		}
		if symbol != "" && st.Symbol != symbol {
			continue
		}
		out = append(out, *st)
	}
	return out, nil
}

func (f *FakePrivateClient) SubscribeOrderUpdates(ctx context.Context) (<-chan execution.NormalizedOrderEvent, error) {
	return f.updates, nil
}

// emit pushes an event to the updates channel (non-blocking). Caller holds f.mu.
func (f *FakePrivateClient) emit(ev execution.NormalizedOrderEvent) {
	ev.Source = execution.SourceWebSocket
	select {
	case f.updates <- ev:
	default:
	}
}
