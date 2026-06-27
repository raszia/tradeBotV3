package exchanges

import (
	"context"
	"errors"
	"testing"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/execution"
)

func TestFakePrivateClientLifecycle(t *testing.T) {
	ctx := context.Background()
	f := NewFakePrivateClient("fakeex")
	f.SetBalances([]domain.Balance{{Exchange: "fakeex", Asset: "USDT", Available: decimal.RequireFromString("1000")}})

	bals, err := f.GetBalances(ctx)
	if err != nil || len(bals) != 1 || bals[0].Asset != "USDT" {
		t.Fatalf("GetBalances = %v err=%v", bals, err)
	}

	ack, err := f.PlaceOrder(ctx, execution.OrderRequest{
		ClientOrderID: "loc-1", Symbol: "BTC/IRT", Side: execution.SideBuy,
		Quantity: decimal.RequireFromString("0.1"), LimitPrice: decimal.RequireFromString("1000"),
		OrderType: execution.OrderTypeLimit,
	})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if ack.Status != execution.StateOpen || ack.ExchangeOrderID == "" {
		t.Fatalf("ack = %+v", ack)
	}

	// Open orders include it.
	open, _ := f.GetOpenOrders(ctx, "")
	if len(open) != 1 {
		t.Fatalf("open orders = %d, want 1", len(open))
	}

	// Cancel it.
	if err := f.CancelOrder(ctx, ack.ExchangeOrderID); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	st, _ := f.GetOrder(ctx, ack.ExchangeOrderID)
	if st.Status != execution.StateCanceled {
		t.Errorf("status after cancel = %s", st.Status)
	}

	// Unknown order id.
	if _, err := f.GetOrder(ctx, "does-not-exist"); !errors.Is(err, execution.ErrOrderUnknown) {
		t.Errorf("GetOrder unknown err = %v", err)
	}

	// An update event was emitted on placement.
	ch, _ := f.SubscribeOrderUpdates(ctx)
	select {
	case ev := <-ch:
		if ev.Source != execution.SourceWebSocket {
			t.Errorf("event source = %s", ev.Source)
		}
	default:
		t.Error("expected at least one buffered order-update event")
	}
}

func TestFakeAutoFillAndCapabilityOverride(t *testing.T) {
	ctx := context.Background()
	f := NewFakePrivateClient("fakeex")
	f.AutoFill = true
	f.Caps = &Capabilities{PlaceOrder: true, ClientOrderID: false} // simulate no client-order-id support

	if f.Capabilities().ClientOrderID {
		t.Error("capability override not applied")
	}

	ack, err := f.PlaceOrder(ctx, execution.OrderRequest{
		Symbol: "ETH/IRT", Side: execution.SideSell,
		Quantity: decimal.RequireFromString("2"), LimitPrice: decimal.RequireFromString("500"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if ack.Status != execution.StateFilled || !ack.FilledQty.Equal(decimal.RequireFromString("2")) {
		t.Errorf("autofill ack = %+v", ack)
	}
}
