package execution

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestEventFromStatus(t *testing.T) {
	now := time.Now()
	st := OrderStatus{
		ExchangeOrderID: "ex1",
		ClientOrderID:   "loc1",
		Symbol:          "BTC/USDT",
		Side:            SideBuy,
		Status:          StatePartiallyFilled,
		FilledQty:       decimal.RequireFromString("0.5"),
		RemainingQty:    decimal.RequireFromString("0.5"),
		AvgPrice:        decimal.RequireFromString("50000"),
		UpdatedAt:       now,
	}
	ev := EventFromStatus("nobitex", st)
	if ev.Source != SourcePolling {
		t.Errorf("source = %q, want polling", ev.Source)
	}
	if ev.Exchange != "nobitex" || ev.ExchangeOrderID != "ex1" || ev.Status != StatePartiallyFilled {
		t.Errorf("event mismatch: %+v", ev)
	}
	if !ev.FilledQty.Equal(decimal.RequireFromString("0.5")) {
		t.Errorf("filled = %s", ev.FilledQty)
	}
	if !ev.EventTime.Equal(now) {
		t.Errorf("event time mismatch")
	}
}

func TestEventFromAckComputesRemaining(t *testing.T) {
	ack := OrderAck{
		ExchangeOrderID: "ex2",
		Symbol:          "ETH/IRT",
		Side:            SideSell,
		Status:          StateOpen,
		RequestedQty:    decimal.RequireFromString("2"),
		FilledQty:       decimal.RequireFromString("0.75"),
		AckedAt:         time.Now(),
	}
	ev := EventFromAck("wallex", ack)
	if !ev.RemainingQty.Equal(decimal.RequireFromString("1.25")) {
		t.Errorf("remaining = %s, want 1.25", ev.RemainingQty)
	}
	if ev.Side != SideSell || ev.Status != StateOpen {
		t.Errorf("event mismatch: %+v", ev)
	}
}

func TestNormalizedOrderStateValues(t *testing.T) {
	// Guard against accidental rename — these strings are persisted/compared.
	if StatePartiallyFilled != "partially_filled" || StateFilled != "filled" || StateUnknown != "unknown" {
		t.Error("normalized order-state string drift")
	}
}
