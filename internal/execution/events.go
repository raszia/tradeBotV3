package execution

import (
	"time"

	"github.com/shopspring/decimal"
)

// OrderEventSource records how an order update was obtained. Some venues push
// updates over WebSocket; others only answer polling. Both are converted into
// the SAME NormalizedOrderEvent so the order-status processor (PR10) has one
// code path regardless of transport.
type OrderEventSource string

const (
	SourceWebSocket OrderEventSource = "websocket"
	SourcePolling   OrderEventSource = "polling"
)

// NormalizedOrderEvent is the unified internal representation of an order update,
// produced from either a WebSocket message or a polled GetOrder result. Fills
// carries any newly-observed fills (may be empty when the venue only reports
// aggregate status).
type NormalizedOrderEvent struct {
	Exchange        string
	Source          OrderEventSource
	ExchangeOrderID string
	ClientOrderID   string
	Symbol          string
	Side            string
	Status          NormalizedOrderState
	FilledQty       decimal.Decimal
	RemainingQty    decimal.Decimal
	AvgPrice        decimal.Decimal
	Fee             decimal.Decimal
	FeeAsset        string
	Fills           []Fill
	EventTime       time.Time
	Raw             string
}

// EventFromStatus converts a polled OrderStatus into a NormalizedOrderEvent so a
// polling-based adapter emits the same shape as a WebSocket-based one.
func EventFromStatus(exchange string, st OrderStatus) NormalizedOrderEvent {
	return NormalizedOrderEvent{
		Exchange:        exchange,
		Source:          SourcePolling,
		ExchangeOrderID: st.ExchangeOrderID,
		ClientOrderID:   st.ClientOrderID,
		Symbol:          st.Symbol,
		Side:            st.Side,
		Status:          st.Status,
		FilledQty:       st.FilledQty,
		RemainingQty:    st.RemainingQty,
		AvgPrice:        st.AvgPrice,
		Fee:             st.Fee,
		FeeAsset:        st.FeeAsset,
		EventTime:       st.UpdatedAt,
		Raw:             st.Raw,
	}
}

// EventFromAck converts a PlaceOrder ack into a NormalizedOrderEvent (the first
// event for an order, sourced from the synchronous place response).
func EventFromAck(exchange string, ack OrderAck) NormalizedOrderEvent {
	return NormalizedOrderEvent{
		Exchange:        exchange,
		Source:          SourcePolling,
		ExchangeOrderID: ack.ExchangeOrderID,
		ClientOrderID:   ack.ClientOrderID,
		Symbol:          ack.Symbol,
		Side:            ack.Side,
		Status:          ack.Status,
		FilledQty:       ack.FilledQty,
		RemainingQty:    ack.RequestedQty.Sub(ack.FilledQty),
		AvgPrice:        ack.AvgPrice,
		Fee:             ack.Fee,
		FeeAsset:        ack.FeeAsset,
		EventTime:       ack.AckedAt,
		Raw:             ack.Raw,
	}
}
