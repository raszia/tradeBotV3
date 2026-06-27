// Package orders is the buy-side order-processing layer (PR10). It turns
// order-executor results into persisted order/cycle state and fills: it runs the
// simulated-IOC buy as queued/scheduled work (PLACE_ORDER → wait → CANCEL_ORDER →
// GET_ORDER → record fills), does the fill accounting, classifies the outcome
// (zero/partial/full/ambiguous), and advances the state machine — conservatively.
//
// It NEVER calls an exchange itself; the order-executor performs the transport and
// hands the typed result (ack/cancel-result/status) to these functions, which do the
// database work inside the caller's transaction. Ambiguity is always resolved to
// NEEDS_RECONCILE, never a guess.
package orders

import (
	"encoding/json"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/execution"
)

// BuyIntentPayload is the executor instruction set carried on a buy PLACE_ORDER
// request (produced by internal/buyflow in PR9, consumed here in PR10). The JSON
// shape is the contract between the two; field names must stay stable.
type BuyIntentPayload struct {
	ExecutionMode            string `json:"execution_mode"` // MAKER_FIRST | MAKER_RETRY | TAKER_FALLBACK
	Side                     string `json:"side"`
	OrderType                string `json:"order_type"`
	SimulatedIOC             bool   `json:"simulated_ioc"`
	IntendedPrice            string `json:"intended_price"`
	IntendedQuantity         string `json:"intended_quantity"`
	BuySizeUnit              string `json:"buy_size_unit"`
	MakerAttemptNumber       int    `json:"maker_attempt_number"`
	MakerAttemptsBeforeTaker int    `json:"maker_attempts_before_taker"`
	MakerOffsetBps           int    `json:"maker_offset_bps"`
	MakerWaitBeforeCancelMs  int    `json:"maker_wait_before_cancel_ms"`
	CancelAfterWait          bool   `json:"cancel_after_wait"`
	FinalStatusCheckRequired bool   `json:"final_status_check_required"`
	TakerPriceMode           string `json:"taker_price_mode"`
	MaxTakerSlippageBps      int    `json:"max_taker_slippage_bps"`
	AskPriceAtDecision       string `json:"ask_price_at_decision"`
	SignalBinancePrice       string `json:"signal_binance_price"`
	SignalIranianPrice       string `json:"signal_iranian_price"`
	QuoteUnit                string `json:"quote_unit"`
	ReferenceRate            string `json:"reference_rate,omitempty"`
	BuyFeeBps                int    `json:"buy_fee_bps"`
	SellFeeBps               int    `json:"sell_fee_bps"`
	ConfigVersion            int64  `json:"config_version"`
	LocalClientOrderID       string `json:"local_client_order_id"`
}

// ParseBuyIntent decodes the PLACE_ORDER payload.
func ParseBuyIntent(raw json.RawMessage) (BuyIntentPayload, error) {
	var p BuyIntentPayload
	err := json.Unmarshal(raw, &p)
	return p, err
}

// OrderRequest builds the exchange PlaceOrder request from the intent. The internal
// local_client_order_id is the ClientOrderID (used as the venue client id only when
// the adapter's capabilities allow); TimeInForce is intentionally LEFT EMPTY —
// native IOC is never forced (the IOC behaviour is simulated by the place → wait →
// cancel → status flow).
func (p BuyIntentPayload) OrderRequest(symbol string) execution.OrderRequest {
	return execution.OrderRequest{
		ClientOrderID: p.LocalClientOrderID,
		Symbol:        symbol,
		Side:          p.Side,
		Quantity:      decimalOrZero(p.IntendedQuantity),
		LimitPrice:    decimalOrZero(p.IntendedPrice),
		OrderType:     p.OrderType,
		TimeInForce:   "", // simulated IOC — do not force a native TIF
	}
}

// MakerWait is the configured wait before cancelling the remainder (simulated IOC).
func (p BuyIntentPayload) MakerWait() time.Duration {
	if p.MakerWaitBeforeCancelMs <= 0 {
		return 0
	}
	return time.Duration(p.MakerWaitBeforeCancelMs) * time.Millisecond
}

func decimalOrZero(s string) decimal.Decimal {
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Zero
	}
	return d
}
