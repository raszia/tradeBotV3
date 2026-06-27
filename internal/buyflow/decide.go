// Package buyflow creates the buy side of a trading cycle: on an accepted signal it
// inserts the cycle + symbol lock + buy order + PLACE_ORDER request in ONE database
// transaction, then leaves execution to the order-executor (PR10+). It NEVER calls
// an exchange and NEVER sends an order — it only prepares and persists the intent.
//
// The owner-defined buy policy is maker-first with taker fallback (see
// PROJECT_ARCHITECTURE.md §2a): try a maker limit slightly below the Iranian ask to
// save fees; after a configurable number of maker attempts within a rolling window,
// escalate to a taker buy at/near the ask. `Decide` is the pure decision; the rest
// of the package performs the transactional persistence.
package buyflow

import (
	"github.com/shopspring/decimal"

	"v3TradeBot/internal/configstore"
)

// Mode is the prepared execution mode for a buy attempt.
type Mode string

const (
	ModeMakerFirst    Mode = "MAKER_FIRST"    // first maker attempt for the scope in the window
	ModeMakerRetry    Mode = "MAKER_RETRY"    // a later maker attempt (still below the taker threshold)
	ModeTakerFallback Mode = "TAKER_FALLBACK" // maker budget exhausted (or maker disabled) -> buy at/near ask
)

var bpsDenom = decimal.NewFromInt(10000)

// Decision is the chosen mode + limit price for one buy attempt.
type Decision struct {
	Mode          Mode
	AttemptNumber int             // 1-based attempt index for the scope in the window
	LimitPrice    decimal.Decimal // the price to place the limit buy at
	OffsetBps     int             // maker offset used (0 for taker)
}

// Decide chooses the buy mode and limit price from the per-symbol policy, the
// current Iranian ask, and how many maker attempts have already happened for this
// scope within the rolling window (priorMakerAttempts, counted from prior cycles).
//
// Maker:  limit = ask × (1 − maker_price_offset_bps/10000)   (buy below the ask)
// Taker:  limit = ask                                        (taker_price_mode=ASK)
//
// The taker slippage cap (max_taker_slippage_bps) is NOT applied to the prepared
// price here — it is carried in the request payload for the executor to enforce
// against the LIVE ask at send time (the ask may move between decision and send).
func Decide(p configstore.MakerPolicy, ask decimal.Decimal, priorMakerAttempts int) Decision {
	threshold := p.MakerAttemptsBeforeTaker
	if threshold < 1 {
		threshold = 1
	}
	attempt := priorMakerAttempts + 1
	if p.MakerFirstEnabled && attempt <= threshold {
		offset := p.MakerPriceOffsetBps
		if offset < 0 {
			offset = 0
		}
		// ask × (10000 − offset) / 10000
		limit := ask.Mul(bpsDenom.Sub(decimal.NewFromInt(int64(offset)))).Div(bpsDenom)
		mode := ModeMakerFirst
		if attempt > 1 {
			mode = ModeMakerRetry
		}
		return Decision{Mode: mode, AttemptNumber: attempt, LimitPrice: limit, OffsetBps: offset}
	}
	// Taker fallback (or maker disabled): buy at the ask.
	return Decision{Mode: ModeTakerFallback, AttemptNumber: attempt, LimitPrice: ask, OffsetBps: 0}
}

// IsMaker reports whether a mode is a maker attempt (counts toward the window total).
func (m Mode) IsMaker() bool { return m == ModeMakerFirst || m == ModeMakerRetry }
