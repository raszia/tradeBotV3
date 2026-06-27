package models

import (
	"database/sql"
	"encoding/json"
	"time"

	"v3TradeBot/internal/state"
)

// This file holds the row structs needed by the PR3 state-machine work. They are
// intentionally lean (identity + state-machine-relevant columns); additional
// columns are added in the PRs that need them (cycle creation in PR9, fill
// processing in PR10, etc.). State-typed fields use the enums from internal/state
// so the model vocabulary matches the state machine exactly.

// Cycle mirrors the cycles table (state-machine-relevant subset).
type Cycle struct {
	ID               int64
	ExchangeMarketID int64
	BuyExchangeID    int64
	CanonicalSymbol  string
	State            state.CycleState
	Version          int64
	ConfigVersion    sql.NullInt64
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// Order mirrors the orders table (state-machine-relevant subset).
type Order struct {
	ID                 int64
	CycleID            int64
	ExchangeID         int64
	ExchangeMarketID   int64
	Side               string // 'buy' | 'sell'
	Role               string // 'entry_buy' | 'exit_sell'
	LocalClientOrderID string
	State              state.OrderState
	Version            int64
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// StateEvent mirrors a row of cycle_state_events or order_events (their shapes
// are identical apart from the parent id column). ParentID is the cycle_id or
// order_id.
type StateEvent struct {
	ID        int64
	ParentID  int64
	EventType string
	FromState sql.NullString
	ToState   string
	Version   int64
	Message   sql.NullString
	Payload   json.RawMessage
	CreatedAt time.Time
}
