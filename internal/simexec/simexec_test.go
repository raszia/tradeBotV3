package simexec

import (
	"context"
	"errors"
	"testing"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
)

func req() execution.OrderRequest {
	return execution.OrderRequest{ClientOrderID: "c1-buy", Symbol: "BTC/IRT", Side: "buy",
		Quantity: decimal.RequireFromString("0.5"), LimitPrice: decimal.RequireFromString("100")}
}

// place + getOrder a scenario and return the final status (+ any errors).
func run(t *testing.T, sc Scenario) (execution.OrderAck, error, execution.OrderStatus, error) {
	t.Helper()
	c := New("sim", sc)
	ack, perr := c.PlaceOrder(context.Background(), req())
	if perr != nil {
		return ack, perr, execution.OrderStatus{}, nil
	}
	st, gerr := c.GetOrder(context.Background(), ack.ExchangeOrderID)
	return ack, nil, st, gerr
}

func TestScenarioOutcomes(t *testing.T) {
	// Full fill.
	if _, _, st, err := run(t, FullFill); err != nil || st.Status != execution.StateFilled || !st.FilledQty.Equal(decimal.RequireFromString("0.5")) {
		t.Errorf("full fill = %+v err=%v", st, err)
	}
	// Partial fill (half).
	if _, _, st, _ := run(t, PartialFill); st.Status != execution.StateCanceled || !st.FilledQty.Equal(decimal.RequireFromString("0.25")) {
		t.Errorf("partial fill = %+v", st)
	}
	// Zero fill.
	if _, _, st, _ := run(t, ZeroFill); st.Status != execution.StateCanceled || !st.FilledQty.IsZero() {
		t.Errorf("zero fill = %+v", st)
	}
	// Cancel race -> full fill status.
	if _, _, st, _ := run(t, CancelRace); st.Status != execution.StateFilled {
		t.Errorf("cancel race = %+v", st)
	}
	// Ambiguous -> GetOrder unknown.
	if _, _, _, gerr := run(t, Ambiguous); !errors.Is(gerr, execution.ErrOrderUnknown) {
		t.Errorf("ambiguous getorder err = %v, want ErrOrderUnknown", gerr)
	}
	// Rejected -> PlaceOrder definite rejection.
	if _, perr, _, _ := run(t, Rejected); perr == nil {
		t.Error("rejected should fail PlaceOrder")
	} else {
		var api *exchanges.NormalizedAPIError
		if !errors.As(perr, &api) || api.Category != exchanges.CatBadRequest {
			t.Errorf("rejected err = %v, want a bad_request NormalizedAPIError", perr)
		}
	}
	// Place timeout -> ambiguous ack timeout.
	if _, perr, _, _ := run(t, PlaceTimeout); !errors.Is(perr, execution.ErrAckTimeout) {
		t.Errorf("place timeout err = %v, want ErrAckTimeout", perr)
	}
}

func TestSatisfiesPrivateClientAndNoMutatingNetwork(t *testing.T) {
	// Compile-time it satisfies the interface; here assert CancelOrder/GetBalances are
	// pure (no error, no network) so dry-run never reaches a real exchange.
	c := New("sim", FullFill)
	if err := c.CancelOrder(context.Background(), "SIM-x"); err != nil {
		t.Errorf("simulated cancel must not error: %v", err)
	}
	bals, err := c.GetBalances(context.Background())
	if err != nil || len(bals) == 0 {
		t.Errorf("simulated balances = %v, %v", bals, err)
	}
	// Unknown order id -> ErrOrderUnknown (not a panic / network call).
	if _, err := c.GetOrder(context.Background(), "never-placed"); !errors.Is(err, execution.ErrOrderUnknown) {
		t.Errorf("unknown order = %v, want ErrOrderUnknown", err)
	}
}

func TestDefaultScenarioIsFullFill(t *testing.T) {
	if New("sim", "").scenario != FullFill {
		t.Error("empty scenario should default to full fill")
	}
}
