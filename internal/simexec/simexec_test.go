package simexec

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/migrate"
)

// ---- offline ----

func TestDefaultScenarioIsFullFill(t *testing.T) {
	if New(nil, "sim", "").scenario != FullFill {
		t.Error("empty scenario should default to full fill")
	}
}

// ---- gated (the simulator is DB-backed: persistent, shared across instances) ----

func setupSim(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the simexec integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := migrate.Run(context.Background(), db, migrate.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

// req builds a canonical order with a unique client-order-id (isolated on the shared DB).
func req() execution.OrderRequest {
	return execution.OrderRequest{
		ClientOrderID: fmt.Sprintf("c%d-buy", time.Now().UnixNano()), Symbol: "BTC/IRT", Side: "buy",
		Quantity: decimal.RequireFromString("0.5"), LimitPrice: decimal.RequireFromString("100"),
	}
}

// run places + GetOrders a scenario and returns the final status (+ any errors).
func run(t *testing.T, db *sql.DB, sc Scenario) (execution.OrderAck, error, execution.OrderStatus, error) {
	t.Helper()
	c := New(db, "sim", sc)
	ack, perr := c.PlaceOrder(context.Background(), req())
	if perr != nil {
		return ack, perr, execution.OrderStatus{}, nil
	}
	st, gerr := c.GetOrder(context.Background(), ack.ExchangeOrderID)
	return ack, nil, st, gerr
}

func TestScenarioOutcomes(t *testing.T) {
	db := setupSim(t)
	if _, _, st, err := run(t, db, FullFill); err != nil || st.Status != execution.StateFilled || !st.FilledQty.Equal(decimal.RequireFromString("0.5")) {
		t.Errorf("full fill = %+v err=%v", st, err)
	}
	if _, _, st, _ := run(t, db, PartialFill); st.Status != execution.StatePartiallyCanceled || !st.FilledQty.Equal(decimal.RequireFromString("0.25")) {
		t.Errorf("partial fill = %+v, want partially_canceled with 0.25 filled", st)
	}
	if _, _, st, _ := run(t, db, ZeroFill); st.Status != execution.StateCanceled || !st.FilledQty.IsZero() {
		t.Errorf("zero fill = %+v", st)
	}
	if _, _, st, _ := run(t, db, CancelRace); st.Status != execution.StateFilled {
		t.Errorf("cancel race = %+v", st)
	}
	if _, _, _, gerr := run(t, db, Ambiguous); !errors.Is(gerr, execution.ErrOrderUnknown) {
		t.Errorf("ambiguous getorder err = %v, want ErrOrderUnknown", gerr)
	}
	if _, perr, _, _ := run(t, db, Rejected); perr == nil {
		t.Error("rejected should fail PlaceOrder")
	} else {
		var api *exchanges.NormalizedAPIError
		if !errors.As(perr, &api) || api.Category != exchanges.CatBadRequest {
			t.Errorf("rejected err = %v, want a bad_request NormalizedAPIError", perr)
		}
	}
	if _, perr, _, _ := run(t, db, PlaceTimeout); !errors.Is(perr, execution.ErrAckTimeout) {
		t.Errorf("place timeout err = %v, want ErrAckTimeout", perr)
	}
}

func TestSatisfiesPrivateClientAndNoMutatingNetwork(t *testing.T) {
	db := setupSim(t)
	c := New(db, "sim", FullFill)
	if err := c.CancelOrder(context.Background(), "SIM-x"); err != nil {
		t.Errorf("simulated cancel must not error: %v", err)
	}
	bals, err := c.GetBalances(context.Background())
	if err != nil || len(bals) == 0 {
		t.Errorf("simulated balances = %v, %v", bals, err)
	}
	if _, err := c.GetOrder(context.Background(), "never-placed-xyz"); !errors.Is(err, execution.ErrOrderUnknown) {
		t.Errorf("unknown order = %v, want ErrOrderUnknown", err)
	}
}

// TestPersistedOrderVisibleToNewInstance proves the fix for cross-instance/restart: an order
// placed by client A is resolvable by a SEPARATELY-constructed client B (a different Client
// value, as a new process/instance would build) — no ErrOrderUnknown, correct stored-scenario
// outcome — because the state lives in the DB, not a process-local map.
func TestPersistedOrderVisibleToNewInstance(t *testing.T) {
	db := setupSim(t)
	r := req()
	a := New(db, "sim", FullFill)
	ack, err := a.PlaceOrder(context.Background(), r)
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	// A brand-new client instance (even with a DIFFERENT default scenario) resolves the order
	// via the STORED scenario — deterministic and independent of which instance placed it.
	b := New(db, "sim", ZeroFill)
	st, gerr := b.GetOrder(context.Background(), ack.ExchangeOrderID)
	if gerr != nil {
		t.Fatalf("new instance GetOrder returned %v (want the persisted order, not ErrOrderUnknown)", gerr)
	}
	if st.Status != execution.StateFilled || !st.FilledQty.Equal(r.Quantity) {
		t.Errorf("new instance resolved %+v, want a FULL fill per the stored scenario", st)
	}
}
