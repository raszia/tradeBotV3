package reconciler

import (
	"context"
	"sync"
	"testing"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/db"
	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/state"
)

// PR19 round 2 #5 — the reconciler must NEVER mix real and dry-run clients. It holds both sets
// and routes STRICTLY by the owning cycle's dry_run flag. Gated on V3_TEST_MYSQL_DSN (setupR).

// recordingRO records the ids passed to GetOrder so tests can assert WHICH client set was used
// for WHICH cycle. Place/Cancel fail the test (read-only, as any reconciler client must be).
type recordingRO struct {
	t    *testing.T
	name string
	mu   sync.Mutex
	seen []string
}

func (r *recordingRO) Name() string { return r.name }
func (r *recordingRO) Capabilities() exchanges.Capabilities {
	return exchanges.Capabilities{FetchByOrderID: true, ClientOrderID: true}
}
func (r *recordingRO) GetOrder(_ context.Context, id string) (execution.OrderStatus, error) {
	r.mu.Lock()
	r.seen = append(r.seen, id)
	r.mu.Unlock()
	// Report "still open" — a benign Continue outcome that changes no cycle state.
	return execution.OrderStatus{ExchangeOrderID: id, Status: execution.StateOpen, IntendedQty: decimal.RequireFromString("1")}, nil
}
func (r *recordingRO) GetOrderByClientOrderID(ctx context.Context, id string) (execution.OrderStatus, error) {
	return r.GetOrder(ctx, id)
}
func (r *recordingRO) GetOpenOrders(context.Context, string) ([]execution.OrderStatus, error) {
	return nil, nil
}
func (r *recordingRO) GetBalances(context.Context) ([]domain.Balance, error) { return nil, nil }
func (r *recordingRO) PlaceOrder(context.Context, execution.OrderRequest) (execution.OrderAck, error) {
	r.t.Fatalf("reconciler client %q must NEVER PlaceOrder", r.name)
	return execution.OrderAck{}, nil
}
func (r *recordingRO) CancelOrder(context.Context, string) error {
	r.t.Fatalf("reconciler client %q must NEVER CancelOrder", r.name)
	return nil
}
func (r *recordingRO) saw(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.seen {
		if s == id {
			return true
		}
	}
	return false
}

func (f *rfix) markDryRun(t *testing.T, cyc int64) {
	t.Helper()
	if _, err := f.db.Exec("UPDATE cycles SET dry_run=1 WHERE id=?", cyc); err != nil {
		t.Fatal(err)
	}
}

// TestReconcilerRoutesByDryRunNeverMixes: a real (dry_run=0) cycle is verified ONLY through the
// real client; a dry-run (dry_run=1) cycle ONLY through the sim client. Neither client is ever
// queried for the other mode's order (no SIM id to a real client, no real id to the simulator).
func TestReconcilerRoutesByDryRunNeverMixes(t *testing.T) {
	f := setupR(t)
	realC := &recordingRO{t: t, name: "real"}
	simC := &recordingRO{t: t, name: "sim"}
	rec := New(db.NewFromDB(f.db), map[string]ReadOnlyClient{f.code: realC}, map[string]ReadOnlyClient{f.code: simC}, nil)

	realCyc, _ := f.seedCycleOrder(t, state.CycleBuySubmitted, state.OrderAcked, "REAL-OID")
	dryCyc, _ := f.seedCycleOrder(t, state.CycleBuySubmitted, state.OrderAcked, "SIM-OID")
	f.markDryRun(t, dryCyc)

	if _, err := rec.ReconcileStartup(f.ctx); err != nil {
		t.Fatal(err)
	}

	if !realC.saw("REAL-OID") {
		t.Error("real cycle must be verified through the REAL client")
	}
	if realC.saw("SIM-OID") {
		t.Error("real client must NEVER see a dry-run cycle's order (no cross-mode query)")
	}
	if !simC.saw("SIM-OID") {
		t.Error("dry-run cycle must be verified through the SIM client")
	}
	if simC.saw("REAL-OID") {
		t.Error("sim client must NEVER see a real cycle's order (no cross-mode query)")
	}
	_ = realCyc
}

// TestReconcilerSkipsWhenModeClientAbsent: a dry-run cycle with NO sim client wired is SKIPPED
// safely (its state is untouched) and is NEVER verified through the available real client.
func TestReconcilerSkipsWhenModeClientAbsent(t *testing.T) {
	f := setupR(t)
	realC := &recordingRO{t: t, name: "real"}
	// Only a real client is wired; the sim set is empty.
	rec := New(db.NewFromDB(f.db), map[string]ReadOnlyClient{f.code: realC}, nil, nil)

	dryCyc, _ := f.seedCycleOrder(t, state.CycleBuySubmitted, state.OrderAcked, "SIM-ONLY")
	f.markDryRun(t, dryCyc)

	if _, err := rec.ReconcileStartup(f.ctx); err != nil {
		t.Fatal(err)
	}
	if realC.saw("SIM-ONLY") {
		t.Error("a dry-run cycle must NOT fall back to the real client when no sim client exists")
	}
	if got := cycState(t, f.db, dryCyc); got != string(state.CycleBuySubmitted) {
		t.Errorf("skipped dry-run cycle state = %s, want unchanged BUY_SUBMITTED", got)
	}
}
