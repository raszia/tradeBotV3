package simexec

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
)

// PR19 round 2 — ambiguous-execution simulation. Gated on V3_TEST_MYSQL_DSN (via setupSim).

// reqID builds a canonical order with a caller-chosen client id (so idempotency/lookup by
// client id can be asserted deterministically).
func reqID(clientID string) execution.OrderRequest {
	return execution.OrderRequest{
		ClientOrderID: clientID, Symbol: "BTC/IRT", Side: "buy",
		Quantity: decimal.RequireFromString("0.5"), LimitPrice: decimal.RequireFromString("100"),
	}
}

func uniqID(p string) string { return fmt.Sprintf("%s-%d", p, time.Now().UnixNano()) }

// TestPlaceTimeoutNotAcceptedPersistsNothing: "accepted?=no" — the ack times out AND no order is
// persisted, so a later read-only lookup by client_order_id PROVES it was never placed.
func TestPlaceTimeoutNotAcceptedPersistsNothing(t *testing.T) {
	db := setupSim(t)
	c := New(db, "sim", PlaceTimeoutNotAccepted)
	id := uniqID("cna")
	ack, err := c.PlaceOrder(context.Background(), reqID(id))
	if !errors.Is(err, execution.ErrAckTimeout) {
		t.Fatalf("place err = %v, want ErrAckTimeout", err)
	}
	if ack.ExchangeOrderID != "" {
		t.Errorf("timed-out ack carried an exchange id %q, want empty", ack.ExchangeOrderID)
	}
	if _, gerr := c.GetOrder(context.Background(), id); !errors.Is(gerr, execution.ErrOrderUnknown) {
		t.Errorf("lookup by client id = %v, want ErrOrderUnknown (provably not placed)", gerr)
	}
}

// TestPlaceTimeoutAcceptedPersistsAndRecoverableByClientID: the three accepted-but-timed-out
// scenarios persist the order FIRST (with its real state), return ErrAckTimeout WITHOUT the
// exchange id, and are then discoverable by client_order_id with the correct state.
func TestPlaceTimeoutAcceptedPersistsAndRecoverableByClientID(t *testing.T) {
	db := setupSim(t)
	half := decimal.RequireFromString("0.25")
	full := decimal.RequireFromString("0.5")
	cases := []struct {
		sc         Scenario
		wantStatus execution.NormalizedOrderState
		wantFilled decimal.Decimal
	}{
		{PlaceTimeoutAcceptedOpen, execution.StateOpen, decimal.Zero},
		{PlaceTimeoutAcceptedPartial, execution.StatePartiallyFilled, half},
		{PlaceTimeoutAcceptedFull, execution.StateFilled, full},
	}
	for _, tc := range cases {
		t.Run(string(tc.sc), func(t *testing.T) {
			c := New(db, "sim", tc.sc)
			id := uniqID("ca")
			ack, err := c.PlaceOrder(context.Background(), reqID(id))
			if !errors.Is(err, execution.ErrAckTimeout) {
				t.Fatalf("place err = %v, want ErrAckTimeout", err)
			}
			if ack.ExchangeOrderID != "" {
				t.Errorf("timed-out ack carried an exchange id %q, want empty (recover by client id)", ack.ExchangeOrderID)
			}
			// The order IS persisted and findable by client_order_id (exchange id was never returned).
			st, gerr := c.GetOrder(context.Background(), id)
			if gerr != nil {
				t.Fatalf("lookup by client id = %v, want the persisted order", gerr)
			}
			if st.Status != tc.wantStatus || !st.FilledQty.Equal(tc.wantFilled) {
				t.Errorf("recovered %s: status=%s filled=%s, want %s / %s", tc.sc, st.Status, st.FilledQty, tc.wantStatus, tc.wantFilled)
			}
			// It resolves to the deterministic exchange id too (SIM-<client id>).
			if st.ExchangeOrderID != "SIM-"+id {
				t.Errorf("exchange id = %q, want SIM-%s", st.ExchangeOrderID, id)
			}
		})
	}
}

// TestCancelTimeoutMutatesThenReadable: each cancel-timeout scenario places normally, then a
// CancelOrder mutates the persisted state and returns ErrAckTimeout; a later GetOrder returns
// the real post-cancel state (the actual filled quantity).
func TestCancelTimeoutMutatesThenReadable(t *testing.T) {
	db := setupSim(t)
	half := decimal.RequireFromString("0.25")
	full := decimal.RequireFromString("0.5")
	cases := []struct {
		sc         Scenario
		wantStatus execution.NormalizedOrderState
		wantFilled decimal.Decimal
	}{
		{CancelTimeoutButCanceled, execution.StateCanceled, decimal.Zero},
		{CancelTimeoutStillOpen, execution.StateOpen, decimal.Zero},
		{CancelTimeoutPartialThenCanceled, execution.StatePartiallyCanceled, half},
		{CancelTimeoutFilledBeforeCancel, execution.StateFilled, full},
	}
	for _, tc := range cases {
		t.Run(string(tc.sc), func(t *testing.T) {
			c := New(db, "sim", tc.sc)
			id := uniqID("ct")
			ack, err := c.PlaceOrder(context.Background(), reqID(id))
			if err != nil {
				t.Fatalf("place err = %v, want a normal ack (cancel scenarios place OPEN)", err)
			}
			if ack.ExchangeOrderID == "" {
				t.Fatal("cancel-timeout place must return the exchange id")
			}
			// The cancel mutates state and times out.
			if cerr := c.CancelOrder(context.Background(), ack.ExchangeOrderID); !errors.Is(cerr, execution.ErrAckTimeout) {
				t.Fatalf("cancel err = %v, want ErrAckTimeout", cerr)
			}
			st, gerr := c.GetOrder(context.Background(), ack.ExchangeOrderID)
			if gerr != nil {
				t.Fatalf("post-cancel GetOrder = %v", gerr)
			}
			if st.Status != tc.wantStatus || !st.FilledQty.Equal(tc.wantFilled) {
				t.Errorf("%s: post-cancel status=%s filled=%s, want %s / %s", tc.sc, st.Status, st.FilledQty, tc.wantStatus, tc.wantFilled)
			}
		})
	}
}

// TestIdempotentPlaceImmutable: re-placing the SAME client id with an identical payload returns
// the same order deterministically and writes NO second row (an accepted order is immutable); a
// DIFFERENT payload on the same client id is a hard conflict, never an overwrite.
func TestIdempotentPlaceImmutable(t *testing.T) {
	db := setupSim(t)
	c := New(db, "sim", FullFill)
	id := uniqID("idem")
	r := reqID(id)
	ack1, err := c.PlaceOrder(context.Background(), r)
	if err != nil {
		t.Fatalf("first place: %v", err)
	}
	ack2, err := c.PlaceOrder(context.Background(), r) // identical re-place
	if err != nil {
		t.Fatalf("idempotent re-place should succeed, got %v", err)
	}
	if ack1.ExchangeOrderID != ack2.ExchangeOrderID {
		t.Errorf("re-place returned a different exchange id: %q vs %q", ack1.ExchangeOrderID, ack2.ExchangeOrderID)
	}
	var rows int
	db.QueryRow("SELECT COUNT(*) FROM sim_exchange_orders WHERE exchange_code='sim' AND client_order_id=?", id).Scan(&rows)
	if rows != 1 {
		t.Errorf("rows for client id = %d, want 1 (no second row; immutable)", rows)
	}
	// Same client id, DIFFERENT payload → conflict, never overwrite.
	conflicting := r
	conflicting.Quantity = decimal.RequireFromString("9.9")
	if _, cerr := c.PlaceOrder(context.Background(), conflicting); cerr == nil {
		t.Error("re-place with a different payload must conflict, not overwrite")
	} else {
		var api *exchanges.NormalizedAPIError
		if !errors.As(cerr, &api) || api.Category != exchanges.CatBadRequest {
			t.Errorf("conflict err = %v, want a bad_request NormalizedAPIError", cerr)
		}
	}
	// The original order is untouched by the conflicting attempt.
	st, _ := c.GetOrder(context.Background(), id)
	if !st.IntendedQty.Equal(decimal.RequireFromString("0.5")) {
		t.Errorf("stored qty = %s, want the immutable original 0.5", st.IntendedQty)
	}
}

// TestCancelUsesPersistedScenarioNotClientScenario (PR19 round 3 #5): an order's lifecycle is
// deterministic across instances — a CancelOrder issued by a DIFFERENT client instance (a restart
// with a different default scenario) follows the order's OWN persisted scenario, not the new
// client's. Instance A places a cancel-timeout-partial order; instance B (default full_fill)
// cancels it; the order behaves per A's scenario (partial-then-canceled + ack timeout).
func TestCancelUsesPersistedScenarioNotClientScenario(t *testing.T) {
	db := setupSim(t)
	a := New(db, "sim", CancelTimeoutPartialThenCanceled)
	id := uniqID("det")
	ack, err := a.PlaceOrder(context.Background(), reqID(id))
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	// A brand-new instance with a DIFFERENT default scenario cancels the order.
	b := New(db, "sim", FullFill)
	cerr := b.CancelOrder(context.Background(), ack.ExchangeOrderID)
	if !errors.Is(cerr, execution.ErrAckTimeout) {
		t.Fatalf("cancel by new instance = %v, want ErrAckTimeout per the order's OWN scenario", cerr)
	}
	st, gerr := b.GetOrder(context.Background(), ack.ExchangeOrderID)
	if gerr != nil {
		t.Fatalf("get: %v", gerr)
	}
	if st.Status != execution.StatePartiallyCanceled || !st.FilledQty.Equal(decimal.RequireFromString("0.25")) {
		t.Errorf("post-cancel = %s / %s, want partially_canceled 0.25 (the ORDER's scenario, not full_fill)", st.Status, st.FilledQty)
	}
}

// TestReplaceUsesPersistedScenarioAcrossInstances (PR19 round 4 #3): instance A creates an
// accepted-but-timeout order; instance B — a DIFFERENT process with a DIFFERENT configured
// scenario — repeats the identical PlaceOrder. The result must follow the ORIGINAL persisted
// scenario (looked up first), never instance B's, and write no second row.
func TestReplaceUsesPersistedScenarioAcrossInstances(t *testing.T) {
	db := setupSim(t)
	id := uniqID("xinst")
	// Instance A: accepted-but-timeout persists a FILLED order and returns ErrAckTimeout.
	a := New(db, "sim", PlaceTimeoutAcceptedFull)
	if _, err := a.PlaceOrder(context.Background(), reqID(id)); !errors.Is(err, execution.ErrAckTimeout) {
		t.Fatalf("instance A place = %v, want ErrAckTimeout", err)
	}
	// Instance B: a different default scenario (ZeroFill). The identical re-place must replay A's.
	b := New(db, "sim", ZeroFill)
	if _, err := b.PlaceOrder(context.Background(), reqID(id)); !errors.Is(err, execution.ErrAckTimeout) {
		t.Errorf("instance B re-place = %v, want ErrAckTimeout (A's persisted scenario, not ZeroFill)", err)
	}
	// The order still resolves to a FULL fill (A's scenario), not ZeroFill.
	st, gerr := b.GetOrder(context.Background(), id)
	if gerr != nil {
		t.Fatalf("get: %v", gerr)
	}
	if st.Status != execution.StateFilled || !st.FilledQty.Equal(decimal.RequireFromString("0.5")) {
		t.Errorf("recovered %s / %s, want a FULL fill per A's scenario", st.Status, st.FilledQty)
	}
	var rows int
	db.QueryRow("SELECT COUNT(*) FROM sim_exchange_orders WHERE exchange_code='sim' AND client_order_id=?", id).Scan(&rows)
	if rows != 1 {
		t.Errorf("rows for client id = %d, want 1 (no second row from the re-place)", rows)
	}
}

// TestImmutableFieldsIncludeTypeAndTIF: a re-place that differs ONLY in order_type or
// time_in_force is a conflict, never an idempotent match.
func TestImmutableFieldsIncludeTypeAndTIF(t *testing.T) {
	db := setupSim(t)
	c := New(db, "sim", FullFill)
	id := uniqID("imm")
	base := reqID(id)
	base.OrderType = "limit"
	base.TimeInForce = "GTC"
	if _, err := c.PlaceOrder(context.Background(), base); err != nil {
		t.Fatalf("first place: %v", err)
	}
	diffTIF := base
	diffTIF.TimeInForce = "IOC"
	if _, err := c.PlaceOrder(context.Background(), diffTIF); err == nil {
		t.Error("re-place with a different time_in_force must conflict")
	}
	diffType := base
	diffType.OrderType = "market"
	if _, err := c.PlaceOrder(context.Background(), diffType); err == nil {
		t.Error("re-place with a different order_type must conflict")
	}
	// An identical re-place (all immutable fields equal) is still idempotent.
	if _, err := c.PlaceOrder(context.Background(), base); err != nil {
		t.Errorf("identical re-place must stay idempotent, got %v", err)
	}
}

// TestLookupByEitherIdentifier: GetOrder resolves by exchange_order_id OR client_order_id.
func TestLookupByEitherIdentifier(t *testing.T) {
	db := setupSim(t)
	c := New(db, "sim", FullFill)
	id := uniqID("both")
	ack, err := c.PlaceOrder(context.Background(), reqID(id))
	if err != nil {
		t.Fatal(err)
	}
	byExo, e1 := c.GetOrder(context.Background(), ack.ExchangeOrderID)
	byClient, e2 := c.GetOrder(context.Background(), id)
	if e1 != nil || e2 != nil {
		t.Fatalf("lookup errors: byExo=%v byClient=%v", e1, e2)
	}
	if byExo.ExchangeOrderID != byClient.ExchangeOrderID || byExo.Status != byClient.Status {
		t.Errorf("lookup by exchange id vs client id disagree: %+v vs %+v", byExo, byClient)
	}
}
