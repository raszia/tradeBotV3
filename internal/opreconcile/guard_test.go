package opreconcile

import (
	"reflect"
	"testing"
)

// TestResolverHoldsNoExchangeClient asserts the Resolver cannot reach an exchange: none
// of its fields expose PlaceOrder/CancelOrder/GetOrder/GetBalances. The resolution tool is
// a LOCAL tool — it can never place or cancel an order, and makes no real exchange call.
func TestResolverHoldsNoExchangeClient(t *testing.T) {
	rt := reflect.TypeOf(Resolver{})
	forbidden := []string{"PlaceOrder", "CancelOrder", "GetOrder", "GetOpenOrders", "GetBalances", "SubscribeOrderUpdates"}
	for i := 0; i < rt.NumField(); i++ {
		ft := rt.Field(i).Type
		for _, m := range forbidden {
			if _, ok := ft.MethodByName(m); ok {
				t.Errorf("Resolver field %q (type %s) exposes %s — the resolver must hold no exchange client", rt.Field(i).Name, ft, m)
			}
		}
	}
}

// TestActionsAreDistinct guards against an accidental duplicate action key.
func TestActionsAreDistinct(t *testing.T) {
	seen := map[Action]bool{}
	for _, a := range []Action{
		ActionCancelZeroExposure, ActionAttachExchangeOrderID, ActionMarkBuyFilled,
		ActionMarkBuyZeroFilled, ActionMarkSellFilled, ActionMarkSellPartiallyFilled,
		ActionMarkOrderCancelledZeroFill, ActionKeepNeedsReconcile, ActionMarkFailed,
	} {
		if seen[a] {
			t.Errorf("duplicate action key %q", a)
		}
		seen[a] = true
	}
}
