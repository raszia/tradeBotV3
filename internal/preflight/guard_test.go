package preflight

import (
	"reflect"
	"testing"
)

// TestCheckerHoldsNoExchangeClient asserts preflight cannot reach an exchange: none of the
// Checker's fields expose a place/cancel/balance/order method. Preflight is strictly
// read-only and makes no real exchange call.
func TestCheckerHoldsNoExchangeClient(t *testing.T) {
	rt := reflect.TypeOf(Checker{})
	forbidden := []string{"PlaceOrder", "CancelOrder", "GetBalances", "GetOrder", "GetOpenOrders"}
	for i := 0; i < rt.NumField(); i++ {
		ft := rt.Field(i).Type
		for _, m := range forbidden {
			if _, ok := ft.MethodByName(m); ok {
				t.Errorf("Checker field %q (type %s) exposes %s — preflight must hold no exchange client", rt.Field(i).Name, ft, m)
			}
		}
	}
}
