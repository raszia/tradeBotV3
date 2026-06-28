package credentials

import (
	"reflect"
	"testing"

	"v3TradeBot/internal/balance"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/reconciler"
)

// Compile-time proof that a real private client satisfies EVERY narrowed read-only
// interface the services hold. The services build a PrivateClient via the factory but
// store it as one of these interfaces, so PlaceOrder/CancelOrder are unreachable from
// balance-sync / health / reconciler / credential validation by construction.
var (
	_ balance.BalanceClient     = (exchanges.PrivateClient)(nil)
	_ reconciler.ReadOnlyClient = (exchanges.PrivateClient)(nil)
	_ BalanceReader             = (exchanges.PrivateClient)(nil)
)

// TestNarrowedReadOnlyInterfacesCannotMutate asserts none of the read-only surfaces the
// credentialed services use expose a place/cancel method — credential validation and the
// read-only services can never mutate an exchange.
func TestNarrowedReadOnlyInterfacesCannotMutate(t *testing.T) {
	ifaces := map[string]reflect.Type{
		"balance.BalanceClient":     reflect.TypeOf((*balance.BalanceClient)(nil)).Elem(),
		"reconciler.ReadOnlyClient": reflect.TypeOf((*reconciler.ReadOnlyClient)(nil)).Elem(),
		"credentials.BalanceReader": reflect.TypeOf((*BalanceReader)(nil)).Elem(),
	}
	for name, typ := range ifaces {
		for _, forbidden := range []string{"PlaceOrder", "CancelOrder"} {
			if _, ok := typ.MethodByName(forbidden); ok {
				t.Errorf("%s must NOT expose %s (read-only services must not be able to mutate)", name, forbidden)
			}
		}
	}
}
