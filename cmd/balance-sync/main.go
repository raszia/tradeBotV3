// Command balance-sync continuously polls exchange balances into the current and
// history tables with hash-based duplicate prevention (PR13). It uses ONLY read-only
// balance clients (the narrow balance.BalanceClient interface = Name + GetBalances), so
// no PlaceOrder/CancelOrder is reachable by construction. It makes no trading decisions,
// creates no cycles, and mutates no queue.
//
// PR20a: real authenticated balance clients are now built via the factory with
// DB-decrypted credentials. A missing/invalid master key disables credential loading
// safely → the binary wires NO clients and idles (the syncer has nothing to poll); it
// never panics and never requires live credentials.
package main

import (
	"context"
	"fmt"
	"os"

	"v3TradeBot/internal/balance"
	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/credentials"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/service"
)

func main() {
	err := service.RunWithDB("balance-sync", service.ConfigFlag(), func(ctx context.Context, base service.Base, store *db.Store) error {
		clients := map[string]balance.BalanceClient{}
		// Build read-only credentialed clients (narrowed to BalanceClient — no place/
		// cancel reachable). No/invalid master key → no clients, idle safely.
		if provider, perr := credentials.NewProvider(store.DB(), base.Cfg.Security.MasterKey, clock.NewSystem(), base.Log); perr != nil {
			base.Log.Warn("balance-sync: credential loading disabled (no/invalid master key); idling", "err", perr)
		} else {
			iolog := exchanges.NewIOLogger(exchanges.IOLogConfig{Enabled: true, Source: "balance-sync"}, store.DB())
			defer iolog.Close()
			builder := credentials.NewBuilder(store.DB(), provider, iolog)
			for _, code := range builder.EnabledCodes(ctx) {
				client, err := builder.BuildPrivate(ctx, code)
				if err != nil {
					continue // no active credential / unsupported adapter -> skip
				}
				clients[code] = client // *exchanges client satisfies BalanceClient (narrowed)
			}
		}
		syncer := balance.New(store, clients, clock.NewSystem(), base.Log, balance.Config{})
		base.Log.Info("balance-sync booted", "clients", len(clients))
		return syncer.Run(ctx) // idles when there are no clients; blocks until shutdown
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "balance-sync: "+err.Error())
		os.Exit(1)
	}
}
