// Command balance-sync continuously polls exchange balances into the current and
// history tables with hash-based duplicate prevention (PR13). It uses ONLY read-only
// balance clients (no credentials → no clients yet), makes no trading decisions,
// creates no cycles, and mutates no queue.
//
// Real authenticated balance clients require decrypted exchange credentials, which
// are a later PR. Until then this binary wires NO clients and idles safely (the
// syncer simply has nothing to poll) — it never panics and never requires live
// credentials.
package main

import (
	"context"
	"fmt"
	"os"

	"v3TradeBot/internal/balance"
	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/service"
)

func main() {
	err := service.RunWithDB("balance-sync", service.ConfigFlag(), func(ctx context.Context, base service.Base, store *db.Store) error {
		// No read-only balance clients until credential decryption lands (later PR).
		clients := map[string]balance.BalanceClient{}
		syncer := balance.New(store, clients, clock.NewSystem(), base.Log, balance.Config{})
		base.Log.Info("balance-sync booted", "clients", len(clients))
		return syncer.Run(ctx) // idles when there are no clients; blocks until shutdown
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "balance-sync: "+err.Error())
		os.Exit(1)
	}
}
