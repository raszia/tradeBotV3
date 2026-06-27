// Command order-executor is the ONLY component that issues order-mutating
// exchange API calls. It is driven solely by the database-backed exchange-request
// queue; there is no direct-send path.
//
// PR7 (foundation): no real private clients are wired yet (credential decryption
// and live-execution gating land in a later PR), and AllowLiveExecution defaults
// to false. So this binary cannot place a real order — it claims read-only
// request types only, has no clients to send through, and runs the conservative
// stuck-IN_FLIGHT sweeper. It is safe to run in development.
package main

import (
	"context"
	"fmt"
	"os"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/executor"
	"v3TradeBot/internal/queue"
	"v3TradeBot/internal/service"
)

func main() {
	err := service.RunWithDB("order-executor", service.ConfigFlag(), func(ctx context.Context, base service.Base, store *db.Store) error {
		q := queue.New(store.DB(), clock.NewSystem())

		// No live private clients in PR7. The empty map + AllowLiveExecution=false
		// guarantees no real order can be sent by this dev service.
		clients := map[string]exchanges.PrivateClient{}

		exec := executor.New(store, q, clients, base.Log, executor.Config{
			Name:               "order-executor",
			AllowLiveExecution: false,
		})
		base.Log.Info("order-executor ready (no live clients; live execution disabled)")
		return exec.Run(ctx) // blocks until shutdown
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "order-executor: "+err.Error())
		os.Exit(1)
	}
}
