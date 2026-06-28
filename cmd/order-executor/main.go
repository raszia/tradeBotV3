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
	"v3TradeBot/internal/config"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/executor"
	"v3TradeBot/internal/queue"
	"v3TradeBot/internal/service"
	"v3TradeBot/internal/simexec"
)

func main() {
	err := service.RunWithDB("order-executor", service.ConfigFlag(), func(ctx context.Context, base service.Base, store *db.Store) error {
		q := queue.New(store.DB(), clock.NewSystem())

		// Execution mode is config-driven (NEVER a runtime env var) and SAFE BY DEFAULT.
		//   off (default): no clients + AllowLiveExecution=false -> no order can be sent.
		//   dry_run: wire SIMULATED clients (simexec) for enabled exchanges -> the full
		//            lifecycle runs with zero real exposure.
		//   live: real private clients (requires decrypted credentials -> a later PR;
		//         until then this falls back to safe 'off' with a warning).
		clients := map[string]exchanges.PrivateClient{}
		allowLive := false
		switch base.Cfg.Execution.Mode {
		case config.ExecutionDryRun:
			for _, code := range loadEnabledExchanges(ctx, store) {
				clients[code] = simexec.New(code, simexec.FullFill)
			}
			allowLive = true
			base.Log.Warn("order-executor in DRY-RUN: SIMULATED clients only; no real orders will be sent", "exchanges", len(clients))
		case config.ExecutionLive:
			base.Log.Warn("execution mode 'live' requested but real credentials are not available yet; running safe (no clients)")
		default:
			base.Log.Info("order-executor ready (execution off; no clients; live execution disabled)")
		}

		exec := executor.New(store, q, clients, base.Log, executor.Config{
			Name:               "order-executor",
			AllowLiveExecution: allowLive,
		})
		return exec.Run(ctx) // blocks until shutdown
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "order-executor: "+err.Error())
		os.Exit(1)
	}
}

// loadEnabledExchanges returns the codes of enabled exchanges (best-effort).
func loadEnabledExchanges(ctx context.Context, store *db.Store) []string {
	rows, err := store.DB().QueryContext(ctx, "SELECT code FROM exchanges WHERE enabled=1")
	if err != nil {
		return nil
	}
	defer rows.Close()
	var codes []string
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err == nil {
			codes = append(codes, code)
		}
	}
	return codes
}
