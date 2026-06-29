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
	"v3TradeBot/internal/credentials"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/executor"
	"v3TradeBot/internal/live"
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
		var guard *live.Guard
		switch base.Cfg.Execution.Mode {
		case config.ExecutionDryRun:
			for _, code := range loadEnabledExchanges(ctx, store) {
				clients[code] = simexec.New(code, simexec.FullFill)
			}
			allowLive = true
			base.Log.Warn("order-executor in DRY-RUN: SIMULATED clients only; no real orders will be sent", "exchanges", len(clients))
		case config.ExecutionLive:
			// LIVE: the safety guard gates every real send; real private clients are
			// built via the factory with DB-decrypted credentials (PR20a). A missing or
			// invalid master key disables credential loading safely → no clients,
			// AllowLiveExecution stays false, nothing is sent.
			guard = live.NewGuard(store.DB(), clock.NewSystem(), base.Log)
			provider, perr := credentials.NewProvider(store.DB(), base.Cfg.Security.MasterKey, clock.NewSystem(), base.Log)
			if perr != nil {
				base.Log.Warn("execution mode 'live': credential loading DISABLED (no/invalid master key); no live clients, nothing will be sent", "err", perr)
				break
			}
			iolog := exchanges.NewIOLogger(exchanges.IOLogConfig{Enabled: true, Source: "order-executor"}, store.DB())
			defer iolog.Close()
			builder := credentials.NewBuilder(store.DB(), provider, iolog)
			for _, code := range builder.LiveEnabledCodes(ctx) {
				client, err := builder.BuildPrivate(ctx, code)
				if err != nil {
					base.Log.Warn("no live client for exchange (skipping; it cannot trade)", "exchange", code, "err", err)
					continue
				}
				clients[code] = client
			}
			allowLive = true // the live guard gates EVERY send; clients exist only for live-enabled exchanges with an active credential
			base.Log.Warn("order-executor in LIVE mode: real clients wired; the live guard gates every order", "exchanges", len(clients))
		default:
			base.Log.Info("order-executor ready (execution off; no clients; live execution disabled)")
		}

		// Startup live-safety summary (no secrets) so an operator can see exactly how
		// dangerous this process is at a glance.
		base.Log.Info("live startup safety", live.BuildSafetySummary(ctx, store.DB(), clock.NewSystem(), base.Cfg.Execution.Mode).LogArgs()...)

		exec := executor.New(store, q, clients, base.Log, executor.Config{
			Name:               "order-executor",
			AllowLiveExecution: allowLive,
			ExecutionMode:      base.Cfg.Execution.Mode,
			Guard:              guard,
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
