// Command order-executor is the ONLY component that issues order-mutating
// exchange API calls. It is driven solely by the database-backed exchange-request
// queue; there is no direct-send path.
//
// This binary CAN place real orders: in `live` mode it builds real private clients from
// DB-decrypted credentials and the live.Guard is the last check before each real venue
// mutation. Safety comes from configuration, not from missing wiring — `off` (the default)
// wires no clients and leaves AllowLiveExecution false, and `dry_run` wires simulated
// (simexec) clients, so neither can touch a venue.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/config"
	"v3TradeBot/internal/configstore"
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
		//   live: REAL private clients built from DB-decrypted credentials -> real orders are
		//         possible; every send is gated by the live.Guard. A missing/invalid master
		//         key or credential yields no client for that exchange (it cannot trade).
		clients := map[string]exchanges.PrivateClient{}
		allowLive := false
		var guard *live.Guard
		// The sink must exist BEFORE the private clients are built (they capture it) and is
		// bound to the executor by executor.New below.
		rlSink := executor.NewRateLimitSink()
		switch base.Cfg.Execution.Mode {
		case config.ExecutionDryRun:
			for _, code := range loadEnabledExchanges(ctx, store) {
				clients[code] = simexec.New(store.DB(), code, simexec.FullFill)
			}
			allowLive = true
			base.Log.Warn("order-executor in DRY-RUN: SIMULATED clients only; no real orders will be sent", "exchanges", len(clients))
		case config.ExecutionLive:
			// LIVE: the safety guard gates every real send; real private clients are built
			// via the factory with DB-decrypted credentials (wiring from §16d). A missing or
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
			// PR20 correction #6: throttle headers on a SUCCESSFUL response must pause FUTURE
			// requests without disturbing the completed operation — the sink parks the
			// exchange's reactive cooldown.
			builder := credentials.NewBuilder(store.DB(), provider, iolog).WithRateLimitSink(rlSink)
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

		// PR20 #7: DB-backed per-exchange operational tuning, served from the copy-on-write
		// configstore cache: rate_limit_per_sec drives the executor's PROACTIVE pacer;
		// retry_backoff_ms is the per-exchange REACTIVE fallback cooldown when a throttled
		// venue provides no wait duration. Previously these fields were stored/edited but
		// consumed by nothing.
		//
		// PR20 correction #4: the FIRST load is synchronous and validated, executed by the
		// executor before it claims anything (Config.StartupLoad). Loading it in the
		// background would let early requests run with uninitialized zeros (no pacing, wrong
		// fallback). Only the periodic REFRESH is asynchronous, and it may fail safely — the
		// cache keeps its last good snapshot.
		cfgStore := configstore.New(store.DB())
		cache := configstore.NewCache()
		liveMode := base.Cfg.Execution.IsLive()
		validate := func(s *configstore.Snapshot) error { return validateTuning(s, clients, liveMode) }
		startupLoad := func(ctx context.Context) error {
			// Initial load is validated BEFORE it becomes active (PR20 correction #1).
			if err := cache.ReloadValidated(ctx, cfgStore, validate); err != nil {
				return fmt.Errorf("initial exchange-tuning load: %w", err)
			}
			// Periodic refresh starts only AFTER a good initial snapshot exists, and it does
			// NOT immediately reload again (no redundant/unvalidated second load). Every
			// periodic reload is validated; an invalid one keeps the last known-good snapshot,
			// so a removed/negative exchange_configs row can never silently disable pacing.
			go cache.RunValidated(ctx, cfgStore, 30*time.Second, validate, func(e error) {
				base.Log.Warn("config reload rejected (keeping last known-good snapshot)", "err", e)
			})
			return nil
		}
		tuningFor := func(code string) (int, time.Duration) {
			snap := cache.Snapshot()
			if snap == nil {
				return 0, 0
			}
			ec, ok := snap.Exchanges[code]
			if !ok {
				return 0, 0
			}
			return ec.RateLimitPerSec, time.Duration(ec.RetryBackoffMs) * time.Millisecond
		}

		// The ambiguous-mutation recovery window is RUNTIME-CONFIGURED from the bootstrap
		// [execution.recovery] section (validated at startup; per-exchange partial overrides
		// inherit the configured global values) — never hard-coded in production.
		recGlobal, recPer := recoveryFromConfig(base.Cfg.Execution)
		base.Log.Info("ambiguous-recovery window",
			"max_attempts", recGlobal.MaxAttempts, "initial_delay", recGlobal.InitialDelay,
			"max_delay", recGlobal.MaxDelay, "total_timeout", recGlobal.TotalTimeout,
			"per_exchange_overrides", len(recPer))

		exec := executor.New(store, q, clients, base.Log, executor.Config{
			Name:                "order-executor",
			AllowLiveExecution:  allowLive,
			ExecutionMode:       base.Cfg.Execution.Mode,
			Guard:               guard,
			Recovery:            recGlobal,
			RecoveryPerExchange: recPer,
			ExchangeTuningFor:   tuningFor,
			RateLimitSink:       rlSink,
			StartupLoad:         startupLoad,
		})
		return exec.Run(ctx) // blocks until shutdown
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "order-executor: "+err.Error())
		os.Exit(1)
	}
}

// validateTuning checks the initial per-exchange tuning snapshot before any request is sent
// (PR20 correction #4). In LIVE mode every wired exchange must have an `exchange_configs`
// row: without one, `tuningFor` would silently return zeros and a real order would be paced
// and backed off with uninitialized values. That is a startup failure, not a default. Values
// must also be sane (negatives would produce a nonsense interval/backoff). In non-live modes
// a missing row is tolerated (nothing real is at stake) and simply means "no pacing".
func validateTuning(snap *configstore.Snapshot, clients map[string]exchanges.PrivateClient, live bool) error {
	if snap == nil {
		return errors.New("exchange tuning snapshot is empty after the initial load")
	}
	for code := range clients {
		ec, ok := snap.Exchanges[code]
		if !ok {
			if live {
				return fmt.Errorf("exchange %q has no exchange_configs row: refusing to trade live with "+
					"uninitialized rate_limit_per_sec/retry_backoff_ms", code)
			}
			continue
		}
		if ec.RateLimitPerSec < 0 || ec.RetryBackoffMs < 0 {
			return fmt.Errorf("exchange %q has invalid tuning (rate_limit_per_sec=%d, retry_backoff_ms=%d)",
				code, ec.RateLimitPerSec, ec.RetryBackoffMs)
		}
	}
	return nil
}

// recoveryFromConfig converts the validated bootstrap [execution.recovery] section into the
// executor's recovery types: the resolved GLOBAL window plus fully-resolved per-exchange
// overrides (partial overrides already inherit the configured global values inside
// config.ResolvedRecovery — never hard-coded defaults). Kept as a separate function so the
// config→executor wiring is unit-testable.
func recoveryFromConfig(ec config.ExecutionConfig) (executor.RecoveryConfig, map[string]executor.RecoveryConfig) {
	g, per := ec.ResolvedRecovery()
	toExec := func(v config.ExecutionRecoveryValues) executor.RecoveryConfig {
		return executor.RecoveryConfig{
			MaxAttempts:  v.MaxAttempts,
			InitialDelay: v.InitialDelay,
			MaxDelay:     v.MaxDelay,
			TotalTimeout: v.TotalTimeout,
		}
	}
	var perExec map[string]executor.RecoveryConfig
	if len(per) > 0 {
		perExec = make(map[string]executor.RecoveryConfig, len(per))
		for code, v := range per {
			perExec[code] = toExec(v)
		}
	}
	return toExec(g), perExec
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
