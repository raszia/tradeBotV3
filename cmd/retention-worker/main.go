// Command retention-worker deletes OLD rows from high-volume operational tables
// (api_call_logs, comparison_events, exchange_health_samples, app_logs,
// wallet_balance_history, market_regime_history) in bounded batches, driven entirely
// by DB config (retention_settings). It NEVER deletes permanent trading records, makes
// no exchange calls, and uses no Redis. Only one worker runs at a time (DB advisory
// lock). Pass -dry-run to report what WOULD be deleted without deleting anything.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/retention"
	"v3TradeBot/internal/service"
)

func main() {
	// Dry-run is an execution MODE of the worker (not a trading parameter), selected by
	// a command-line flag — never a runtime environment variable. Registered before
	// ConfigFlag() so the shared flag.Parse() picks it up.
	dryRunFlag := flag.Bool("dry-run", false, "report what retention WOULD delete, without deleting anything")

	err := service.RunWithDB("retention-worker", service.ConfigFlag(), func(ctx context.Context, base service.Base, store *db.Store) error {
		dryRun := *dryRunFlag
		w := retention.New(store.DB(), clock.NewSystem(), base.Log, retention.Config{})
		base.Log.Info("retention-worker starting", "dry_run", dryRun)

		// Run once on startup, then periodically. RunOnce self-guards with the advisory
		// lock, so overlapping schedules across processes are safe.
		runOnce := func() {
			if _, err := w.RunOnce(ctx, dryRun); err != nil {
				base.Log.Warn("retention run failed", "err", err)
			}
		}
		runOnce()
		t := time.NewTicker(6 * time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				runOnce()
			}
		}
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "retention-worker: "+err.Error())
		os.Exit(1)
	}
}
