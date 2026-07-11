// Command reconciler makes the system safe after restart/timeout/partial-fill/
// disconnect by comparing database state with exchange state through READ-ONLY
// APIs. It never auto-sends or auto-cancels; ambiguous cases become
// NEEDS_RECONCILE for an operator.
//
// PR20a: read-only private clients are built via the factory with DB-decrypted
// credentials, but the reconciler holds them through the ReadOnlyClient interface
// (no Place/Cancel reachable). A missing/invalid master key or absent credential keeps
// the client set empty, so it only inspects DB state and safely leaves unverifiable
// orders alone (it never auto-closes/auto-cancels them).
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/credentials"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/reconciler"
	"v3TradeBot/internal/service"
	"v3TradeBot/internal/simexec"
)

func main() {
	err := service.RunWithDB("reconciler", service.ConfigFlag(), func(ctx context.Context, base service.Base, store *db.Store) error {
		// The reconciler cannot place/cancel by construction (it holds a ReadOnlyClient
		// interface). PR19 round 2: it holds BOTH client sets and routes STRICTLY by each
		// cycle's dry_run flag — a dry-run cycle is only ever verified through a simulated
		// client, a real cycle only through a real read-only client, regardless of the process's
		// own execution mode. This makes a cross-mode query impossible even when both a
		// dry-run and a real cycle exist at once.

		// Simulated (dry_run=1) read-only clients — always available (DB-backed, no network, no
		// credentials). They read the SAME persisted sim_exchange_orders the executor wrote.
		simClients := map[string]reconciler.ReadOnlyClient{}
		for _, code := range enabledExchanges(ctx, store) {
			simClients[code] = simexec.New(store.DB(), code, simexec.FullFill)
		}

		// Real (dry_run=0) read-only clients — built from DB-decrypted credentials where
		// available; a missing/invalid master key or absent credential leaves the set empty, so
		// real cycles are inspect-only (never verified through the simulator).
		realClients := map[string]reconciler.ReadOnlyClient{}
		if provider, perr := credentials.NewProvider(store.DB(), base.Cfg.Security.MasterKey, clock.NewSystem(), base.Log); perr != nil {
			base.Log.Warn("reconciler: real read-only clients disabled (no/invalid master key); real cycles inspect-only", "err", perr)
		} else {
			iolog := exchanges.NewIOLogger(exchanges.IOLogConfig{Enabled: true, Source: "reconciler"}, store.DB())
			defer iolog.Close()
			builder := credentials.NewBuilder(store.DB(), provider, iolog)
			for _, code := range builder.EnabledCodes(ctx) {
				client, err := builder.BuildPrivate(ctx, code)
				if err != nil {
					continue
				}
				realClients[code] = client // narrowed to ReadOnlyClient (no place/cancel reachable)
			}
		}

		rec := reconciler.New(store, realClients, simClients, base.Log)
		base.Log.Info("reconciler ready (read-only)", "real_clients", len(realClients), "sim_clients", len(simClients), "mode", base.Cfg.Execution.Mode)
		return rec.RunPeriodic(ctx, 30*time.Second)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "reconciler: "+err.Error())
		os.Exit(1)
	}
}

// enabledExchanges returns the codes of enabled exchanges (best-effort).
func enabledExchanges(ctx context.Context, store *db.Store) []string {
	rows, err := store.DB().QueryContext(ctx, "SELECT code FROM exchanges WHERE enabled=1")
	if err != nil {
		return nil
	}
	defer rows.Close()
	var codes []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err == nil {
			codes = append(codes, c)
		}
	}
	return codes
}
