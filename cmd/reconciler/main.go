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
)

func main() {
	err := service.RunWithDB("reconciler", service.ConfigFlag(), func(ctx context.Context, base service.Base, store *db.Store) error {
		// The reconciler cannot place/cancel by construction (it holds a ReadOnlyClient
		// interface). Build credentialed read-only clients where available; no/invalid
		// master key → empty set → inspect + report only.
		clients := map[string]reconciler.ReadOnlyClient{}
		if provider, perr := credentials.NewProvider(store.DB(), base.Cfg.Security.MasterKey, clock.NewSystem(), base.Log); perr != nil {
			base.Log.Warn("reconciler: read-only clients disabled (no/invalid master key); inspect-only", "err", perr)
		} else {
			iolog := exchanges.NewIOLogger(exchanges.IOLogConfig{Enabled: true, Source: "reconciler"}, store.DB())
			defer iolog.Close()
			builder := credentials.NewBuilder(store.DB(), provider, iolog)
			for _, code := range builder.EnabledCodes(ctx) {
				client, err := builder.BuildPrivate(ctx, code)
				if err != nil {
					continue
				}
				clients[code] = client // narrowed to ReadOnlyClient (no place/cancel reachable)
			}
		}

		rec := reconciler.New(store, clients, base.Log)
		base.Log.Info("reconciler ready (read-only)", "clients", len(clients))
		return rec.RunPeriodic(ctx, 30*time.Second)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "reconciler: "+err.Error())
		os.Exit(1)
	}
}
