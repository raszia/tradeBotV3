// Command reconciler makes the system safe after restart/timeout/partial-fill/
// disconnect by comparing database state with exchange state through READ-ONLY
// APIs. It never auto-sends or auto-cancels; ambiguous cases become
// NEEDS_RECONCILE for an operator.
//
// PR12 (foundation): no read-only private clients are wired yet (credential
// decryption lands later). The binary runs the startup + periodic reconciliation
// loop; with no clients it inspects DB state and safely leaves orders for
// exchanges it cannot verify (it never auto-closes them).
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"v3TradeBot/internal/db"
	"v3TradeBot/internal/reconciler"
	"v3TradeBot/internal/service"
)

func main() {
	err := service.RunWithDB("reconciler", service.ConfigFlag(), func(ctx context.Context, base service.Base, store *db.Store) error {
		// No read-only clients in PR12. The reconciler cannot place/cancel orders
		// by construction (it holds a ReadOnlyClient interface), and with an empty
		// client set it only inspects and reports.
		clients := map[string]reconciler.ReadOnlyClient{}

		rec := reconciler.New(store, clients, base.Log)
		base.Log.Info("reconciler ready (read-only; no clients wired yet)")
		return rec.RunPeriodic(ctx, 30*time.Second)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "reconciler: "+err.Error())
		os.Exit(1)
	}
}
