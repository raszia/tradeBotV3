// Command dashboard serves the operator UI (HTTP initial load + WebSocket live
// updates) and config editing. It runs as its own binary so restarting it never
// affects the trading engine, and it never controls trading directly. In the PR1
// skeleton it serves only a /healthz endpoint; real views land in PR16/PR17.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"v3TradeBot/internal/db"
	"v3TradeBot/internal/service"
)

func main() {
	err := service.RunWithDB("dashboard", service.ConfigFlag(), func(ctx context.Context, base service.Base, store *db.Store) error {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})

		srv := &http.Server{
			Addr:              base.Cfg.Dashboard.ListenAddr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}

		// Run the listener in the background so we can react to shutdown signals.
		errCh := make(chan error, 1)
		go func() {
			base.Log.Info("dashboard http listening", "addr", srv.Addr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				errCh <- err
			}
		}()

		select {
		case <-ctx.Done():
		case err := <-errCh:
			return fmt.Errorf("dashboard http server: %w", err)
		}

		// Graceful shutdown with a bounded timeout.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "dashboard: "+err.Error())
		os.Exit(1)
	}
}
