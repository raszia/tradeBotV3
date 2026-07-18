// Command dashboard serves the operator dashboard. PR16 read-only views + snapshot-only
// WebSocket; PR17 adds login/session authentication + versioned, audited config editing.
// It runs as its own binary so restarting it never affects the trading services, and it
// never controls trading — it holds only a database handle + the versioned config store
// (no exchange client, no queue). It never places/cancels orders or mutates cycles/orders/
// queue/locks.
//
// Bootstrap the first admin user (one-off, then it exits). The PASSWORD is NEVER passed on
// the command line (it would leak via shell history / ps / logs). It is read from a hidden
// TTY prompt (with confirmation), or from stdin when not a terminal:
//
//	./bin/dashboard -config configs/config.toml -create-user alice:admin        # prompts (hidden)
//	printf '%s\n' "$PW" | ./bin/dashboard -config configs/config.toml -create-user alice:admin
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"v3TradeBot/internal/dashboard"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/service"
)

func main() {
	// -create-user is a one-off maintenance action (username:role — NO password). Registered
	// before ConfigFlag() so the shared flag.Parse() picks it up. Never an env var.
	createUser := flag.String("create-user", "", "create a dashboard user then exit (format username:role; password read from a hidden prompt or stdin)")

	err := service.RunWithDB("dashboard", service.ConfigFlag(), func(ctx context.Context, base service.Base, store *db.Store) error {
		cfg := dashboard.Config{
			SecureCookies: base.Cfg.Dashboard.SecureCookies,
			SessionTTL:    time.Duration(base.Cfg.Dashboard.SessionTTLMinutes) * time.Minute,
			// PR22: the master key enables credential provisioning (create/validate/activate/
			// disable). Absent/invalid → credential writes are disabled (no plaintext fallback).
			MasterKey: base.Cfg.Security.MasterKey,
		}
		srv := dashboard.New(store.DB(), base.Log, cfg)

		if *createUser != "" {
			return provisionUser(ctx, srv, *createUser)
		}

		httpSrv := &http.Server{
			Addr:              base.Cfg.Dashboard.ListenAddr,
			Handler:           srv.Handler(),
			ReadHeaderTimeout: 5 * time.Second,
		}

		// Run the listener in the background so we can react to shutdown signals.
		errCh := make(chan error, 1)
		go func() {
			base.Log.Info("dashboard http listening", "addr", httpSrv.Addr)
			if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
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
		return httpSrv.Shutdown(shutdownCtx)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "dashboard: "+err.Error())
		os.Exit(1)
	}
}

// provisionUser creates one dashboard user from a "username:role" spec, reading the
// password securely (NOT from the command line), then the binary exits.
func provisionUser(ctx context.Context, srv *dashboard.Server, spec string) error {
	parts := strings.SplitN(spec, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("-create-user must be username:role (the password is read from a hidden prompt or stdin, never the command line)")
	}
	username, role := parts[0], parts[1]
	pw, err := readPassword()
	if err != nil {
		return err
	}
	id, err := srv.CreateUser(ctx, username, pw, role)
	if err != nil {
		return fmt.Errorf("create user: %w", err)
	}
	fmt.Printf("created dashboard user %q (role %s, id %d)\n", username, role, id)
	return nil
}

// readPassword reads the new user's password from STDIN — never from the command line, so
// it can't leak via shell history / ps / logs. When stdin is an interactive terminal it
// disables echo (best-effort, via `stty`) and asks twice, requiring a match; otherwise it
// reads a single line (so the password can be piped or redirected from a securely
// permissioned file). No external Go dependency is used.
func readPassword() (string, error) {
	in := bufio.NewReader(os.Stdin)
	if isTerminal(os.Stdin) {
		restore, _ := disableEcho() // best-effort; if it fails we still read (visible)
		defer restore()
		fmt.Fprint(os.Stderr, "New password: ")
		p1, err := readLine(in)
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return "", err
		}
		fmt.Fprint(os.Stderr, "Confirm password: ")
		p2, err := readLine(in)
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return "", err
		}
		if p1 != p2 {
			return "", fmt.Errorf("passwords do not match")
		}
		return p1, nil
	}
	line, err := readLine(in)
	if err != nil {
		return "", fmt.Errorf("no password provided on stdin: %w", err)
	}
	return line, nil
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	line = strings.TrimRight(line, "\r\n")
	if err != nil && line == "" {
		return "", err
	}
	return line, nil
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && (fi.Mode()&os.ModeCharDevice) != 0
}

// disableEcho best-effort turns off terminal echo via `stty` and returns a restore func.
// If stty is unavailable it is a no-op (the password is still read from stdin, just visible).
func disableEcho() (restore func(), err error) {
	stty := func(args ...string) error {
		c := exec.Command("stty", args...)
		c.Stdin = os.Stdin
		return c.Run()
	}
	if err := stty("-echo"); err != nil {
		return func() {}, err
	}
	return func() { _ = stty("echo") }, nil
}
