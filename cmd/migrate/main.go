// Command migrate applies all embedded database migrations and exits. It is the
// only component that writes schema in normal operation (service binaries verify
// the schema with EnsureCurrent and refuse to self-migrate). Run it during
// deploy/CI before rolling the service binaries.
package main

import (
	"fmt"
	"os"

	"v3TradeBot/internal/db"
	"v3TradeBot/internal/migrate"
	"v3TradeBot/internal/service"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "migrate: "+err.Error())
		os.Exit(1)
	}
}

func run() error {
	base, err := service.Bootstrap("migrate", service.ConfigFlag())
	if err != nil {
		return err
	}
	ctx, stop := service.SignalContext()
	defer stop()

	store, err := db.New(ctx, base.Cfg.MySQL)
	if err != nil {
		return fmt.Errorf("connect mysql: %w", err)
	}
	defer store.Close()

	res, err := migrate.Run(ctx, store.DB(), migrate.FS)
	if err != nil {
		return err
	}
	if len(res.Applied) == 0 {
		base.Log.Info("schema already current", "version", res.EndVersion)
		return nil
	}
	names := make([]string, len(res.Applied))
	for i, m := range res.Applied {
		names[i] = fmt.Sprintf("%03d_%s", m.Version, m.Name)
	}
	base.Log.Info("migrations applied", "from", res.StartVersion, "to", res.EndVersion, "applied", names)
	return nil
}
