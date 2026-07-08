package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// moduleRoot walks up from cwd to the directory containing go.mod.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

// TestProductionExampleIsSafeAndSecretFree (PR27) loads configs/production.example.toml and
// asserts the shipped example is SAFE BY DEFAULT and contains no secret material: execution
// mode is not live, the master key is empty (placeholder), no API key/secret appears, and
// Redacted() masks the DSN + master key.
func TestProductionExampleIsSafeAndSecretFree(t *testing.T) {
	root := moduleRoot(t)
	path := filepath.Join(root, "configs", "production.example.toml")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("production.example.toml must load: %v", err)
	}
	if cfg.Execution.Mode == ExecutionLive {
		t.Errorf("example execution mode = %q; must NOT be live by default", cfg.Execution.Mode)
	}
	if cfg.Execution.Mode != ExecutionOff && cfg.Execution.Mode != ExecutionDryRun && cfg.Execution.Mode != "" {
		t.Errorf("example execution mode = %q; expected off/dry_run only", cfg.Execution.Mode)
	}
	if cfg.Security.MasterKey != "" {
		t.Errorf("example master_key must be an empty placeholder, got %q", cfg.Security.MasterKey)
	}

	// Redaction: the redacted DSN must not leak the DB password, and the master key is masked.
	r := cfg.Redacted()
	if strings.Contains(r.MySQL.DSN, "CHANGE_ME") {
		t.Errorf("Redacted DSN still contains the DB password: %q", r.MySQL.DSN)
	}

	// The file itself contains no api key / secret / plaintext credential.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	low := strings.ToLower(string(raw))
	for _, leak := range []string{"api_key", "api_secret", "secret_key", "apikey", "apisecret"} {
		if strings.Contains(low, leak) {
			t.Errorf("production.example.toml references %q — it must contain NO credentials", leak)
		}
	}
	// master_key must be present as an empty placeholder (= "").
	if !strings.Contains(string(raw), `master_key = ""`) {
		t.Error("production.example.toml should ship master_key as an empty placeholder")
	}
}

// TestProductionExampleDashboardIsLoopbackOnly (PR16) guards the deployment-security
// default: the PR16 dashboard is READ-ONLY but UNAUTHENTICATED, so the shipped example
// must NOT bind it to a public/all-interfaces address. It must be loopback-only (operators
// expose it deliberately, only behind a VPN or authenticated reverse proxy).
func TestProductionExampleDashboardIsLoopbackOnly(t *testing.T) {
	root := moduleRoot(t)
	for _, name := range []string{"production.example.toml", "config.example.toml"} {
		path := filepath.Join(root, "configs", name)
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("%s must load: %v", name, err)
		}
		addr := cfg.Dashboard.ListenAddr
		host := addr
		if i := strings.LastIndex(addr, ":"); i >= 0 {
			host = addr[:i] // strip :port (handles host:port; bare :port -> "")
		}
		loopback := host == "127.0.0.1" || host == "localhost" || host == "::1" || host == "[::1]"
		if !loopback {
			t.Errorf("%s binds the unauthenticated PR16 dashboard to %q (host %q) — must be loopback-only (127.0.0.1/localhost)", name, addr, host)
		}
		// Explicitly reject the classic public-exposure footguns.
		for _, bad := range []string{"0.0.0.0", "::", "[::]"} {
			if host == bad {
				t.Errorf("%s binds the dashboard to %q — public exposure of an unauthenticated dashboard is unsafe", name, addr)
			}
		}
	}
}
