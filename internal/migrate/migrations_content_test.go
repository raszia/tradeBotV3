package migrate

import (
	"regexp"
	"strings"
	"testing"
)

// expectedTables are every table the PR2 schema must define (excludes
// schema_migrations, which the runner creates, and app_meta from PR1).
var expectedTables = []string{
	// 002 reference
	"exchanges", "assets", "markets", "exchange_markets",
	// 003 credentials
	"exchange_credentials", "exchange_credential_audit",
	// 004 config
	"config_versions", "config_change_audit", "symbol_configs", "exchange_configs",
	"exchange_fees", "retention_settings",
	// 005 trading core
	"cycles", "cycle_state_events", "orders", "order_events", "fills",
	"symbol_locks", "exchange_requests", "cycle_fee_snapshots",
	// 006 observability
	"wallet_balances_current", "wallet_balance_history", "exchange_health_current",
	"exchange_health_samples", "api_call_logs", "app_logs", "comparison_events", "signals",
	// 007 discovery
	"market_discovery_runs",
}

// allMigrationSQL concatenates the embedded migration files for static checks
// that do not need a database.
func allMigrationSQL(t *testing.T) string {
	t.Helper()
	migs, err := parse(FS)
	if err != nil {
		t.Fatalf("parse embedded migrations: %v", err)
	}
	var sb strings.Builder
	for _, m := range migs {
		sb.WriteString(m.SQL)
		sb.WriteString("\n")
	}
	return sb.String()
}

func TestEmbeddedMigrationsParseAndAreContiguousDDL(t *testing.T) {
	migs, err := parse(FS)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(migs) < 7 {
		t.Fatalf("expected at least 7 migrations, got %d", len(migs))
	}
	for i, m := range migs {
		if m.Version != i+1 {
			t.Errorf("migration %d has version %d (expected contiguous numbering)", i, m.Version)
		}
		if m.Kind != KindDDL {
			t.Errorf("%03d_%s: kind = %s, want ddl", m.Version, m.Name, m.Kind)
		}
	}
}

func TestEmbeddedMigrationsDefineExpectedTables(t *testing.T) {
	all := allMigrationSQL(t)
	for _, tbl := range expectedTables {
		if !strings.Contains(all, "CREATE TABLE IF NOT EXISTS "+tbl+" (") {
			t.Errorf("missing CREATE TABLE for %q", tbl)
		}
	}
}

func TestNoPlaintextCredentialColumns(t *testing.T) {
	all := allMigrationSQL(t)
	// A column whose name is exactly api_key/api_secret/passphrase would appear as
	// the first token on its own line. encrypted_api_key etc. start with
	// "encrypted_" and must NOT trip this.
	bad := regexp.MustCompile(`(?m)^\s*(api_key|api_secret|passphrase)\b`)
	if loc := bad.FindString(all); loc != "" {
		t.Errorf("found a plaintext credential column definition: %q", strings.TrimSpace(loc))
	}
	for _, want := range []string{"encrypted_api_key", "encrypted_api_secret", "encrypted_passphrase"} {
		if !strings.Contains(all, want) {
			t.Errorf("expected encrypted column %q", want)
		}
	}
}

func TestExchangeRequestStatusEnumIsStable(t *testing.T) {
	all := allMigrationSQL(t)
	// The queue status enum must stay consistent system-wide.
	want := "ENUM('QUEUED','CLAIMED','IN_FLIGHT','SUCCEEDED','FAILED','RETRY_SCHEDULED','DEAD')"
	if !strings.Contains(all, want) {
		t.Errorf("exchange_requests status enum changed; expected %s", want)
	}
}
