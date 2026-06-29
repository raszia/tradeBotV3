// Package audit holds static, source-scanning invariant tests (PR26). They walk the
// first-party Go source and fail if a hard safety boundary is violated — mirroring
// scripts/check-critical-invariants.sh so `go test ./...` enforces the same rules in CI.
// These tests need no database and no network.
package audit

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// repoRoot walks up from the test's working directory until it finds go.mod.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found walking up from cwd")
		}
		dir = parent
	}
}

type srcLine struct {
	path string
	num  int
	text string
}

// scan returns every non-test, non-blank Go source line under internal/ and cmd/.
func scan(t *testing.T) []srcLine {
	t.Helper()
	root := repoRoot(t)
	var out []srcLine
	for _, base := range []string{"internal", "cmd"} {
		err := filepath.Walk(filepath.Join(root, base), func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			for i, line := range strings.Split(string(b), "\n") {
				out = append(out, srcLine{path: filepath.ToSlash(rel), num: i + 1, text: line})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(out) == 0 {
		t.Fatal("scanned no source — repo layout changed?")
	}
	return out
}

// isComment reports whether a source line is a pure comment (so doc text never trips a check).
func isComment(s string) bool {
	t := strings.TrimSpace(s)
	return strings.HasPrefix(t, "//") || strings.HasPrefix(t, "*") || strings.HasPrefix(t, "/*")
}

func TestNoRuntimeV3EnvVar(t *testing.T) {
	re := regexp.MustCompile(`os\.Getenv\("V3_`)
	for _, l := range scan(t) {
		if isComment(l.text) {
			continue
		}
		if re.MatchString(l.text) && !strings.Contains(l.text, "V3_TEST_") {
			t.Errorf("%s:%d uses a runtime V3_* env var (only V3_TEST_* in tests is allowed): %s", l.path, l.num, strings.TrimSpace(l.text))
		}
	}
}

func TestMutatingExchangeCallsConfinedToExecutor(t *testing.T) {
	re := regexp.MustCompile(`\.(PlaceOrder|CancelOrder)\(`)
	for _, l := range scan(t) {
		if isComment(l.text) || strings.Contains(l.text, "interface") {
			continue
		}
		if !re.MatchString(l.text) {
			continue
		}
		// Allowed only inside the order-executor + the exchange adapters/simulator.
		if strings.HasPrefix(l.path, "internal/executor/") ||
			strings.HasPrefix(l.path, "internal/exchanges/") ||
			strings.HasPrefix(l.path, "internal/simexec/") {
			continue
		}
		t.Errorf("%s:%d calls a mutating exchange method outside the executor boundary: %s", l.path, l.num, strings.TrimSpace(l.text))
	}
}

func TestNoDirectStateUpdates(t *testing.T) {
	// Direct `UPDATE cycles|orders ... SET ... state` outside internal/state means a
	// transition bypassed the state machine (CAS + event row).
	re := regexp.MustCompile(`(?i)UPDATE\s+(cycles|orders)\b[^"';]*SET[^"';]*\bstate\b`)
	for _, l := range scan(t) {
		if isComment(l.text) || strings.HasPrefix(l.path, "internal/state/") {
			continue
		}
		if re.MatchString(l.text) {
			t.Errorf("%s:%d updates cycles/orders state directly (must go through internal/state): %s", l.path, l.num, strings.TrimSpace(l.text))
		}
	}
}

func TestDashboardNeverSelectsCredentialBlobs(t *testing.T) {
	re := regexp.MustCompile(`encrypted_api_(key|secret)|encrypted_passphrase`)
	for _, l := range scan(t) {
		if isComment(l.text) || !strings.HasPrefix(l.path, "internal/dashboard/") {
			continue
		}
		if re.MatchString(l.text) {
			t.Errorf("%s:%d references an encrypted credential blob column (dashboard must show status only): %s", l.path, l.num, strings.TrimSpace(l.text))
		}
	}
}

func TestNoSecretNamedLogFields(t *testing.T) {
	logCall := regexp.MustCompile(`\.(Info|Warn|Error|Debug)\(`)
	secretKey := regexp.MustCompile(`"(api_key|api_secret|secret_key|passphrase|master_key)"`)
	for _, l := range scan(t) {
		if isComment(l.text) {
			continue
		}
		if logCall.MatchString(l.text) && secretKey.MatchString(l.text) {
			t.Errorf("%s:%d passes a secret-named field to a logger: %s", l.path, l.num, strings.TrimSpace(l.text))
		}
	}
}

func TestReadOnlyServiceMainsHoldNoPrivateClient(t *testing.T) {
	for _, l := range scan(t) {
		if isComment(l.text) {
			continue
		}
		readOnlyMain := strings.HasPrefix(l.path, "cmd/trade-engine/") ||
			strings.HasPrefix(l.path, "cmd/reconciler/") ||
			strings.HasPrefix(l.path, "cmd/balance-sync/") ||
			strings.HasPrefix(l.path, "cmd/health-monitor/") ||
			strings.HasPrefix(l.path, "cmd/dashboard/")
		if readOnlyMain && strings.Contains(l.text, "PrivateClient") {
			t.Errorf("%s:%d a read-only service main references PrivateClient: %s", l.path, l.num, strings.TrimSpace(l.text))
		}
	}
}
