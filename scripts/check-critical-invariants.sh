#!/usr/bin/env bash
# Critical-invariant static checks (PR26). Scans the source tree for dangerous patterns
# that would violate the system's hard safety boundaries. Exits non-zero on any violation.
#
# Run from the repo root:  ./scripts/check-critical-invariants.sh
# Intended for CI (and mirrored by internal/audit/invariants_test.go so `go test ./...`
# also enforces these).
set -uo pipefail
cd "$(dirname "$0")/.."

fail=0
say()  { printf '%s\n' "$*"; }
bad()  { fail=1; printf 'VIOLATION: %s\n' "$*"; }

# Search only first-party Go source, excluding tests. Drops pure comment lines
# (content after "file:line:" starting with // or * or --) to avoid matching doc text.
src() { grep -rnE "$1" internal cmd --include='*.go' 2>/dev/null | grep -v '_test.go' | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(//|\*|--)'; }

say "== 1. no runtime V3_* env var (only V3_TEST_* allowed, in tests) =="
hits=$(src 'os\.Getenv\("V3_' | grep -v 'V3_TEST_' || true)
[ -n "$hits" ] && { bad "runtime V3_* environment variable used:"; echo "$hits"; } || say "  ok"

say "== 2. PlaceOrder/CancelOrder only in executor / exchange adapters =="
hits=$(src '\.(PlaceOrder|CancelOrder)\(' \
  | grep -vE 'internal/(executor|exchanges|simexec)/' \
  | grep -vE 'PrivateClient|interface\b' || true)
[ -n "$hits" ] && { bad "mutating exchange call outside the order-executor boundary:"; echo "$hits"; } || say "  ok"

say "== 3. no direct ASSIGNMENT of cycles.state / orders.state outside internal/state =="
# Only flag a state ASSIGNMENT (SET state=… or , state=…), NOT a state GUARD in WHERE
# (… AND state='QUEUED'), which is the safe/required pattern for guarded UPDATEs.
hits=$(src "UPDATE (cycles|orders)[^;\"]*(SET +state *=|, *state *=)" | grep -v 'internal/state/' || true)
[ -n "$hits" ] && { bad "direct state ASSIGNMENT outside internal/state:"; echo "$hits"; } || say "  ok"

say "== 4. dashboard never selects encrypted credential blobs =="
hits=$(grep -rnE 'encrypted_api_(key|secret)|encrypted_passphrase' internal/dashboard --include='*.go' 2>/dev/null \
  | grep -v '_test.go' | grep -vE '^[^:]+:[0-9]+:[[:space:]]*(//|\*)' || true)
[ -n "$hits" ] && { bad "dashboard references an encrypted credential blob column:"; echo "$hits"; } || say "  ok"

say "== 5. no logging of secret-named fields (heuristic) =="
# slog/Info/Warn/Error calls with a literal "api_key"/"api_secret"/"passphrase"/"token" key.
hits=$(src '\.(Info|Warn|Error|Debug)\(' | grep -iE '"(api_key|api_secret|passphrase|secret_key|master_key)"' || true)
[ -n "$hits" ] && { bad "secret-named field passed to a logger:"; echo "$hits"; } || say "  ok"

say "== 6. trade-engine / reconciler / balance-sync / health-monitor / dashboard hold no PrivateClient field =="
# These binaries/packages must not embed a mutating client. (PlaceOrder/CancelOrder check in #2
# already covers call sites; this is a defensive scan of the read-only service mains.)
hits=$(grep -rnE 'PrivateClient' cmd/trade-engine cmd/reconciler cmd/balance-sync cmd/health-monitor cmd/dashboard --include='*.go' 2>/dev/null | grep -v '_test.go' || true)
[ -n "$hits" ] && { bad "a read-only service main references PrivateClient:"; echo "$hits"; } || say "  ok"

say "== 7. buyflow stays gated behind Config.PrepareBuyCycles (the engine never calls it unconditionally) =="
# PR9 enables PrepareBuyCycles in cmd/trade-engine, so we no longer forbid that. What must hold
# is that the ENGINE only reaches buyflow through the PrepareBuyCycles gate — i.e. every
# buyflow.CreateBuyCycle/RefreshActiveCycleBuy call in internal/engine is inside prepareBuy,
# which the evaluate() path guards with `e.cfg.PrepareBuyCycles`.
hits=$(src 'buyflow\.(CreateBuyCycle|RefreshActiveCycleBuy)\(' | grep -vE 'internal/engine/engine\.go' || true)
[ -n "$hits" ] && { bad "buyflow entrypoint called outside internal/engine/engine.go (must stay gated in prepareBuy):"; echo "$hits"; } || say "  ok"

if [ "$fail" -ne 0 ]; then
  say ""
  say "CRITICAL INVARIANT CHECK FAILED"
  exit 1
fi
say ""
say "all critical-invariant checks passed"
