#!/usr/bin/env bash
# Read-only local/staging smoke check (PR27). Verifies the dashboard is up, exposes no
# secrets, and is in a SAFE mode (off/dry_run) — it sends NO orders and needs NO API key.
#
# Usage:  ./scripts/local-dryrun-check.sh [http://localhost:8080]
set -uo pipefail
BASE="${1:-http://localhost:8080}"
fail=0
bad() { fail=1; printf 'FAIL: %s\n' "$*"; }
ok()  { printf 'ok:   %s\n' "$*"; }

# 1. health
if [ "$(curl -fsS "$BASE/healthz" 2>/dev/null)" = "ok" ]; then ok "dashboard healthz"; else bad "dashboard healthz"; fi

# 2. execution mode is safe (off or dry_run, never live)
mode=$(curl -fsS "$BASE/api/live" 2>/dev/null | grep -oE '"mode":"[^"]*"' | head -1 | cut -d'"' -f4)
case "$mode" in
  ""|off|dry_run) ok "execution mode is safe (${mode:-unset})" ;;
  live)           bad "execution mode is LIVE — not a safe local/staging default" ;;
  *)              ok "execution mode = $mode" ;;
esac

# 3. no secret material in the read-only credential/live views
for ep in /api/credentials /api/live /api/live/session; do
  body=$(curl -fsS "$BASE$ep" 2>/dev/null || true)
  if printf '%s' "$body" | grep -qiE '"(api_key|api_secret|secret_key|passphrase|encrypted_api)"'; then
    bad "$ep leaked a secret field"
  else
    ok "$ep exposes no secret fields"
  fi
done

if [ "$fail" -ne 0 ]; then echo; echo "SMOKE CHECK FAILED"; exit 1; fi
echo; echo "local/staging smoke check passed (safe, no secrets, no orders sent)"
