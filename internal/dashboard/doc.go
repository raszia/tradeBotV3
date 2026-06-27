// Package dashboard implements the operator UI server: HTTP for initial loads of
// recent cycles/orders/fills/signals/comparison-events/balances/health/API-logs/
// regime, and WebSocket for live updates. It also serves config-editing endpoints
// whose changes create new versioned config / auditable change records.
//
// HARD RULE: the dashboard does not control trading. It reads state, shows logs,
// and edits config; it never places, cancels, or reprices orders directly — that
// stays inside the engine/executor/reconciler flow. It runs as its own binary so
// restarting it never affects the trading engine.
//
// Implemented in PR16/PR17. Placeholder for the PR1 skeleton.
package dashboard
