// Package dashboard implements the operator UI server.
//
// PR16 provides a STRICTLY READ-ONLY operational dashboard: HTTP endpoints for the
// initial load of recent cycles/orders/fills/signals/comparison-events/balances/health/
// API-logs/app-logs/regime/config, plus a snapshot-only WebSocket for live updates. It
// only SELECTs and displays; it never places, cancels, or reprices orders, and never
// mutates cycles/orders/queue/locks/config. The WebSocket takes no commands from the
// browser and permits only same-origin connections.
//
// Config editing and operator actions are NOT part of PR16 — they are planned for PR17
// and must be implemented there with explicit authentication, authorization, audit, and
// safety controls. (Config editing is not removed from the project plan; it is simply
// out of scope for the read-only PR16.)
//
// HARD RULE: the dashboard does not control trading. It runs as its own binary so
// restarting it never affects the trading engine/executor/reconciler flow.
//
// Read-only dashboard implemented in PR16; config editing/operator actions in PR17.
package dashboard
