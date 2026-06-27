// Package reconciler makes the system safe after restart, timeout, partial fill,
// WebSocket disconnect, or Redis/DB divergence. It is part of the first usable
// version, not an afterthought.
//
// On startup it loads open cycles and orders from MariaDB, polls each exchange
// for open orders, balances and recent fills, compares DB state with exchange
// state, and per cycle decides: continue / mark NEEDS_RECONCILE / safe-close.
// Locks are released only when it is provably safe. The reconciler NEVER
// auto-cancels and NEVER auto-sends orders; the safe default for any ambiguity
// is NEEDS_RECONCILE. Startup reconciliation is poll-only (WebSocket gives no
// history for the downtime gap); a periodic poll backstop complements steady-
// state WebSocket updates.
//
// Implemented in PR12. Placeholder for the PR1 skeleton.
package reconciler
