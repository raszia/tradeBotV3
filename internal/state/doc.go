// Package state is the deterministic state machine for trading cycles and orders.
//
// Cycle and order states change ONLY through the validated transitions here; no
// other package mutates the state column directly with ad-hoc SQL. This keeps the
// lifecycle deterministic and every change auditable.
//
// Contents:
//   - CycleState / OrderState enums + the authoritative transition maps, plus
//     RequestStatus constants (the queue vocabulary, for system-wide consistency).
//   - ValidateCycleTransition / ValidateOrderTransition — pure legality checks.
//   - ApplyCycleTransition / ApplyOrderTransition — within a caller-supplied
//     *sql.Tx, perform an optimistic-concurrency (version-guarded) UPDATE and, in
//     the SAME transaction, insert a state-event row. A zero-row update is
//     disambiguated into replay (already in target → idempotent no-op), stale
//     version, state mismatch, or missing row.
//
// Safety rules enforced: transitions are deterministic; the state update and its
// event insert are atomic; replays do not double-write events; terminal states
// have no outgoing transitions; NEEDS_RECONCILE is reachable from any non-terminal
// state but has NO automatic exit (operator/reconciler resolution is a separate,
// later path).
package state
