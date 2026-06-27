// Package state is the deterministic state machine for trading cycles and
// orders. Cycle and order states change ONLY through transitions defined here;
// no code mutates a state column directly.
//
// It will define CycleState/OrderState enums, the valid-transition maps, a
// Transition(from,to) validator, and ApplyCycleTransition/ApplyOrderTransition
// helpers that — inside a caller-supplied *sql.Tx — update the row guarded by an
// optimistic-concurrency version column AND insert a state-event row, so every
// transition is persisted and double-applies are rejected. NEEDS_RECONCILE is
// reachable from any non-terminal state and is exited only by an operator path.
//
// Implemented in PR3. This file is a placeholder for the PR1 skeleton.
package state
