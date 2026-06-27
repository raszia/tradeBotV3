package state

import (
	"errors"
	"fmt"
)

// Errors returned by validation and Apply. Callers use errors.Is to branch.
var (
	// ErrInvalidTransition: from->to is not a legal transition (or a state is
	// unknown). Returned before any database write.
	ErrInvalidTransition = errors.New("state: invalid transition")
	// ErrStaleVersion: the row exists with the expected current state context but
	// a different version — another writer moved it. The caller should re-read and
	// decide whether to retry.
	ErrStaleVersion = errors.New("state: stale version")
	// ErrStateMismatch: the row's version matches but its state is neither the
	// expected `from` nor the target `to` — the caller's view is inconsistent.
	ErrStateMismatch = errors.New("state: unexpected current state")
	// ErrUnknownRow: no row with the given id exists.
	ErrUnknownRow = errors.New("state: row not found")
)

// ValidateCycleTransition returns nil if from->to is a legal cycle transition,
// else ErrInvalidTransition. NEEDS_RECONCILE is reachable from any non-terminal
// state; nothing may transition out of NEEDS_RECONCILE or out of a terminal state
// here (operator resolution is a separate, later path).
func ValidateCycleTransition(from, to CycleState) error {
	if !IsKnownCycleState(from) || !IsKnownCycleState(to) {
		return fmt.Errorf("%w: unknown cycle state in %q -> %q", ErrInvalidTransition, from, to)
	}
	if cycleCanTransition(from, to) {
		return nil
	}
	return fmt.Errorf("%w: cycle %s -> %s", ErrInvalidTransition, from, to)
}

// ValidateOrderTransition is the order-state analogue of ValidateCycleTransition.
func ValidateOrderTransition(from, to OrderState) error {
	if !IsKnownOrderState(from) || !IsKnownOrderState(to) {
		return fmt.Errorf("%w: unknown order state in %q -> %q", ErrInvalidTransition, from, to)
	}
	if orderCanTransition(from, to) {
		return nil
	}
	return fmt.Errorf("%w: order %s -> %s", ErrInvalidTransition, from, to)
}
