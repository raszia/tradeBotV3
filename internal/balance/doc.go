// Package balance implements continuous balance synchronisation: it polls each
// exchange's balances on a configurable interval and writes a current-balance row
// plus a history row. To avoid unbounded history growth it computes a hash over
// (exchange, asset, available, locked, total) and inserts a history row only when
// that hash changes.
//
// Implemented in PR13. Placeholder for the PR1 skeleton.
package balance
