// Package symbollock implements the database-backed symbol lock that prevents a
// symbol with an active cycle from accepting a new signal. The lock lives in
// MariaDB (not memory) so it survives process restarts and is recoverable by the
// reconciler.
//
// Lock SCOPE is explicit and composite — it includes the exchange and the
// canonical market (and, later, strategy), e.g. "nobitex|BTC/USDT". So BTC/USDT
// on Nobitex does not block BTC/USDT on Wallex unless a deliberately global scope
// is configured. Acquiring the lock, creating the cycle, and enqueuing the first
// request happen in ONE transaction, so there is never a lock without a cycle or
// a cycle without its registered request. The reconciler reclaims a stale lock
// only after it has positively determined the owning cycle is safe — never on
// lease expiry alone.
//
// Implemented in PR9 (with reclaim logic completed in PR12). Placeholder for the
// PR1 skeleton.
package symbollock
