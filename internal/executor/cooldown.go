package executor

// Per-exchange REACTIVE cooldown + PROACTIVE pacer (PR20 corrections #4/#7).
//
// Reactive cooldown: when a venue reports throttling (any signal the adapters normalize
// into a CatRateLimit error — HTTP 429, other statuses, HTTP-200 business codes, headers, or
// throttle headers on a SUCCESSFUL response), the affected exchange is PARKED: the executor
// claims and sends NOTHING for it until the cooldown expires. Unrelated exchanges are
// unaffected. The deadline is absolute and EXTEND-ONLY (a later, longer wait extends it; a
// shorter one never shortens it — iranArb-proven rule), bounded by a maximum, mutex-guarded
// for in-process concurrency, and observable via safe logs (exchange, reason, source,
// cooldown-until; never bodies, tokens, or credentials). Parking is poll-driven (the claim
// loop simply skips the parked exchange each tick) — no busy loop and no goroutine sleeps
// holding claims.
//
// DURABILITY (PR20 correction #5): the deadline is persisted to `exchange_cooldowns` on every
// arm and reloaded at executor startup, so a restart cannot resume sending to a venue that is
// still inside its cooldown. The in-memory map is a write-through cache of that table (safe
// under the single-instance design); the DB row is the record that survives the process.
//
// Proactive pacer: a per-exchange minimum send interval derived from the operational
// config `exchange_configs.rate_limit_per_sec` (PR20 #7 — the field is now WIRED, not
// dead). Pacing PREVENTS exceeding a known budget; the reactive cooldown responds AFTER
// the venue reports throttling. They are deliberately separate mechanisms.

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/queue"
)

// Defaults for the reactive cooldown (iranArb-proven values: nobitex fallback 60s, cap 15m).
const (
	defaultRateLimitFallbackCooldown = 60 * time.Second
	defaultRateLimitMaxCooldown      = 15 * time.Minute
	// cooldownPersistTimeout bounds a single durable write.
	cooldownPersistTimeout = 5 * time.Second
	// Bounded backoff for the persistence worker's retries.
	cooldownPersistMinBackoff = 250 * time.Millisecond
	cooldownPersistMaxBackoff = 10 * time.Second
	// defaultCooldownPersistGrace is how long a park may stay un-persisted before live
	// ENTRY BUYS for that exchange are disabled (fail closed rather than pretend it is durable).
	defaultCooldownPersistGrace = 30 * time.Second
	// defaultShutdownFlushTimeout bounds the graceful-shutdown cooldown flush.
	defaultShutdownFlushTimeout = 5 * time.Second
)

type cooldownEntry struct {
	until  time.Time
	reason string // safe, short (error category/code — never response bodies)
	source string // status | header | body | code | fallback
	// Persistence state is tracked SEPARATELY from the deadline (PR20 correction #3).
	// Deriving "needs persisting" from "the deadline was just extended" loses writes: if the
	// first write fails and the next signal carries an equal/shorter deadline (extended =
	// false), the row would never be retried and a restart would lose the cooldown. Instead
	// the invariant is `persistedUntil == until`; anything else is pending, forever, until it
	// succeeds or the failure policy disables live execution.
	persistedUntil time.Time // what is durably stored in exchange_cooldowns
	pendingSince   time.Time // when this entry first became un-persisted (zero = in sync)
	lastErr        string    // last persistence error (safe: DB error text, never venue data)
}

// needsPersist reports whether the durable row is behind the in-memory deadline.
func (e cooldownEntry) needsPersist() bool { return e.until.After(e.persistedUntil) }

// cooldowns tracks per-exchange park deadlines. Safe for concurrent use.
type cooldowns struct {
	mu      sync.Mutex
	entries map[string]cooldownEntry
}

func newCooldowns() *cooldowns { return &cooldowns{entries: map[string]cooldownEntry{}} }

// arm parks the exchange until `until` (extend-only: an earlier deadline never shortens an
// active later one) and marks the entry pending-persist if the durable row is now behind.
// Returns the effective deadline and whether this call extended it. It NEVER does I/O — the
// caller may be an exchange HTTP response path (PR20 correction #2).
func (c *cooldowns) arm(code string, until time.Time, reason, source string, now time.Time) (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.entries[code]
	extended := until.After(cur.until)
	if extended {
		cur.until, cur.reason, cur.source = until, reason, source
	}
	// Pending is derived from the persisted-vs-active gap, NOT from `extended`: a previously
	// failed write stays pending even when this call did not extend anything.
	if cur.needsPersist() && cur.pendingSince.IsZero() {
		cur.pendingSince = now
	}
	c.entries[code] = cur
	return cur.until, extended
}

// markPersisted records a successful durable write up to `until`, clearing the pending state
// when the durable row has caught up with the active deadline.
func (c *cooldowns) markPersisted(code string, until time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.entries[code]
	if until.After(cur.persistedUntil) {
		cur.persistedUntil = until
	}
	if !cur.needsPersist() {
		cur.pendingSince, cur.lastErr = time.Time{}, ""
	}
	c.entries[code] = cur
}

// markPersistFailed records a failed durable write; the entry stays pending and is retried.
func (c *cooldowns) markPersistFailed(code string, err error, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := c.entries[code]
	if cur.pendingSince.IsZero() {
		cur.pendingSince = now
	}
	cur.lastErr = err.Error()
	c.entries[code] = cur
}

// durabilityPending reports whether ONE exchange has an un-persisted, still-active cooldown,
// and since when. An expired deadline is not pending (a restart would not park anything), so
// it never fails durability. Used for the per-exchange durability policy (PR20 correction #4).
func (c *cooldowns) durabilityPending(code string, now time.Time) (since time.Time, pending bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[code]
	if !e.needsPersist() || !now.Before(e.until) {
		return time.Time{}, false
	}
	return e.pendingSince, true
}

// pending returns the exchanges whose durable row is behind their active deadline, with the
// deadline to write and how long each has been un-persisted.
func (c *cooldowns) pending(now time.Time) []pendingPersist {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []pendingPersist
	for code, e := range c.entries {
		if !e.needsPersist() {
			continue
		}
		// An already-expired deadline no longer needs durability: it would not park anything
		// after a restart anyway. Drop it from the pending set rather than failing forever.
		if !now.Before(e.until) {
			e.persistedUntil, e.pendingSince, e.lastErr = e.until, time.Time{}, ""
			c.entries[code] = e
			continue
		}
		out = append(out, pendingPersist{code: code, until: e.until, reason: e.reason,
			source: e.source, since: e.pendingSince})
	}
	return out
}

// pendingPersist is one exchange's outstanding durable write.
type pendingPersist struct {
	code, reason, source string
	until                time.Time
	since                time.Time
}

// remaining returns how long the exchange stays parked (0 when not parked) and the entry.
func (c *cooldowns) remaining(code string, now time.Time) (time.Duration, cooldownEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[code]
	if e.until.IsZero() || !now.Before(e.until) {
		return 0, e
	}
	return e.until.Sub(now), e
}

// pacer enforces a per-exchange minimum interval between sends (proactive budget pacing).
type pacer struct {
	mu   sync.Mutex
	next map[string]time.Time
}

func newPacer() *pacer { return &pacer{next: map[string]time.Time{}} }

// reserve returns how long the caller must wait before its send slot for the exchange, and
// books the slot. perSec <= 0 disables pacing (returns 0).
func (p *pacer) reserve(code string, perSec int, now time.Time) time.Duration {
	if perSec <= 0 {
		return 0
	}
	interval := time.Second / time.Duration(perSec)
	p.mu.Lock()
	defer p.mu.Unlock()
	slot := p.next[code]
	if slot.Before(now) {
		slot = now
	}
	p.next[code] = slot.Add(interval)
	return slot.Sub(now)
}

// pace sleeps until the exchange's next proactive send slot (honoring ctx). No-op when
// rate_limit_per_sec is unconfigured for the exchange.
func (e *Executor) pace(ctx context.Context, code string) {
	perSec, _ := e.exchangeTuning(code)
	wait := e.pacers.reserve(code, perSec, e.nowFn())
	if wait <= 0 {
		return
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// paceSend blocks until the exchange's next PROACTIVE send slot and reports whether the
// caller may proceed. It is called at each real network boundary — after decode/validation/
// DB loads/live guard/audit — so a request rejected locally never consumes a slot of a
// scarce per-exchange budget, and exactly ONCE per request. No-op when rate_limit_per_sec is
// unconfigured for the exchange.
//
// It returns ctx.Err() when the wait is cut short by shutdown/cancellation. The caller MUST
// abort on a non-nil error: it must not MarkInFlight and must not call the exchange client.
// A request that never left the process is DEFINITELY unsent — turning it into an ambiguous
// mutation (IN_FLIGHT → "maybe sent" → NEEDS_RECONCILE) would be a false ambiguity. Aborting
// here leaves the row CLAIMED, which the stale-claim sweeper returns to QUEUED unsent.
func (e *Executor) paceSend(ctx context.Context, code string) error {
	e.pace(ctx, code)
	return ctx.Err()
}

// exchangeTuning resolves the operational per-exchange tuning (proactive rate_limit_per_sec
// and the retry_backoff_ms fallback cooldown) via the wired config source; zeros when absent.
func (e *Executor) exchangeTuning(code string) (perSec int, fallback time.Duration) {
	if e.cfg.ExchangeTuningFor == nil {
		return 0, 0
	}
	return e.cfg.ExchangeTuningFor(code)
}

// noteRateLimit inspects an error for a normalized rate-limit signal; when present it parks
// the exchange (reactive cooldown) and returns the structured info. The wait prefers the
// venue-provided duration, else the exchange's configured retry_backoff_ms, else the global
// fallback — always bounded by the maximum. Returns nil for non-rate-limit errors.
func (e *Executor) noteRateLimit(code string, err error) *exchanges.RateLimitInfo {
	rl := exchanges.RateLimitOf(err)
	if rl == nil {
		return nil
	}
	e.armRateLimit(code, *rl)
	return rl
}

// RateLimitSink is the late-bound bridge from the exchange adapters to the executor's
// reactive cooldown (PR20 correction #6). The private clients are constructed BEFORE the
// executor exists, so main creates this sink first, hands it to the client builder, and
// passes it in Config; New binds the executor into it.
//
// It is a separate object rather than a method on Executor deliberately: rule #1's reflection
// guard requires the Executor's ONLY exported method to be Run, so that no exported surface
// can ever send an order. A sink cannot send anything — it can only park an exchange.
type RateLimitSink struct {
	mu sync.Mutex
	e  *Executor
}

// NewRateLimitSink creates an unbound sink. Signals arriving before New binds an executor are
// dropped (no client can send before the executor drives it, so this cannot lose a real park).
func NewRateLimitSink() *RateLimitSink { return &RateLimitSink{} }

// NoteHeaderRateLimit implements exchanges.RateLimitSink: it parks the exchange for FUTURE
// requests from a throttle signal seen on a SUCCESSFUL response. The completed operation is
// untouched — a successful mutation is never turned into a failure or an ambiguous outcome
// just because the venue's remaining quota hit zero.
func (s *RateLimitSink) NoteHeaderRateLimit(code string, info exchanges.RateLimitInfo) {
	s.mu.Lock()
	e := s.e
	s.mu.Unlock()
	if e != nil {
		e.armRateLimit(code, info)
	}
}

func (s *RateLimitSink) bind(e *Executor) {
	s.mu.Lock()
	s.e = e
	s.mu.Unlock()
}

// armRateLimit resolves the effective wait (venue-provided → the exchange's configured
// retry_backoff_ms → the global fallback, always bounded) and parks the exchange IN MEMORY,
// immediately and without any I/O (PR20 correction #2).
//
// This runs on the exchange's HTTP response path (the transport observes throttle headers on
// SUCCESSFUL responses), so it must never block on the database: a slow cooldown write would
// delay a successful PlaceOrder response back to the adapter, which could time out the caller
// and turn a CONFIRMED-successful mutation into an ambiguous one — the exact outcome this
// system spends most of its complexity avoiding. Durability is handed to the persistence
// worker, which retries independently (see persistPendingCooldowns).
func (e *Executor) armRateLimit(code string, rl exchanges.RateLimitInfo) {
	wait := rl.RetryAfter
	source := rl.Source
	if wait <= 0 {
		if _, fb := e.exchangeTuning(code); fb > 0 {
			wait = fb
		} else {
			wait = e.fallbackCooldown()
		}
		source = "fallback"
	}
	if max := e.maxCooldown(); wait > max {
		wait = max
	}
	now := e.nowFn()
	reason := "rate_limit:" + safeCode(rl.Code)
	until, extended := e.cooldowns.arm(code, now.Add(wait), reason, source, now)
	// Nudge the persistence worker (non-blocking: a full channel already means "work to do").
	select {
	case e.persistWake <- struct{}{}:
	default:
	}
	if extended && e.log != nil {
		// Safe observability: exchange, reason, source, deadline. Never credentials,
		// tokens, signatures, or response bodies.
		e.log.Warn("exchange rate-limited — parked (no requests until cooldown expires)",
			"exchange", code, "source", source, "code", safeCode(rl.Code),
			"wait", wait.Round(time.Millisecond), "cooldown_until", until.UTC().Format(time.RFC3339))
	}
}

// runCooldownPersister is the separate, controlled worker that gives cooldowns their
// durability (PR20 corrections #2/#3). It never sits in an exchange response path. Each pass
// retries EVERY entry whose durable row is behind its active deadline — not just ones that
// were extended — with bounded backoff, and applies the failure policy.
func (e *Executor) runCooldownPersister(ctx context.Context) {
	backoff := cooldownPersistMinBackoff
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.persistWake:
		case <-timer.C:
		}
		if e.persistPendingCooldowns(ctx) {
			backoff = cooldownPersistMinBackoff // healthy: return to the fast cadence
		} else if backoff *= 2; backoff > cooldownPersistMaxBackoff {
			backoff = cooldownPersistMaxBackoff
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(backoff)
	}
}

// persistPendingCooldowns writes every outstanding cooldown and enforces the durability
// failure policy. It reports whether all writes succeeded (used for backoff).
func (e *Executor) persistPendingCooldowns(ctx context.Context) bool {
	now := e.nowFn()
	ok := true
	for _, p := range e.cooldowns.pending(now) {
		if err := e.persistCooldown(ctx, p.code, p.until, p.reason, p.source); err != nil {
			ok = false
			e.cooldowns.markPersistFailed(p.code, err, now)
			if e.log != nil {
				e.log.Error("cooldown persistence failed — park enforced in-process, durability PENDING (retrying)",
					"exchange", p.code, "pending_for", now.Sub(p.since).Round(time.Second), "err", err)
			}
			continue
		}
		e.cooldowns.markPersisted(p.code, p.until)
		if e.log != nil {
			e.log.Info("cooldown persisted (durable across restart)",
				"exchange", p.code, "cooldown_until", p.until.UTC().Format(time.RFC3339))
		}
	}
	e.applyCooldownDurabilityPolicy(now)
	return ok
}

// applyCooldownDurabilityPolicy is the "never silently continue" rule (PR20 correction #2/#3).
// While a park cannot be made durable, the in-process park still holds — but a restart would
// forget it and resume sending to a throttled venue. If that state persists beyond the grace
// period, live execution is DISABLED (fail closed) rather than continuing while pretending the
// cooldown is durable. It re-enables automatically once every write catches up.
func (e *Executor) applyCooldownDurabilityPolicy(now time.Time) {
	// Durability health is PER EXCHANGE (PR20 correction #4): an outage on exchange A must not
	// disable live entries on exchange B. The persister maintains a stored per-exchange flag so
	// the gate can read it in O(1) — and, crucially, so it remains observable to a place attempt
	// even after the park deadline passes (a parked exchange never reaches the gate; a failed
	// exchange whose park just expired still must not immediately create fresh exposure until
	// durability is confirmed). An exchange is unhealthy while it has an un-persisted, still-
	// active park older than grace; it recovers when the park is persisted OR expires.
	grace := e.cooldownPersistGrace()
	e.durMu.Lock()
	defer e.durMu.Unlock()
	for code := range e.exIDs {
		since, pending := e.cooldowns.durabilityPending(code, now)
		failed := pending && !since.IsZero() && now.Sub(since) > grace
		if failed == e.durabilityFailed[code] {
			continue
		}
		e.durabilityFailed[code] = failed
		if e.log == nil {
			continue
		}
		if failed {
			e.log.Error("LIVE ENTRIES DISABLED for exchange: its cooldown could not be persisted within "+
				"the grace period — a restart would forget the park, so new entry buys are blocked until "+
				"durability recovers (proven exit sells and cancels remain available)",
				"exchange", code, "grace", grace)
		} else {
			e.log.Warn("cooldown durability recovered — live entries re-enabled for exchange", "exchange", code)
		}
	}
}

// cooldownDurable reports whether THIS exchange's cooldown durability is healthy (PR20
// correction #4). It reads the stored per-exchange flag maintained by the persister, so an
// outage on exchange A never affects exchange B. In live mode a false value denies new ENTRY
// BUYS for this exchange only — proven exit sells and cancels stay available (risk-reducing).
func (e *Executor) cooldownDurable(code string) bool {
	e.durMu.Lock()
	defer e.durMu.Unlock()
	return !e.durabilityFailed[code]
}

// markCooldownDurabilityFailed forces an exchange's stored durability flag (used by tests to
// exercise the gate directly; production sets it via applyCooldownDurabilityPolicy).
func (e *Executor) markCooldownDurabilityFailed(code string, failed bool) {
	e.durMu.Lock()
	defer e.durMu.Unlock()
	e.durabilityFailed[code] = failed
}

// flushCooldownsOnShutdown drains pending cooldown persistence during a graceful shutdown
// (PR20 correction #5). Because persistence is asynchronous, an armed-but-unwritten cooldown
// would otherwise be lost on exit and a restart could resume sending to a throttled venue.
// It uses a SEPARATE bounded context (the run context is already cancelled) and cannot hang.
// A hard crash cannot be made perfectly durable this way — see PROJECT_ARCHITECTURE.md §16c.
func (e *Executor) flushCooldownsOnShutdown() {
	if len(e.cooldowns.pending(e.nowFn())) == 0 {
		return
	}
	fctx, cancel := context.WithTimeout(context.Background(), e.shutdownFlushTimeout())
	defer cancel()
	for {
		if e.persistPendingCooldowns(fctx) {
			if e.log != nil {
				e.log.Info("graceful shutdown: pending cooldowns flushed (durable across restart)")
			}
			return
		}
		remaining := len(e.cooldowns.pending(e.nowFn()))
		if remaining == 0 {
			return
		}
		if fctx.Err() != nil {
			if e.log != nil {
				e.log.Error("graceful shutdown: could not flush all cooldowns before the deadline — "+
					"those parks may be lost on restart (they will be re-detected on the next throttle)",
					"unflushed", remaining)
			}
			return
		}
		select {
		case <-fctx.Done():
		case <-time.After(cooldownPersistMinBackoff):
		}
	}
}

func (e *Executor) shutdownFlushTimeout() time.Duration {
	if e.cfg.ShutdownFlushTimeout > 0 {
		return e.cfg.ShutdownFlushTimeout
	}
	return defaultShutdownFlushTimeout
}

func (e *Executor) cooldownPersistGrace() time.Duration {
	if e.cfg.CooldownPersistGrace > 0 {
		return e.cfg.CooldownPersistGrace
	}
	return defaultCooldownPersistGrace
}

// persistCooldown write-throughs the park deadline to `exchange_cooldowns`. EXTEND-ONLY is
// enforced in SQL with GREATEST(...), so even a concurrent/re-ordered write can never shorten
// an active longer deadline. reason/source are assigned BEFORE cooldown_until because MariaDB
// evaluates ON DUPLICATE KEY assignments left to right — they must compare against the OLD
// deadline so they only change when this call actually extends it.
func (e *Executor) persistCooldown(ctx context.Context, code string, until time.Time, reason, source string) error {
	exID, ok := e.exIDs[code]
	if !ok {
		return errors.New("unknown exchange code " + code)
	}
	ctx, cancel := context.WithTimeout(ctx, cooldownPersistTimeout)
	defer cancel()
	_, err := e.store.DB().ExecContext(ctx, `
		INSERT INTO exchange_cooldowns (exchange_id, cooldown_until, reason, source)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			reason         = IF(VALUES(cooldown_until) > cooldown_until, VALUES(reason), reason),
			source         = IF(VALUES(cooldown_until) > cooldown_until, VALUES(source), source),
			cooldown_until = GREATEST(cooldown_until, VALUES(cooldown_until))`,
		exID, until.UTC(), reason, source)
	return err
}

// loadCooldowns restores still-active park deadlines at startup (PR20 correction #5), so a
// restarted executor keeps honoring a throttled venue's cooldown instead of immediately
// sending to it. Expired rows are ignored (and swept lazily by the next arm).
func (e *Executor) loadCooldowns(ctx context.Context) error {
	rows, err := e.store.DB().QueryContext(ctx, `
		SELECT ex.code, c.cooldown_until, c.reason, c.source
		FROM exchange_cooldowns c
		JOIN exchanges ex ON ex.id = c.exchange_id
		WHERE c.cooldown_until > ?`, e.nowFn().UTC())
	if err != nil {
		return err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var code, reason, source string
		var until time.Time
		if err := rows.Scan(&code, &until, &reason, &source); err != nil {
			return err
		}
		e.cooldowns.arm(code, until, reason, source, e.nowFn())
		e.cooldowns.markPersisted(code, until) // this deadline came FROM the durable row
		n++
		if e.log != nil {
			e.log.Warn("restored active exchange cooldown from the database (still parked)",
				"exchange", code, "source", source, "cooldown_until", until.UTC().Format(time.RFC3339))
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if n > 0 && e.log != nil {
		e.log.Info("exchange cooldowns restored after startup", "count", n)
	}
	return nil
}

func (e *Executor) fallbackCooldown() time.Duration {
	if e.cfg.RateLimitFallbackCooldown > 0 {
		return e.cfg.RateLimitFallbackCooldown
	}
	return defaultRateLimitFallbackCooldown
}

func (e *Executor) maxCooldown() time.Duration {
	if e.cfg.RateLimitMaxCooldown > 0 {
		return e.cfg.RateLimitMaxCooldown
	}
	return defaultRateLimitMaxCooldown
}

// parked reports whether the exchange is inside an active cooldown; logs (debounced by the
// claim cadence) are intentionally omitted here to avoid per-tick spam.
func (e *Executor) parked(code string) bool {
	rem, _ := e.cooldowns.remaining(code, e.nowFn())
	return rem > 0
}

// requeueProvenRejected re-schedules a mutating request that the venue PROVED (documented
// contract, RateLimitInfo.DefiniteRejection) was rejected before execution: the mutation
// never happened, so retrying it after the exchange's cooldown is safe and NOT blind. The
// retry is persisted (survives restart), bounded by the request's max_retries (exhaustion
// dead-letters conservatively to DEAD + NEEDS_RECONCILE inside the queue).
// requeueUnsent re-queues a mutating request KNOWN not to have executed — a venue-proven
// pre-execution rejection, or a temporary pre-network ErrNotSent. It is the ONLY sanctioned
// mutating retry, and it is atomic + status-guarded in the queue. `claimed` selects the status
// guard: false → IN_FLIGHT (post-MarkInFlight), true → CLAIMED (a pre-handler failure).
//
// retry_at is chosen by cause (PR20 correction #5): a VENUE RATE LIMIT (including a Bitpin
// auth-endpoint 429, correction #3) arms the cooldown and retries at the cooldown deadline; an
// ordinary temporary local failure uses a bounded exponential backoff (from retry_backoff_ms,
// jittered). The real failure reason is preserved in last_error + logs — never relabelled as a
// rate limit. onExhaust applies the operation-specific terminal disposition atomically at the
// retry limit (correction #4).
func (e *Executor) requeueUnsent(ctx context.Context, c queue.Claimed, kind orders.MutationKind, cause error, claimed bool) {
	at, source, logCause := e.notSentRetryAt(c, cause)
	onExhaust := e.exhaustClosure(c, kind, cause.Error())
	var status string
	var err error
	if claimed {
		status, err = e.q.RequeueClaimedUnsent(ctx, c.ID, at, logCause, onExhaust)
	} else {
		status, err = e.q.RequeueProvenUnexecuted(ctx, c.ID, at, logCause, onExhaust)
	}
	if e.log == nil {
		return
	}
	if err != nil {
		e.log.Warn("requeue of definitely-unsent mutation failed", "id", c.ID, "err", err)
		return
	}
	e.log.Info("mutation definitely not sent — re-queued", "id", c.ID, "exchange", c.ExchangeCode,
		"status", status, "retry_at", at.UTC().Format(time.RFC3339), "backoff_source", source, "cause", cause.Error())
}

// notSentRetryAt computes the retry deadline and its source for a definitely-not-sent cause.
// A rate limit (venue-proven, or a Bitpin auth 429) → arm the cooldown and use its deadline; an
// ordinary temporary local failure → bounded exponential backoff (PR20 correction #3/#5).
func (e *Executor) notSentRetryAt(c queue.Claimed, cause error) (at time.Time, source, logCause string) {
	if rl := exchanges.RateLimitOf(cause); rl != nil {
		e.armRateLimit(c.ExchangeCode, *rl) // ensure the cooldown is armed (esp. auth-endpoint 429s)
		rem, _ := e.cooldowns.remaining(c.ExchangeCode, e.nowFn())
		return e.nowFn().Add(rem), "cooldown", "not sent (venue rate-limited before execution): " + cause.Error()
	}
	return e.nowFn().Add(e.notSentBackoff(c)), "backoff", "not sent (temporary pre-execution failure): " + cause.Error()
}

// notSentBackoff is a bounded exponential backoff (with jitter) for a temporary local
// pre-execution failure — distinct from a venue cooldown, so a credential blip does not burn all
// retries in a second (PR20 correction #5). Base is the exchange's retry_backoff_ms (else 1s),
// doubled per prior attempt, capped, then jittered.
func (e *Executor) notSentBackoff(c queue.Claimed) time.Duration {
	base := time.Second
	if _, fb := e.exchangeTuning(c.ExchangeCode); fb > 0 {
		base = fb
	}
	max := e.maxCooldown()
	d := base
	for i := 0; i < c.RetryCount && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(randInt63n(int64(half)+1))
}

// exhaustClosure builds the atomic, operation-specific terminal disposition run inside the
// requeue transaction when the retry limit is reached (PR20 correction #4).
func (e *Executor) exhaustClosure(c queue.Claimed, kind orders.MutationKind, cause string) queue.ExhaustFunc {
	return func(ctx context.Context, tx *sql.Tx) error {
		return orders.DisposeDeniedMutation(ctx, tx, e.q, orders.DenialParams{
			RequestID: c.ID, OrderID: derefID(c.OrderID), ClaimedCycleID: derefID(c.CycleID),
			ClaimedExchangeID: c.ExchangeID, Kind: kind, RequestDead: true,
			Cause: "definitely-unsent retries exhausted: " + cause,
		})
	}
}

// safeCode truncates a venue error code for logs (codes are short identifiers; anything
// longer is suspicious and clipped so a body can never leak through this field).
func safeCode(code string) string {
	const max = 48
	if len(code) > max {
		return code[:max]
	}
	return code
}
