package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/migrate"
)

func intgQueue(t *testing.T) (*sql.DB, *Queue, context.Context) {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the queue integration test")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := migrate.Run(ctx, db, migrate.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db, New(db, clock.NewSystem()), ctx
}

var seedCounter int

func uniqueCode(prefix string) string {
	seedCounter++
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano()%1_000_000, seedCounter)
}

func seedExchange(t *testing.T, db *sql.DB, enabled int) int64 {
	t.Helper()
	res, err := db.Exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'q', ?)", uniqueCode("qex"), enabled)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id
}

type reqOpt struct {
	priority    int16
	status      string
	nextRetryAt *time.Time
	inflightAt  *time.Time
	claimedAt   *time.Time
	claimedBy   string
	reqType     RequestType
	orderID     *int64
	cycleID     *int64
	timeoutMs   int
}

// seedOrderFor inserts a minimal entry_buy order on a cycle and returns its id (so mutating
// requests can carry a valid order_id, which Claim now requires).
func seedOrderFor(t *testing.T, db *sql.DB, exID, cycleID int64) int64 {
	t.Helper()
	var em int64
	if err := db.QueryRow("SELECT exchange_market_id FROM cycles WHERE id=?", cycleID).Scan(&em); err != nil {
		t.Fatal(err)
	}
	res, err := db.Exec(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, order_type, limit_price, quantity)
		VALUES (?, ?, ?, 'buy', 'entry_buy', ?, 'QUEUED', 'limit', '100', '1')`, cycleID, exID, em, uniqueCode("qo"))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id
}

func seedRequest(t *testing.T, db *sql.DB, exID int64, o reqOpt) int64 {
	t.Helper()
	if o.reqType == "" {
		o.reqType = TypeGetBalance
	}
	if o.status == "" {
		o.status = "QUEUED"
	}
	if o.timeoutMs == 0 {
		o.timeoutMs = 10000
	}
	res, err := db.Exec(`INSERT INTO exchange_requests
		(exchange_id, order_id, cycle_id, request_type, priority, status, payload, timeout_ms, max_retries, next_retry_at, inflight_at, claimed_at, claimed_by, idempotency_key)
		VALUES (?, ?, ?, ?, ?, ?, '{}', ?, 5, ?, ?, ?, ?, ?)`,
		exID, o.orderID, o.cycleID, string(o.reqType), o.priority, o.status, o.timeoutMs, o.nextRetryAt, o.inflightAt, o.claimedAt, nullStr(o.claimedBy), uniqueCode("idem"))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id
}

// seedCycleDryRun creates the minimal FK chain for a cycle with the given dry_run flag and
// returns the cycle id.
func seedCycleDryRun(t *testing.T, db *sql.DB, exID int64, dry int) int64 {
	t.Helper()
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	ex := func(q string, a ...any) sql.Result {
		r, err := db.Exec(q, a...)
		if err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
		return r
	}
	b := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", uniqueCode("QB")))
	qa := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", uniqueCode("QQ")))
	m := last(ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", uniqueCode("QM")+"/IRT", b, qa))
	em := last(ex("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, ?)", exID, m, uniqueCode("QES"), uniqueCode("QM")+"/IRT"))
	return last(ex("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state, dry_run) VALUES (?, ?, ?, 'BUY_REQUEST_QUEUED', ?)", em, exID, uniqueCode("QM")+"/IRT", dry))
}

// TestClaimSeparatesDryRunFromLive: a dry-run claimer (&true) claims only requests whose
// cycle has dry_run=1; a live claimer (&false) only dry_run=0. A mismatched request is left
// QUEUED and untouched.
func TestClaimSeparatesDryRunFromLive(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)
	dryCyc := seedCycleDryRun(t, db, exID, 1)
	liveCyc := seedCycleDryRun(t, db, exID, 0)
	dryOrd := seedOrderFor(t, db, exID, dryCyc)
	liveOrd := seedOrderFor(t, db, exID, liveCyc)
	dryReq := seedRequest(t, db, exID, reqOpt{reqType: TypePlaceOrder, cycleID: &dryCyc, orderID: &dryOrd})
	liveReq := seedRequest(t, db, exID, reqOpt{reqType: TypePlaceOrder, cycleID: &liveCyc, orderID: &liveOrd})

	yes, no := true, false
	// Dry-run claimer: only the dry request.
	claimed, err := q.Claim(ctx, exID, "dry", 10, AllTypes, &yes)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != dryReq {
		t.Fatalf("dry-run claim = %v, want only the dry request %d", claimed, dryReq)
	}
	// The live request was NOT claimed — still QUEUED, untouched.
	if st, _, _ := reqRow(t, db, liveReq); st != "QUEUED" {
		t.Errorf("live request under a dry-run claimer = %s, want QUEUED (untouched)", st)
	}

	// Live claimer: only the live request (the dry one is now CLAIMED by the dry claimer, but
	// even if it were free, a live claimer would skip it).
	claimed2, err := q.Claim(ctx, exID, "live", 10, AllTypes, &no)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed2) != 1 || claimed2[0].ID != liveReq {
		t.Fatalf("live claim = %v, want only the live request %d", claimed2, liveReq)
	}
}

// reqRow reads a request's status + claim fields (for assertions).
func reqRow(t *testing.T, db *sql.DB, id int64) (status string, claimedBy sql.NullString, claimedAt sql.NullTime) {
	t.Helper()
	if err := db.QueryRow("SELECT status, claimed_by, claimed_at FROM exchange_requests WHERE id=?", id).
		Scan(&status, &claimedBy, &claimedAt); err != nil {
		t.Fatalf("read request %d: %v", id, err)
	}
	return status, claimedBy, claimedAt
}

func TestClaimPriorityAndLimitAndSkips(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)

	future := time.Now().Add(time.Hour)
	past := time.Now().Add(-time.Hour)

	low := seedRequest(t, db, exID, reqOpt{priority: 200})
	high := seedRequest(t, db, exID, reqOpt{priority: 10})
	_ = seedRequest(t, db, exID, reqOpt{status: "SUCCEEDED"})                             // skipped
	_ = seedRequest(t, db, exID, reqOpt{status: "RETRY_SCHEDULED", nextRetryAt: &future}) // not due
	dueRetry := seedRequest(t, db, exID, reqOpt{priority: 50, status: "RETRY_SCHEDULED", nextRetryAt: &past})

	// limit 2 -> claims the two most urgent eligible (high=10, dueRetry=50).
	claimed, err := q.Claim(ctx, exID, "w1", 2, AllTypes, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d, want 2", len(claimed))
	}
	if claimed[0].ID != high || claimed[1].ID != dueRetry {
		t.Errorf("claim order wrong: got %d,%d want %d,%d", claimed[0].ID, claimed[1].ID, high, dueRetry)
	}
	_ = low
}

func TestClaimRespectsPerExchangeConcurrency(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)

	// Two slots already occupied (IN_FLIGHT + CLAIMED).
	seedRequest(t, db, exID, reqOpt{status: "IN_FLIGHT", inflightAt: ptr(time.Now())})
	seedRequest(t, db, exID, reqOpt{status: "CLAIMED"})
	seedRequest(t, db, exID, reqOpt{})
	seedRequest(t, db, exID, reqOpt{})

	// limit 3, 2 in use -> only 1 new claim.
	claimed, err := q.Claim(ctx, exID, "w1", 3, AllTypes, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d, want 1 (limit 3 minus 2 in-flight)", len(claimed))
	}
}

func TestClaimConcurrentClaimersHoldLimit(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)
	for i := 0; i < 10; i++ {
		seedRequest(t, db, exID, reqOpt{})
	}

	// Two claimers race with the SAME per-exchange limit of 3. Across both, no
	// more than 3 may be claimed (the GET_LOCK + count makes the limit exact).
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			c, err := q.Claim(ctx, exID, fmt.Sprintf("w%d", n), 3, AllTypes, nil)
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			mu.Lock()
			total += len(c)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if total > 3 {
		t.Fatalf("concurrent claimers claimed %d, exceeds limit 3", total)
	}
}

func TestClaimSkipsDisabledExchange(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 0) // disabled
	seedRequest(t, db, exID, reqOpt{})
	claimed, err := q.Claim(ctx, exID, "w1", 5, AllTypes, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Fatalf("claimed %d from a disabled exchange, want 0", len(claimed))
	}
}

func TestClaimTypeFilter(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)
	tcyc := seedCycleDryRun(t, db, exID, 0)
	tord := seedOrderFor(t, db, exID, tcyc)
	seedRequest(t, db, exID, reqOpt{reqType: TypePlaceOrder, cycleID: &tcyc, orderID: &tord})
	seedRequest(t, db, exID, reqOpt{reqType: TypeGetBalance})

	// Only read-only types allowed -> the PLACE_ORDER is not claimed.
	claimed, err := q.Claim(ctx, exID, "w1", 5, ReadOnlyTypes, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].Type != TypeGetBalance {
		t.Fatalf("type filter failed: %+v", claimed)
	}
}

// TestSweepStuckReadOnlyRequeuesMutatingSkipped (PR19 round 4 #1): the generic sweeper re-queues
// stale read-only IN_FLIGHT but LEAVES stale mutating IN_FLIGHT UNTOUCHED — those are recovered
// exclusively by the order-executor's atomic recoverStaleMutating (read-only probe), so the two
// paths cannot race a probe against a premature reconcile.
func TestSweepStuckReadOnlyRequeuesMutatingSkipped(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)

	old := time.Now().Add(-time.Hour)
	// stuck read-only -> should be re-queued (RETRY_SCHEDULED)
	roID := seedRequest(t, db, exID, reqOpt{reqType: TypeGetOrder, status: "IN_FLIGHT", inflightAt: &old})
	// stuck mutating with an order -> LEFT IN_FLIGHT for the executor's recovery (not dead-lettered)
	orderID := seedOrderInState(t, db, exID, "SUBMITTED")
	mutID := seedRequest(t, db, exID, reqOpt{reqType: TypePlaceOrder, status: "IN_FLIGHT", inflightAt: &old, orderID: &orderID})

	res, err := q.SweepStuck(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeuedReadOnly < 1 || res.SkippedMutating < 1 {
		t.Fatalf("sweep result = %+v", res)
	}
	if s := statusOf(t, db, roID); s != "RETRY_SCHEDULED" {
		t.Errorf("read-only stuck status = %s, want RETRY_SCHEDULED", s)
	}
	if s := statusOf(t, db, mutID); s != "IN_FLIGHT" {
		t.Errorf("mutating stuck status = %s, want IN_FLIGHT (left for the executor's recovery, not swept)", s)
	}
	if os := orderStateOf(t, db, orderID); os != "SUBMITTED" {
		t.Errorf("order state = %s, want unchanged SUBMITTED (sweeper never touched it)", os)
	}
}

// --- helpers ---

func ptr(t time.Time) *time.Time { return &t }

func statusOf(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()
	var s string
	if err := db.QueryRow("SELECT status FROM exchange_requests WHERE id=?", id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func orderStateOf(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()
	var s string
	if err := db.QueryRow("SELECT state FROM orders WHERE id=?", id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

// seedOrderInState builds the minimal cycle+order chain and sets the order state.
func seedOrderInState(t *testing.T, db *sql.DB, exID int64, st string) int64 {
	t.Helper()
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	ex := func(q string, a ...any) sql.Result {
		r, err := db.Exec(q, a...)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	baseID := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", uniqueCode("AB")))
	quoteID := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", uniqueCode("AQ")))
	mktID := last(ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", uniqueCode("M")+"/Q", baseID, quoteID))
	emID := last(ex("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, 'X/Y')", exID, mktID, uniqueCode("ES")))
	cycID := last(ex("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol) VALUES (?, ?, 'X/Y')", emID, exID))
	ordID := last(ex(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, quantity)
		VALUES (?, ?, ?, 'buy', 'entry_buy', ?, ?, '1')`, cycID, exID, emID, uniqueCode("loc"), st))
	return ordID
}

// TestScheduleRetryRefusesMutating verifies the PR26 defense-in-depth guard: even if a
// future caller passes a MUTATING request to ScheduleRetry, it is dead-lettered (never
// rescheduled/blindly re-sent) and its owning order is pushed to NEEDS_RECONCILE.
func TestScheduleRetryRefusesMutating(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)
	ord := seedOrderInState(t, db, exID, "SUBMITTED")
	reqID := seedRequest(t, db, exID, reqOpt{reqType: TypePlaceOrder, status: "IN_FLIGHT", orderID: &ord, inflightAt: ptr(time.Now())})

	st, err := q.ScheduleRetry(ctx, reqID, "buggy caller tried to retry a place")
	if err != nil {
		t.Fatal(err)
	}
	if st != string(StatusDead) {
		t.Errorf("ScheduleRetry(mutating) status = %s, want DEAD", st)
	}
	if got := statusOf(t, db, reqID); got != "DEAD" {
		t.Errorf("mutating request = %s, want DEAD (never rescheduled)", got)
	}
	if got := orderStateOf(t, db, ord); got != "NEEDS_RECONCILE" {
		t.Errorf("owning order = %s, want NEEDS_RECONCILE", got)
	}
}

// TestScheduledStepVsRetryConvention documents+verifies the RETRY_SCHEDULED
// overloading: a scheduled next step (EnqueueScheduled) has retry_count 0; an actual
// retry (ScheduleRetry) has retry_count > 0. Reporting uses retry_count to tell a
// planned simulated-IOC/reprice step from a failed-and-retried request.
func TestScheduledStepVsRetryConvention(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)

	// Scheduled next step: RETRY_SCHEDULED, retry_count == 0.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	cyc := seedCycleDryRun(t, db, exID, 0)
	ord := seedOrderFor(t, db, exID, cyc)
	schedID, err := q.EnqueueScheduled(ctx, tx, Request{ExchangeID: exID, Type: TypeCancelOrder, CycleID: &cyc, OrderID: &ord, IdempotencyKey: uniqueCode("sched")}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if s, rc := statusRetry(t, db, schedID); s != "RETRY_SCHEDULED" || rc != 0 {
		t.Errorf("scheduled next step = %s/retry_count=%d, want RETRY_SCHEDULED/0", s, rc)
	}

	// Actual retry: RETRY_SCHEDULED, retry_count > 0.
	reqID := seedRequest(t, db, exID, reqOpt{status: "IN_FLIGHT"})
	if _, err := q.ScheduleRetry(ctx, reqID, "transient"); err != nil {
		t.Fatal(err)
	}
	if s, rc := statusRetry(t, db, reqID); s != "RETRY_SCHEDULED" || rc < 1 {
		t.Errorf("actual retry = %s/retry_count=%d, want RETRY_SCHEDULED/>=1", s, rc)
	}
}

// TestSweepStuckRecoversStaleClaimed (PR7 correction) — a request stuck in CLAIMED (the
// executor crashed between Claim and MarkInFlight) must be requeued: status→QUEUED,
// claimed_by/claimed_at cleared, and claimable again by another executor. A CLAIMED request
// was never sent, so this is safe even for mutating types.
func TestSweepStuckRecoversStaleClaimed(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)
	old := time.Now().Add(-time.Hour)

	// A stale mutating CLAIMED (claimed long ago, never went IN_FLIGHT). It carries a valid
	// order+cycle so, after requeue, it is re-claimable (Claim refuses malformed mutations).
	scyc := seedCycleDryRun(t, db, exID, 0)
	sord := seedOrderFor(t, db, exID, scyc)
	staleID := seedRequest(t, db, exID, reqOpt{reqType: TypePlaceOrder, status: "CLAIMED", claimedAt: &old, claimedBy: "deadworker", cycleID: &scyc, orderID: &sord})
	// A FRESH CLAIMED (just claimed) must NOT be swept.
	freshID := seedRequest(t, db, exID, reqOpt{status: "CLAIMED", claimedAt: ptr(time.Now()), claimedBy: "liveworker"})

	res, err := q.SweepStuck(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeuedClaimed < 1 {
		t.Errorf("RequeuedClaimed = %d, want >= 1 (this run's stale CLAIMED; shared DB may hold others)", res.RequeuedClaimed)
	}

	st, by, at := reqRow(t, db, staleID)
	if st != "QUEUED" {
		t.Errorf("stale CLAIMED status = %s, want QUEUED", st)
	}
	if by.Valid || at.Valid {
		t.Errorf("claim not cleared: claimed_by=%v claimed_at=%v", by, at)
	}
	if fst, _, _ := reqRow(t, db, freshID); fst != "CLAIMED" {
		t.Errorf("fresh CLAIMED was swept (status=%s), want left CLAIMED", fst)
	}

	// It can be claimed again by another executor.
	claimed, err := q.Claim(ctx, exID, "newworker", 5, AllTypes, nil)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range claimed {
		if c.ID == staleID {
			found = true
		}
	}
	if !found {
		t.Errorf("requeued request %d was not re-claimable; claimed=%v", staleID, claimed)
	}
}

// TestMarkInFlightRejectsNonClaimed (PR7 correction) — MarkInFlight must check RowsAffected:
// a request not in CLAIMED yields ErrRequestNotClaimed (so the executor will not send).
func TestMarkInFlightRejectsNonClaimed(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)
	// QUEUED (never claimed).
	id := seedRequest(t, db, exID, reqOpt{reqType: TypePlaceOrder, status: "QUEUED"})

	if err := q.MarkInFlight(ctx, id); !errors.Is(err, ErrRequestNotClaimed) {
		t.Fatalf("MarkInFlight(non-claimed) err = %v, want ErrRequestNotClaimed", err)
	}
	if got := statusOf(t, db, id); got != "QUEUED" {
		t.Errorf("status = %s, want unchanged QUEUED", got)
	}

	// Happy path: a CLAIMED request transitions to IN_FLIGHT.
	cl := seedRequest(t, db, exID, reqOpt{reqType: TypePlaceOrder, status: "CLAIMED", claimedAt: ptr(time.Now()), claimedBy: "w"})
	if err := q.MarkInFlight(ctx, cl); err != nil {
		t.Fatalf("MarkInFlight(CLAIMED) err = %v, want nil", err)
	}
	if got := statusOf(t, db, cl); got != "IN_FLIGHT" {
		t.Errorf("status = %s, want IN_FLIGHT", got)
	}
}

// TestTerminalMarksAreStatusGuarded (PR7 correction) — a late worker must not overwrite a
// newer terminal status: MarkSucceeded/MarkFailed/MarkDead on a DEAD request return
// ErrRequestNotActive and leave it DEAD. The valid transitions still work.
func TestTerminalMarksAreStatusGuarded(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)

	inTx := func(fn func(tx *sql.Tx) error) error {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback() //nolint:errcheck // these cases expect an error → roll back
		return fn(tx)
	}

	// A CONFLICTING newer terminal status must NOT be overwritten by a late worker.
	for _, tc := range []struct {
		name, seedStatus string
		call             func(tx *sql.Tx, id int64) error
	}{
		{"MarkSucceeded over DEAD", "DEAD", func(tx *sql.Tx, id int64) error { return q.MarkSucceeded(ctx, tx, id, nil) }},
		{"MarkFailed over DEAD", "DEAD", func(tx *sql.Tx, id int64) error { return q.MarkFailed(ctx, tx, id, "late") }},
		{"MarkDead over SUCCEEDED", "SUCCEEDED", func(tx *sql.Tx, id int64) error { return q.MarkDead(ctx, tx, id, "late") }},
	} {
		req := seedRequest(t, db, exID, reqOpt{status: tc.seedStatus})
		if err := inTx(func(tx *sql.Tx) error { return tc.call(tx, req) }); !errors.Is(err, ErrRequestNotActive) {
			t.Errorf("%s: err = %v, want ErrRequestNotActive", tc.name, err)
		}
		if got := statusOf(t, db, req); got != tc.seedStatus {
			t.Errorf("%s: status = %s, want unchanged %s", tc.name, got, tc.seedStatus)
		}
	}

	// Re-applying the SAME terminal status is an idempotent no-op (crash-recovery reprocessing).
	already := seedRequest(t, db, exID, reqOpt{status: "SUCCEEDED"})
	if err := inTx(func(tx *sql.Tx) error { return q.MarkSucceeded(ctx, tx, already, nil) }); err != nil {
		t.Errorf("MarkSucceeded on already-SUCCEEDED should be a no-op, got %v", err)
	}

	// Valid transitions still succeed (committed).
	cl := seedRequest(t, db, exID, reqOpt{status: "CLAIMED", claimedAt: ptr(time.Now()), claimedBy: "w"})
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := q.MarkSucceeded(ctx, tx, cl, nil); err != nil {
		t.Fatalf("MarkSucceeded(CLAIMED) err = %v, want nil", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := statusOf(t, db, cl); got != "SUCCEEDED" {
		t.Errorf("CLAIMED->SUCCEEDED status = %s, want SUCCEEDED", got)
	}
}

func statusRetry(t *testing.T, db *sql.DB, id int64) (string, int) {
	t.Helper()
	var s string
	var rc int
	if err := db.QueryRow("SELECT status, retry_count FROM exchange_requests WHERE id=?", id).Scan(&s, &rc); err != nil {
		t.Fatal(err)
	}
	return s, rc
}

// --- PR20 round-5 #4: RequeueProvenUnexecuted is atomic + status-guarded --------------------

// reqStatusOf reads a request's current status + retry_count.
func reqStatusOf(t *testing.T, db *sql.DB, id int64) (string, int) {
	t.Helper()
	var status string
	var rc int
	if err := db.QueryRow("SELECT status, retry_count FROM exchange_requests WHERE id=?", id).Scan(&status, &rc); err != nil {
		t.Fatal(err)
	}
	return status, rc
}

// TestRequeueProvenUnexecutedRequiresInFlight: a terminal request (DEAD/FAILED/SUCCEEDED) can
// never be resurrected to RETRY_SCHEDULED — the status guard leaves it exactly as it was.
func TestRequeueProvenUnexecutedRequiresInFlight(t *testing.T) {
	for _, terminal := range []string{"DEAD", "FAILED", "SUCCEEDED"} {
		t.Run(terminal, func(t *testing.T) {
			db, q, ctx := intgQueue(t)
			exID := seedExchange(t, db, 1)
			id := seedRequest(t, db, exID, reqOpt{reqType: TypePlaceOrder, status: terminal})
			got, err := q.RequeueProvenUnexecuted(ctx, id, time.Now().Add(time.Minute), "should be ignored", nil)
			if err != nil {
				t.Fatalf("RequeueProvenUnexecuted: %v", err)
			}
			if got != terminal {
				t.Errorf("returned status = %s, want %s (unchanged)", got, terminal)
			}
			if s, rc := reqStatusOf(t, db, id); s != terminal || rc != 0 {
				t.Errorf("row = (%s, retry_count=%d), want (%s, 0) — a terminal request must never be resurrected", s, rc, terminal)
			}
		})
	}
}

// TestRequeueProvenUnexecutedInFlightRetries: an IN_FLIGHT request is moved to RETRY_SCHEDULED
// with the retry count incremented exactly once.
func TestRequeueProvenUnexecutedInFlightRetries(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)
	id := seedRequest(t, db, exID, reqOpt{reqType: TypePlaceOrder, status: "IN_FLIGHT"})
	got, err := q.RequeueProvenUnexecuted(ctx, id, time.Now().Add(time.Minute), "proven not executed", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != "RETRY_SCHEDULED" {
		t.Fatalf("status = %s, want RETRY_SCHEDULED", got)
	}
	if s, rc := reqStatusOf(t, db, id); s != "RETRY_SCHEDULED" || rc != 1 {
		t.Errorf("row = (%s, retry_count=%d), want (RETRY_SCHEDULED, 1)", s, rc)
	}
}

// TestRequeueProvenUnexecutedExhaustionDeadLetters: at the retry limit it atomically moves the
// request to DEAD and pushes the owning order to NEEDS_RECONCILE.
func TestRequeueProvenUnexecutedExhaustionDeadLetters(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)
	cyc := seedCycleDryRun(t, db, exID, 0)
	// Register an order in a state that can enter NEEDS_RECONCILE.
	var ordID int64
	res, err := db.Exec(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, order_type, limit_price, quantity)
		SELECT ?, ?, em.id, 'buy', 'entry_buy', ?, 'ACKED', 'limit', '100', '1'
		FROM exchange_markets em WHERE em.exchange_id=? LIMIT 1`, cyc, exID, uniqueCode("ol"), exID)
	if err != nil {
		t.Fatal(err)
	}
	ordID, _ = res.LastInsertId()
	// max_retries defaults to 5 in seedRequest; set retry_count to the limit so next > max.
	id := seedRequest(t, db, exID, reqOpt{reqType: TypePlaceOrder, status: "IN_FLIGHT", cycleID: &cyc, orderID: &ordID})
	db.Exec("UPDATE exchange_requests SET retry_count=5 WHERE id=?", id)

	got, err := q.RequeueProvenUnexecuted(ctx, id, time.Now().Add(time.Minute), "exhausted", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != "DEAD" {
		t.Fatalf("status = %s, want DEAD (exhausted)", got)
	}
	if s, _ := reqStatusOf(t, db, id); s != "DEAD" {
		t.Errorf("request = %s, want DEAD", s)
	}
	var ordState string
	db.QueryRow("SELECT state FROM orders WHERE id=?", ordID).Scan(&ordState)
	if ordState != "NEEDS_RECONCILE" {
		t.Errorf("order state = %s, want NEEDS_RECONCILE (atomic dead-letter)", ordState)
	}
}

// TestRequeueProvenUnexecutedConcurrent: two concurrent handlers for the same IN_FLIGHT request
// produce exactly ONE retry-count increment and ONE transition to RETRY_SCHEDULED.
func TestRequeueProvenUnexecutedConcurrent(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)
	id := seedRequest(t, db, exID, reqOpt{reqType: TypePlaceOrder, status: "IN_FLIGHT"})

	var wg sync.WaitGroup
	results := make([]string, 2)
	errs := make([]error, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = q.RequeueProvenUnexecuted(ctx, id, time.Now().Add(time.Minute), "concurrent", nil)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("handler %d: %v", i, err)
		}
	}
	// The definitive proof of "exactly one transition" is that retry_count incremented ONCE:
	// the FOR UPDATE lock serializes the two handlers, the first moves IN_FLIGHT →
	// RETRY_SCHEDULED (retry_count 0→1), and the second observes RETRY_SCHEDULED (status guard)
	// and makes NO further change. (Both may RETURN "RETRY_SCHEDULED" — one transitioned it, the
	// other observed it — so the row state, not the return string, is the invariant.)
	if s, rc := reqStatusOf(t, db, id); s != "RETRY_SCHEDULED" || rc != 1 {
		t.Errorf("row = (%s, retry_count=%d), want (RETRY_SCHEDULED, 1) — exactly one increment despite two concurrent handlers", s, rc)
	}
	for _, r := range results {
		if r != "RETRY_SCHEDULED" {
			t.Errorf("each handler must end observing RETRY_SCHEDULED, got %v", results)
		}
	}
}

// --- PR20 round-7 #1: mutating requests require cycle_id AND order_id ------------------------

func TestEnqueueRejectsMutationWithoutOrder(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)
	cyc := seedCycleDryRun(t, db, exID, 0)
	ord := seedOrderFor(t, db, exID, cyc)
	cases := []struct {
		name string
		r    Request
	}{
		{"place no order", Request{ExchangeID: exID, Type: TypePlaceOrder, CycleID: &cyc, IdempotencyKey: uniqueCode("e1")}},
		{"place no cycle", Request{ExchangeID: exID, Type: TypePlaceOrder, OrderID: &ord, IdempotencyKey: uniqueCode("e2")}},
		{"cancel no order", Request{ExchangeID: exID, Type: TypeCancelOrder, CycleID: &cyc, IdempotencyKey: uniqueCode("e3")}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tx, _ := db.Begin()
			_, err := q.Enqueue(ctx, tx, c.r)
			_ = tx.Rollback()
			if !errors.Is(err, ErrMalformedMutation) {
				t.Errorf("Enqueue err = %v, want ErrMalformedMutation", err)
			}
			tx2, _ := db.Begin()
			_, err = q.EnqueueScheduled(ctx, tx2, c.r, time.Second)
			_ = tx2.Rollback()
			if !errors.Is(err, ErrMalformedMutation) {
				t.Errorf("EnqueueScheduled err = %v, want ErrMalformedMutation", err)
			}
		})
	}
}

// TestClaimRefusesMalformedMutation: a historical/manually-written mutating row without an
// order_id must NOT be claimed for sending.
func TestClaimRefusesMalformedMutation(t *testing.T) {
	db, q, ctx := intgQueue(t)
	exID := seedExchange(t, db, 1)
	cyc := seedCycleDryRun(t, db, exID, 0)
	// Direct insert bypassing Enqueue validation (simulates a historical/manual row).
	malformed := seedRequest(t, db, exID, reqOpt{reqType: TypePlaceOrder, cycleID: &cyc}) // NULL order_id
	claimed, err := q.Claim(ctx, exID, "w", 10, AllTypes, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range claimed {
		if c.ID == malformed {
			t.Fatalf("Claim returned a malformed mutating request %d (NULL order_id)", malformed)
		}
	}
	if s, _ := statusRetry(t, db, malformed); s != "QUEUED" {
		t.Errorf("malformed row status = %s, want still QUEUED (unclaimed)", s)
	}
}
