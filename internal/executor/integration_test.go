package executor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/execution"
	"v3TradeBot/internal/migrate"
	"v3TradeBot/internal/orders"
	"v3TradeBot/internal/queue"
	"v3TradeBot/internal/state"
)

// fakeClient is a configurable exchanges.PrivateClient for executor tests. It
// makes no network calls (rule #10). Each call returns the configured value/error.
type fakeClient struct {
	code      string
	placeAck  execution.OrderAck
	placeErr  error
	cancelErr error
	balErr    error
	getStatus execution.OrderStatus
	getErr    error
	// send counters (PR26 crash/rollback tests assert no blind resend; PR11 empty-id tests
	// assert no CancelOrder("")/GetOrder("")).
	placeCount  int32
	cancelCount int32
	getCount    int32
	balCount    int32
	onBalances  func()
	// PR19 round 3 test hooks.
	capsOverride *exchanges.Capabilities // capability set (nil → default)
	normalizeCID func(string) string     // ClientOrderIDForSend transform (nil → passthrough)
	onPlace      func()                  // called INSIDE PlaceOrder (assert persisted-before-send)
	lastSentCID  string                  // the ClientOrderID PlaceOrder actually received
	// round-4 #3: capture the PlaceOrder context deadline to prove the exchange timeout starts
	// AFTER pacing (a fresh, non-expired deadline at the network boundary).
	lastPlaceDeadline    time.Time
	lastPlaceHasDeadline bool
	lastPlaceCtxErr      error
}

func (f *fakeClient) Name() string { return f.code }
func (f *fakeClient) Capabilities() exchanges.Capabilities {
	if f.capsOverride != nil {
		return *f.capsOverride
	}
	return exchanges.Capabilities{PlaceOrder: true}
}

// ClientOrderIDForSend implements exchanges.ClientOrderIDNormalizer (passthrough unless a
// transform is set), so tests can prove the EXACT sent id is persisted before the network call.
func (f *fakeClient) ClientOrderIDForSend(local string) string {
	if f.normalizeCID != nil {
		return f.normalizeCID(local)
	}
	return local
}
func (f *fakeClient) GetBalances(context.Context) ([]domain.Balance, error) {
	atomic.AddInt32(&f.balCount, 1)
	if f.onBalances != nil {
		f.onBalances()
	}
	if f.balErr != nil {
		return nil, f.balErr
	}
	return []domain.Balance{{Asset: "USDT", Available: decimal.RequireFromString("100")}}, nil
}
func (f *fakeClient) PlaceOrder(ctx context.Context, req execution.OrderRequest) (execution.OrderAck, error) {
	atomic.AddInt32(&f.placeCount, 1)
	f.lastSentCID = req.ClientOrderID
	if dl, ok := ctx.Deadline(); ok {
		f.lastPlaceDeadline, f.lastPlaceHasDeadline = dl, true
	}
	f.lastPlaceCtxErr = ctx.Err()
	if f.onPlace != nil {
		f.onPlace()
	}
	return f.placeAck, f.placeErr
}
func (f *fakeClient) CancelOrder(context.Context, string) error {
	atomic.AddInt32(&f.cancelCount, 1)
	return f.cancelErr
}
func (f *fakeClient) GetOrder(context.Context, string) (execution.OrderStatus, error) {
	atomic.AddInt32(&f.getCount, 1)
	return f.getStatus, f.getErr
}

// GetOrderByClientOrderID implements exchanges.ClientOrderLookup (delegates to the fake's
// canned status/error, like GetOrder) so recovery-by-client-id tests can drive the fake.
func (f *fakeClient) GetOrderByClientOrderID(context.Context, string) (execution.OrderStatus, error) {
	atomic.AddInt32(&f.getCount, 1)
	return f.getStatus, f.getErr
}
func (f *fakeClient) GetOpenOrders(context.Context, string) ([]execution.OrderStatus, error) {
	return nil, nil
}
func (f *fakeClient) SubscribeOrderUpdates(context.Context) (<-chan execution.NormalizedOrderEvent, error) {
	return nil, exchanges.Unsupported(f.code, "SubscribeOrderUpdates")
}

// seedSeq gives unique short symbols/ids across seeded rows within a test run.
var seedSeq int

type intg struct {
	db    *sql.DB
	store *db.Store
	q     *queue.Queue
	ctx   context.Context
	code  string
	exID  int64
	fake  *fakeClient
	exec  *Executor
}

func setup(t *testing.T) *intg {
	t.Helper()
	dsn := os.Getenv("V3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set V3_TEST_MYSQL_DSN to run the executor integration test")
	}
	sqlDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	ctx := context.Background()
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if _, err := migrate.Run(ctx, sqlDB, migrate.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	code := fmt.Sprintf("exec_%d", time.Now().UnixNano()%1_000_000_000)
	res, err := sqlDB.Exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'x', 1)", code)
	if err != nil {
		t.Fatal(err)
	}
	exID, _ := res.LastInsertId()
	fake := &fakeClient{code: code}
	store := db.NewFromDB(sqlDB)
	q := queue.New(sqlDB, clock.NewSystem())
	exec := New(store, q, map[string]exchanges.PrivateClient{code: fake}, nil,
		Config{Name: "test-exec", AllowLiveExecution: true})
	return &intg{db: sqlDB, store: store, q: q, ctx: ctx, code: code, exID: exID, fake: fake, exec: exec}
}

func (it *intg) seedRequest(t *testing.T, typ queue.RequestType, payload string, orderID *int64) queue.Claimed {
	t.Helper()
	res, err := it.db.Exec(`INSERT INTO exchange_requests
		(exchange_id, order_id, request_type, priority, status, payload, timeout_ms, max_retries, idempotency_key)
		VALUES (?, ?, ?, 100, 'CLAIMED', ?, 10000, 5, ?)`,
		it.exID, orderID, string(typ), payload, fmt.Sprintf("idem_%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return queue.Claimed{ID: id, ExchangeID: it.exID, ExchangeCode: it.code, Type: typ,
		Payload: json.RawMessage(payload), OrderID: orderID, TimeoutMS: 10000, MaxRetries: 5}
}

func (it *intg) seedOrder(t *testing.T, st string) int64 {
	t.Helper()
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	ex := func(q string, a ...any) sql.Result {
		r, err := it.db.Exec(q, a...)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	seedSeq++
	// Globally-unique across packages (go test ./... shares one DB): include nanos.
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), seedSeq) }
	b := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B")))
	qa := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("Q")))
	m := last(ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", u("M"), b, qa))
	em := last(ex("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, 'X/Y')", it.exID, m, u("ES")))
	c := last(ex("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol) VALUES (?, ?, 'X/Y')", em, it.exID))
	return last(ex(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, quantity)
		VALUES (?, ?, ?, 'buy', 'entry_buy', ?, ?, '1')`, c, it.exID, em, u("loc"), st))
}

func reqStatus(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()
	var s string
	if err := db.QueryRow("SELECT status FROM exchange_requests WHERE id=?", id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}
func ordState(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()
	var s string
	if err := db.QueryRow("SELECT state FROM orders WHERE id=?", id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func cycState(t *testing.T, db *sql.DB, id int64) string {
	t.Helper()
	var s string
	if err := db.QueryRow("SELECT state FROM cycles WHERE id=?", id).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestReadOnlySuccessAndErrors(t *testing.T) {
	it := setup(t)

	// success
	c := it.seedRequest(t, queue.TypeGetBalance, "{}", nil)
	it.exec.process(it.ctx, c)
	if s := reqStatus(t, it.db, c.ID); s != "SUCCEEDED" {
		t.Errorf("read-only success status = %s", s)
	}

	// non-retryable error -> FAILED (checked BEFORE any rate-limit case: a rate limit now
	// parks the whole exchange — see below).
	it.fake.balErr = fmt.Errorf("permanent boom")
	c3 := it.seedRequest(t, queue.TypeGetBalance, "{}", nil)
	it.exec.process(it.ctx, c3)
	if s := reqStatus(t, it.db, c3.ID); s != "FAILED" {
		t.Errorf("non-retryable status = %s, want FAILED", s)
	}

	// retryable error -> RETRY_SCHEDULED. A RATE-LIMITED read additionally PARKS the
	// exchange (PR20 #4), so a follow-up claimed request is deferred back to QUEUED
	// without any network call.
	it.fake.balErr = execution.ErrRateLimited
	c2 := it.seedRequest(t, queue.TypeGetBalance, "{}", nil)
	it.exec.process(it.ctx, c2)
	if s := reqStatus(t, it.db, c2.ID); s != "RETRY_SCHEDULED" {
		t.Errorf("retryable status = %s, want RETRY_SCHEDULED", s)
	}
	if !it.exec.parked(it.code) {
		t.Error("a rate-limited read must park the exchange")
	}
	before := atomic.LoadInt32(&it.fake.balCount)
	c4 := it.seedRequest(t, queue.TypeGetBalance, "{}", nil)
	it.exec.process(it.ctx, c4)
	if s := reqStatus(t, it.db, c4.ID); s != "QUEUED" {
		t.Errorf("request during cooldown = %s, want QUEUED (deferred, not sent)", s)
	}
	if after := atomic.LoadInt32(&it.fake.balCount); after != before {
		t.Errorf("GetBalances called during cooldown (%d -> %d), want no network call", before, after)
	}
}

// seedBuyOrder seeds a cycle (cycleSt) + entry_buy order (orderSt) and returns both
// ids — used by the PR10 place tests that need cycle context on the request.
func (it *intg) seedBuyOrder(t *testing.T, cycleSt, orderSt string) (orderID, cycleID int64) {
	t.Helper()
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	ex := func(q string, a ...any) sql.Result {
		r, err := it.db.Exec(q, a...)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	seedSeq++
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), seedSeq) }
	b := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B")))
	qa := last(ex("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q")))
	m := last(ex("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", u("M")+"/IRT", b, qa))
	em := last(ex("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol) VALUES (?, ?, ?, ?)", it.exID, m, u("ES"), u("M")+"/IRT"))
	cycleID = last(ex("INSERT INTO cycles (exchange_market_id, buy_exchange_id, canonical_symbol, state) VALUES (?, ?, ?, ?)", em, it.exID, u("M")+"/IRT", cycleSt))
	orderID = last(ex(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role, local_client_order_id, state, order_type, limit_price, quantity)
		VALUES (?, ?, ?, 'buy', 'entry_buy', ?, ?, 'limit', '100', '1')`, cycleID, it.exID, em, u("loc"), orderSt))
	return orderID, cycleID
}

// seedPlace seeds a CLAIMED PLACE_ORDER request (BuyIntentPayload) tied to an
// order+cycle and returns the Claimed for direct process() dispatch.
func (it *intg) seedPlace(t *testing.T, orderID, cycleID int64, intent orders.BuyIntentPayload) queue.Claimed {
	t.Helper()
	payload, _ := json.Marshal(intent)
	seedSeq++
	res, err := it.db.Exec(`INSERT INTO exchange_requests
		(exchange_id, cycle_id, order_id, request_type, priority, status, payload, timeout_ms, max_retries, idempotency_key)
		VALUES (?, ?, ?, 'PLACE_ORDER', 50, 'CLAIMED', ?, 10000, 5, ?)`,
		it.exID, cycleID, orderID, payload, fmt.Sprintf("idem_%d_%d", time.Now().UnixNano(), seedSeq))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return queue.Claimed{ID: id, ExchangeID: it.exID, ExchangeCode: it.code, Type: queue.TypePlaceOrder,
		Payload: json.RawMessage(payload), OrderID: &orderID, CycleID: &cycleID, Symbol: "X/IRT", TimeoutMS: 10000, MaxRetries: 5}
}

func TestPlaceSuccessAcksAndSchedulesCancel(t *testing.T) {
	it := setup(t)
	orderID, cycleID := it.seedBuyOrder(t, string(state.CycleBuyRequestQueued), string(state.OrderQueued))
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EX123", ClientOrderID: "loc-x", Status: execution.StateOpen}
	c := it.seedPlace(t, orderID, cycleID, orders.BuyIntentPayload{Side: "buy", OrderType: "limit", SimulatedIOC: true, IntendedPrice: "100", IntendedQuantity: "1", LocalClientOrderID: "loc-x", MakerWaitBeforeCancelMs: 2000})
	it.exec.process(it.ctx, c)

	if s := reqStatus(t, it.db, c.ID); s != "SUCCEEDED" {
		t.Errorf("place success status = %s", s)
	}
	if os := ordState(t, it.db, orderID); os != string(state.OrderAcked) {
		t.Errorf("order state = %s, want ACKED", os)
	}
	if cs := cycState(t, it.db, cycleID); cs != string(state.CycleBuySubmitted) {
		t.Errorf("cycle state = %s, want BUY_SUBMITTED", cs)
	}
	var exOID string
	it.db.QueryRow("SELECT exchange_order_id FROM orders WHERE id=?", orderID).Scan(&exOID)
	if exOID != "EX123" {
		t.Errorf("exchange_order_id = %q, want EX123", exOID)
	}
	var scheduled int
	it.db.QueryRow("SELECT COUNT(*) FROM exchange_requests WHERE order_id=? AND request_type='CANCEL_ORDER' AND status='RETRY_SCHEDULED'", orderID).Scan(&scheduled)
	if scheduled != 1 {
		t.Errorf("scheduled cancels = %d, want 1 (simulated-IOC wait, queued not slept)", scheduled)
	}
}

func TestPlaceDefiniteRejectionFailsCleanly(t *testing.T) {
	it := setup(t)
	orderID, cycleID := it.seedBuyOrder(t, string(state.CycleBuyRequestQueued), string(state.OrderQueued))
	it.fake.placeErr = execution.ErrInsufficientBalance
	c := it.seedPlace(t, orderID, cycleID, orders.BuyIntentPayload{Side: "buy", OrderType: "limit", SimulatedIOC: true, IntendedPrice: "100", IntendedQuantity: "1", LocalClientOrderID: "loc"})
	it.exec.process(it.ctx, c)

	// Definitely not placed -> no exposure: request FAILED, order + cycle FAILED.
	if s := reqStatus(t, it.db, c.ID); s != "FAILED" {
		t.Errorf("definite-rejection status = %s, want FAILED", s)
	}
	if os := ordState(t, it.db, orderID); os != string(state.OrderFailed) {
		t.Errorf("order = %s, want FAILED", os)
	}
	if cs := cycState(t, it.db, cycleID); cs != string(state.CycleFailed) {
		t.Errorf("cycle = %s, want FAILED", cs)
	}
}

// TestPlaceAmbiguousSchedulesRecoveryProbe (PR19 round 2 #3): an ambiguous PLACE timeout is an
// UNKNOWN outcome — never a blind resend and NO LONGER an immediate NEEDS_RECONCILE. Instead the
// place request is DEAD-lettered (consumed) and a READ-ONLY GET_ORDER recovery probe is
// scheduled; the order is left QUEUED/SUBMITTED for the probe to resolve.
func TestPlaceAmbiguousSchedulesRecoveryProbe(t *testing.T) {
	it := setup(t)
	orderID, cycleID := it.seedBuyOrder(t, string(state.CycleBuySubmitted), string(state.OrderSubmitted))
	it.fake.placeErr = execution.ErrAckTimeout // ambiguous: maybe placed, maybe not
	c := it.seedPlace(t, orderID, cycleID, orders.BuyIntentPayload{Side: "buy", OrderType: "limit", SimulatedIOC: true, IntendedPrice: "100", IntendedQuantity: "1", LocalClientOrderID: "loc"})
	it.exec.process(it.ctx, c)

	if s := reqStatus(t, it.db, c.ID); s != "DEAD" {
		t.Errorf("ambiguous place request status = %s, want DEAD (consumed, never re-sent)", s)
	}
	// The order must NOT be prematurely failed/reconciled — recovery is pending.
	if os := ordState(t, it.db, orderID); os == string(state.OrderNeedsReconcile) || os == string(state.OrderFailed) {
		t.Errorf("order state = %s, want it left for recovery (not NEEDS_RECONCILE/FAILED)", os)
	}
	// Exactly one read-only recovery probe must be scheduled for this order.
	var probes int
	it.db.QueryRow(`SELECT COUNT(*) FROM exchange_requests
		WHERE order_id=? AND request_type='GET_ORDER'
		  AND JSON_EXTRACT(payload,'$.purpose') = ?`, orderID, orders.PurposeAmbiguousPlaceProbe).Scan(&probes)
	if probes != 1 {
		t.Errorf("scheduled ambiguous_place_probe = %d, want 1 (read-only recovery, not blind resend)", probes)
	}
}

// TestMarkInFlightFailureBlocksSend (PR7 correction) — if MarkInFlight does not move the
// request CLAIMED→IN_FLIGHT (e.g. a concurrent sweep requeued it), the executor must NOT
// call PlaceOrder or CancelOrder. We simulate the race by flipping the row off CLAIMED after
// building the Claimed but before processing.
func TestMarkInFlightFailureBlocksSend(t *testing.T) {
	it := setup(t)
	orderID, cycleID := it.seedBuyOrder(t, string(state.CycleBuyRequestQueued), string(state.OrderQueued))

	// PLACE: row no longer CLAIMED -> MarkInFlight fails -> no PlaceOrder.
	cPlace := it.seedPlace(t, orderID, cycleID, orders.BuyIntentPayload{Side: "buy", OrderType: "limit", SimulatedIOC: true, IntendedPrice: "100", IntendedQuantity: "1", LocalClientOrderID: "loc"})
	if _, err := it.db.Exec("UPDATE exchange_requests SET status='QUEUED', claimed_by=NULL, claimed_at=NULL WHERE id=?", cPlace.ID); err != nil {
		t.Fatal(err)
	}
	it.exec.process(it.ctx, cPlace)
	if got := atomic.LoadInt32(&it.fake.placeCount); got != 0 {
		t.Errorf("PlaceOrder called %d times after MarkInFlight failure, want 0", got)
	}

	// CANCEL: same — a request not CLAIMED must not reach CancelOrder.
	fp := orders.FollowupPayload{ExchangeOrderID: "EX1"}
	payload, _ := json.Marshal(fp)
	res, err := it.db.Exec(`INSERT INTO exchange_requests
		(exchange_id, cycle_id, order_id, request_type, priority, status, payload, timeout_ms, max_retries, idempotency_key)
		VALUES (?, ?, ?, 'CANCEL_ORDER', 50, 'QUEUED', ?, 10000, 5, ?)`,
		it.exID, cycleID, orderID, payload, fmt.Sprintf("idem_cancel_%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	cancelID, _ := res.LastInsertId()
	cCancel := queue.Claimed{ID: cancelID, ExchangeID: it.exID, ExchangeCode: it.code, Type: queue.TypeCancelOrder,
		Payload: json.RawMessage(payload), OrderID: &orderID, CycleID: &cycleID, Symbol: "X/IRT", TimeoutMS: 10000, MaxRetries: 5}
	it.exec.process(it.ctx, cCancel)
	if got := atomic.LoadInt32(&it.fake.cancelCount); got != 0 {
		t.Errorf("CancelOrder called %d times after MarkInFlight failure, want 0", got)
	}
}

func TestPlaceSuccessRollsBackWhenOrderTransitionInvalid(t *testing.T) {
	it := setup(t)
	// Order already FILLED (terminal): the place-ack transitions are invalid, so the
	// whole completion tx must roll back -> request NOT marked SUCCEEDED.
	orderID, cycleID := it.seedBuyOrder(t, string(state.CycleBuySubmitted), string(state.OrderFilled))
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EX999", Status: execution.StateOpen}
	c := it.seedPlace(t, orderID, cycleID, orders.BuyIntentPayload{Side: "buy", OrderType: "limit", SimulatedIOC: true, IntendedPrice: "100", IntendedQuantity: "1", LocalClientOrderID: "loc"})
	it.exec.process(it.ctx, c)

	if s := reqStatus(t, it.db, c.ID); s == "SUCCEEDED" {
		t.Errorf("request must NOT be SUCCEEDED when order transition fails; got %s", s)
	}
	if os := ordState(t, it.db, orderID); os != string(state.OrderFilled) {
		t.Errorf("order must be unchanged (FILLED) after rollback, got %s", os)
	}
}

// TestCancelWithoutCycleFailsClosed (PR19 round 3 #6): a mutating CANCEL_ORDER with no cycle/order
// context cannot be classified dry-run vs live nor recovered — it must FAIL CLOSED with NO send.
func TestCancelWithoutCycleFailsClosed(t *testing.T) {
	it := setup(t)
	c := it.seedRequest(t, queue.TypeCancelOrder, `{"exchange_order_id":"EX1"}`, nil)
	it.exec.process(it.ctx, c)
	if n := atomic.LoadInt32(&it.fake.cancelCount); n != 0 {
		t.Errorf("CancelOrder called %d times for a cycle-less request, want 0 (fail closed)", n)
	}
	// A malformed mutating request (no cycle/order) is now finalized DEAD (not merely FAILED) —
	// with a known cycle it would also reconcile that cycle + hold its lock (PR20 correction #1).
	if s := reqStatus(t, it.db, c.ID); s != "DEAD" {
		t.Errorf("cycle-less cancel status = %s, want DEAD (malformed, not sent)", s)
	}
}
