package executor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
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
}

func (f *fakeClient) Name() string { return f.code }
func (f *fakeClient) Capabilities() exchanges.Capabilities {
	return exchanges.Capabilities{PlaceOrder: true}
}
func (f *fakeClient) GetBalances(context.Context) ([]domain.Balance, error) {
	if f.balErr != nil {
		return nil, f.balErr
	}
	return []domain.Balance{{Asset: "USDT", Available: decimal.RequireFromString("100")}}, nil
}
func (f *fakeClient) PlaceOrder(context.Context, execution.OrderRequest) (execution.OrderAck, error) {
	return f.placeAck, f.placeErr
}
func (f *fakeClient) CancelOrder(context.Context, string) error { return f.cancelErr }
func (f *fakeClient) GetOrder(context.Context, string) (execution.OrderStatus, error) {
	return execution.OrderStatus{}, nil
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
	u := func(p string) string { return fmt.Sprintf("%s%d", p, seedSeq) }
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

func TestReadOnlySuccessAndErrors(t *testing.T) {
	it := setup(t)

	// success
	c := it.seedRequest(t, queue.TypeGetBalance, "{}", nil)
	it.exec.process(it.ctx, c)
	if s := reqStatus(t, it.db, c.ID); s != "SUCCEEDED" {
		t.Errorf("read-only success status = %s", s)
	}

	// retryable error -> RETRY_SCHEDULED
	it.fake.balErr = execution.ErrRateLimited
	c2 := it.seedRequest(t, queue.TypeGetBalance, "{}", nil)
	it.exec.process(it.ctx, c2)
	if s := reqStatus(t, it.db, c2.ID); s != "RETRY_SCHEDULED" {
		t.Errorf("retryable status = %s, want RETRY_SCHEDULED", s)
	}

	// non-retryable error -> FAILED
	it.fake.balErr = fmt.Errorf("permanent boom")
	c3 := it.seedRequest(t, queue.TypeGetBalance, "{}", nil)
	it.exec.process(it.ctx, c3)
	if s := reqStatus(t, it.db, c3.ID); s != "FAILED" {
		t.Errorf("non-retryable status = %s, want FAILED", s)
	}
}

func TestPlaceSuccessAdvancesOrderAtomically(t *testing.T) {
	it := setup(t)
	orderID := it.seedOrder(t, string(state.OrderQueued))
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EX123", Status: execution.StateOpen}

	payload, _ := json.Marshal(execution.OrderRequest{Symbol: "X/Y", Side: execution.SideBuy,
		Quantity: decimal.RequireFromString("1"), LimitPrice: decimal.RequireFromString("100"), OrderType: execution.OrderTypeLimit})
	c := it.seedRequest(t, queue.TypePlaceOrder, string(payload), &orderID)
	it.exec.process(it.ctx, c)

	if s := reqStatus(t, it.db, c.ID); s != "SUCCEEDED" {
		t.Errorf("place success status = %s", s)
	}
	if os := ordState(t, it.db, orderID); os != string(state.OrderSubmitted) {
		t.Errorf("order state = %s, want SUBMITTED", os)
	}
	var exOID string
	it.db.QueryRow("SELECT exchange_order_id FROM orders WHERE id=?", orderID).Scan(&exOID)
	if exOID != "EX123" {
		t.Errorf("exchange_order_id = %q, want EX123", exOID)
	}
}

func TestPlaceDefiniteRejectionFailsAndOrderUnchanged(t *testing.T) {
	it := setup(t)
	orderID := it.seedOrder(t, string(state.OrderQueued))
	it.fake.placeErr = execution.ErrInsufficientBalance

	payload, _ := json.Marshal(execution.OrderRequest{Symbol: "X/Y", Side: execution.SideBuy, Quantity: decimal.RequireFromString("1")})
	c := it.seedRequest(t, queue.TypePlaceOrder, string(payload), &orderID)
	it.exec.process(it.ctx, c)

	if s := reqStatus(t, it.db, c.ID); s != "FAILED" {
		t.Errorf("definite-rejection status = %s, want FAILED", s)
	}
	if os := ordState(t, it.db, orderID); os != string(state.OrderQueued) {
		t.Errorf("order should be unchanged (QUEUED), got %s", os)
	}
}

func TestPlaceAmbiguousDeadAndReconcile(t *testing.T) {
	it := setup(t)
	orderID := it.seedOrder(t, string(state.OrderSubmitted))
	it.fake.placeErr = execution.ErrAckTimeout // ambiguous: maybe placed, maybe not

	payload, _ := json.Marshal(execution.OrderRequest{Symbol: "X/Y", Side: execution.SideBuy, Quantity: decimal.RequireFromString("1")})
	c := it.seedRequest(t, queue.TypePlaceOrder, string(payload), &orderID)
	it.exec.process(it.ctx, c)

	if s := reqStatus(t, it.db, c.ID); s != "DEAD" {
		t.Errorf("ambiguous status = %s, want DEAD (never re-sent)", s)
	}
	if os := ordState(t, it.db, orderID); os != string(state.OrderNeedsReconcile) {
		t.Errorf("order state = %s, want NEEDS_RECONCILE", os)
	}
}

func TestPlaceSuccessRollsBackWhenOrderTransitionInvalid(t *testing.T) {
	it := setup(t)
	// Order already FILLED (terminal): QUEUED->SUBMITTED is invalid, so the whole
	// completion tx must roll back -> request NOT marked SUCCEEDED.
	orderID := it.seedOrder(t, string(state.OrderFilled))
	it.fake.placeAck = execution.OrderAck{ExchangeOrderID: "EX999", Status: execution.StateOpen}

	payload, _ := json.Marshal(execution.OrderRequest{Symbol: "X/Y", Side: execution.SideBuy, Quantity: decimal.RequireFromString("1")})
	c := it.seedRequest(t, queue.TypePlaceOrder, string(payload), &orderID)
	it.exec.process(it.ctx, c)

	if s := reqStatus(t, it.db, c.ID); s == "SUCCEEDED" {
		t.Errorf("request must NOT be SUCCEEDED when order transition fails; got %s", s)
	}
	if os := ordState(t, it.db, orderID); os != string(state.OrderFilled) {
		t.Errorf("order must be unchanged (FILLED) after rollback, got %s", os)
	}
}

func TestCancelSuccess(t *testing.T) {
	it := setup(t)
	c := it.seedRequest(t, queue.TypeCancelOrder, `{"exchange_order_id":"EX1"}`, nil)
	it.exec.process(it.ctx, c)
	if s := reqStatus(t, it.db, c.ID); s != "SUCCEEDED" {
		t.Errorf("cancel success status = %s", s)
	}
}
