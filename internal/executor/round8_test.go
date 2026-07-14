package executor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"v3TradeBot/internal/exchanges"
)

// PR20 correction round 8. Two-stage boundary at the executor level (#1/#2/#3) and
// authoritative ownership in stale recovery (#4).

func (it *intg) cycleSymbol(t *testing.T, cyc int64) string {
	t.Helper()
	var s string
	if err := it.db.QueryRow("SELECT canonical_symbol FROM cycles WHERE id=?", cyc).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

// getOrderProbeCount counts ANY recovery probe (place or cancel) referencing an order.
func (it *intg) getOrderProbeCount(t *testing.T, orderID int64) int {
	t.Helper()
	var n int
	it.db.QueryRow(`SELECT COUNT(*) FROM exchange_requests WHERE order_id=? AND request_type='GET_ORDER'`, orderID).Scan(&n)
	return n
}

func (it *intg) pacerSlots(frozen time.Time, perSec int) int {
	it.exec.pacers.mu.Lock()
	next := it.exec.pacers.next[it.code]
	it.exec.pacers.mu.Unlock()
	if next.IsZero() {
		return 0
	}
	return int(next.Sub(frozen) / (time.Second / time.Duration(perSec)))
}

func (it *intg) resetPacer() {
	it.exec.pacers.mu.Lock()
	delete(it.exec.pacers.next, it.code)
	it.exec.pacers.mu.Unlock()
}

type failingCredsExec struct{}

func (failingCredsExec) Credentials(context.Context, string) (exchanges.Credentials, error) {
	return exchanges.Credentials{}, errors.New("credential db temporarily unavailable")
}

// bitpinExecServer counts auth vs order hits and serves valid token/order responses.
func bitpinExecServer(t *testing.T) (*httptest.Server, *int32, *int32) {
	t.Helper()
	var authHits, orderHits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/usr/authenticate/" || r.URL.Path == "/api/v1/usr/refresh_token/":
			atomic.AddInt32(&authHits, 1)
			_, _ = w.Write([]byte(`{"access":"ACCESS-1","refresh":"REFRESH-1"}`))
		case strings.HasPrefix(r.URL.Path, "/api/v1/odr/orders/"):
			atomic.AddInt32(&orderHits, 1)
			_, _ = w.Write([]byte(`{"id":"991","identifier":"c1","state":"active","type":"limit","side":"buy","symbol":"BTC_IRT","price":"100","base_amount":"0.5","remain_amount":"0.5","dealed_base_amount":"0"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &authHits, &orderHits
}

// TestBitpinExecutorPacingMatchesActualHTTPCalls (round 8 #3): end-to-end, a Bitpin place with a
// missing/expired token consumes TWO pacing slots (auth + order), and a subsequent place reusing
// the cached token consumes exactly ONE (order only) — pacing reservations equal actual HTTP calls.
func TestBitpinExecutorPacingMatchesActualHTTPCalls(t *testing.T) {
	it := setup(t)
	const perSec = 4
	frozen := time.Now().UTC().Truncate(time.Second)

	cycA, _, reqA := it.seedBuyCycle(t, "0.5")
	cycB, _, reqB := it.seedBuyCycle(t, "0.5")
	symA, symB := it.cycleSymbol(t, cycA), it.cycleSymbol(t, cycB)

	srv, authHits, orderHits := bitpinExecServer(t)
	client, err := exchanges.NewPrivateClient(exchanges.ClientConfig{
		Code: "bitpin", BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{symA: "BTC_IRT", symB: "BTC_IRT"},
		Creds:   exchanges.StaticCredentialProvider{Creds: exchanges.Credentials{APIKey: "k", APISecret: "s"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	it.installClient(t, client)
	it.exec.cfg.ExchangeTuningFor = func(string) (int, time.Duration) { return perSec, 0 }
	it.exec.nowFn = func() time.Time { return frozen }
	it.liveControls(0, true)
	it.db.Exec("UPDATE exchanges SET live_enabled=1")
	it.db.Exec("UPDATE exchange_markets SET live_enabled=1")
	it.enableAllLive()

	// Phase 1 — no cached token: auth call + order call → 2 pacing slots.
	it.exec.handlePlace(it.ctx, it.claimOf(t, reqA), client)
	if got := it.pacerSlots(frozen, perSec); got != 2 {
		t.Errorf("expired-token place consumed %d pacing slots, want 2 (auth + order)", got)
	}
	if a := atomic.LoadInt32(authHits); a != 1 {
		t.Errorf("auth endpoint calls = %d, want 1", a)
	}

	// Phase 2 — cached token: no auth call → 1 pacing slot.
	it.resetPacer()
	atomic.StoreInt32(authHits, 0)
	atomic.StoreInt32(orderHits, 0)
	it.exec.handlePlace(it.ctx, it.claimOf(t, reqB), client)
	if got := it.pacerSlots(frozen, perSec); got != 1 {
		t.Errorf("cached-token place consumed %d pacing slots, want 1 (order only)", got)
	}
	if a := atomic.LoadInt32(authHits); a != 0 {
		t.Errorf("auth endpoint calls = %d with a cached token, want 0", a)
	}
}

// TestTwoStageCredentialFailureNeverInFlight (round 8 #1): a credential-preparation failure on a
// real Nobitex/Wallex client (both PlaceOrder and CancelOrder) leaves the request NOT IN_FLIGHT,
// makes zero mutation HTTP calls, and creates no ambiguous recovery probe.
func TestTwoStageCredentialFailureNeverInFlight(t *testing.T) {
	cases := []struct {
		name string
		code string
		nat  string
	}{
		{"nobitex", "nobitex", "BTCIRT"},
		{"wallex", "wallex", "BTCTMN"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			it := setup(t)
			var hits int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&hits, 1)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{}`))
			}))
			t.Cleanup(srv.Close)

			cyc, ord, req := it.seedBuyCycle(t, "0.5")
			sym := it.cycleSymbol(t, cyc)
			client, err := exchanges.NewPrivateClient(exchanges.ClientConfig{
				Code: tc.code, BaseURL: srv.URL, HTTPClient: srv.Client(),
				Symbols: map[string]string{sym: tc.nat}, Creds: failingCredsExec{},
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			it.installClient(t, client)
			it.liveControls(0, true)
			it.db.Exec("UPDATE exchanges SET live_enabled=1")
			it.db.Exec("UPDATE exchange_markets SET live_enabled=1")
			it.enableAllLive()

			// PlaceOrder path.
			it.exec.handlePlace(it.ctx, it.claimOf(t, req), client)
			if s := reqStatus(t, it.db, req); s == "IN_FLIGHT" {
				t.Errorf("place: request = IN_FLIGHT after a credential-prep failure, want NOT IN_FLIGHT")
			}
			if n := atomic.LoadInt32(&hits); n != 0 {
				t.Errorf("place: %d mutation HTTP calls after a credential failure, want 0", n)
			}
			if p := it.getOrderProbeCount(t, ord); p != 0 {
				t.Errorf("place: %d recovery probes created, want 0 (definitely not sent)", p)
			}

			// CancelOrder path: seed a cancel request on the same order.
			atomic.StoreInt32(&hits, 0)
			cancelReq := it.seedCancelFor(t, cyc, ord, "EXT-123")
			it.exec.handleCancel(it.ctx, it.claimOf(t, cancelReq), client)
			if s := reqStatus(t, it.db, cancelReq); s == "IN_FLIGHT" {
				t.Errorf("cancel: request = IN_FLIGHT after a credential-prep failure, want NOT IN_FLIGHT")
			}
			if n := atomic.LoadInt32(&hits); n != 0 {
				t.Errorf("cancel: %d mutation HTTP calls after a credential failure, want 0", n)
			}
		})
	}
}

// seedCancelFor inserts a CLAIMED CANCEL_ORDER for an order, with a followup payload carrying the
// exchange order id, mirroring what the sell/repricing flow enqueues.
func (it *intg) seedCancelFor(t *testing.T, cyc, ord int64, extID string) int64 {
	t.Helper()
	payload := `{"exchange_order_id":"` + extID + `"}`
	res, err := it.db.Exec(`INSERT INTO exchange_requests
		(exchange_id, cycle_id, order_id, symbol, request_type, priority, status, payload, timeout_ms, max_retries, idempotency_key)
		VALUES (?, ?, ?, 'X/IRT', 'CANCEL_ORDER', 60, 'CLAIMED', ?, 10000, 5, ?)`,
		it.exID, cyc, ord, payload, "cxl_"+time.Now().Format("150405.000000000"))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id
}

// --- round 8 #4: authoritative ownership in stale recovery -----------------------------------

// TestStaleRecoveryOwnershipMismatchPlace: a stale IN_FLIGHT PLACE whose order belongs to cycle A
// but whose queue row claims cycle B must NOT probe cycle B. Cycle/lock B are untouched; the ACTUAL
// order + cycle A go NEEDS_RECONCILE with lock A held; the request is DEAD; no probe is created.
func TestStaleRecoveryOwnershipMismatchPlace(t *testing.T) {
	assertStaleOwnershipMismatch(t, "PLACE_ORDER")
}

func TestStaleRecoveryOwnershipMismatchCancel(t *testing.T) {
	assertStaleOwnershipMismatch(t, "CANCEL_ORDER")
}

func assertStaleOwnershipMismatch(t *testing.T, typ string) {
	t.Helper()
	it := setup(t)
	it.iocExec(t)
	cycA, ordA, _ := it.seedBuyCycle(t, "0.5") // owns ordA
	cycB, _, _ := it.seedBuyCycle(t, "0.5")    // unrelated; claimed by the stale row

	// Stale row: order_id = ordA (cycle A) but cycle_id = cycB — mixed ownership.
	req := it.seedRawMutation(t, typ, &cycB, &ordA, "IN_FLIGHT", time.Hour)

	it.exec.recoverStaleMutating(it.ctx, 5)

	if s := reqStatus(t, it.db, req); s != "DEAD" {
		t.Errorf("mixed-ownership stale %s = %s, want DEAD", typ, s)
	}
	// Actual order + cycle A reconcile; lock A held.
	if st := it.orderState(ordA); st != "NEEDS_RECONCILE" {
		t.Errorf("order A = %s, want NEEDS_RECONCILE", st)
	}
	if st := it.cycleState(cycA); st != "NEEDS_RECONCILE" {
		t.Errorf("cycle A = %s, want NEEDS_RECONCILE", st)
	}
	if st := it.lockStateByCycle(cycA); st != "ACTIVE" {
		t.Errorf("lock A = %s, want ACTIVE (held)", st)
	}
	// Unrelated cycle B and its lock are UNCHANGED.
	if st := it.cycleState(cycB); st != "BUY_REQUEST_QUEUED" {
		t.Errorf("unrelated cycle B = %s, want unchanged BUY_REQUEST_QUEUED", st)
	}
	if st := it.lockStateByCycle(cycB); st != "ACTIVE" {
		t.Errorf("unrelated lock B = %s, want unchanged ACTIVE", st)
	}
	// No probe was created against the (wrong) claimed cycle or the order.
	if p := it.getOrderProbeCount(t, ordA); p != 0 {
		t.Errorf("%d probes created for a mixed-ownership stale row, want 0", p)
	}
}

// TestStaleRecoveryNullCycleDerivesFromOrderPlace: a stale IN_FLIGHT PLACE with a valid order_id
// and NULL cycle_id derives the actual cycle FROM THE ORDER — request DEAD, actual order+cycle
// NEEDS_RECONCILE, lock held (never finalizes only the queue row).
func TestStaleRecoveryNullCycleDerivesFromOrderPlace(t *testing.T) {
	assertStaleNullCycleDerives(t, "PLACE_ORDER")
}

func TestStaleRecoveryNullCycleDerivesFromOrderCancel(t *testing.T) {
	assertStaleNullCycleDerives(t, "CANCEL_ORDER")
}

func assertStaleNullCycleDerives(t *testing.T, typ string) {
	t.Helper()
	it := setup(t)
	it.iocExec(t)
	cyc, ord, _ := it.seedBuyCycle(t, "0.5")

	// Stale row: valid order_id, NULL cycle_id.
	req := it.seedRawMutation(t, typ, nil, &ord, "IN_FLIGHT", time.Hour)

	it.exec.recoverStaleMutating(it.ctx, 5)

	if s := reqStatus(t, it.db, req); s != "DEAD" {
		t.Errorf("NULL-cycle stale %s = %s, want DEAD", typ, s)
	}
	if st := it.orderState(ord); st != "NEEDS_RECONCILE" {
		t.Errorf("order = %s, want NEEDS_RECONCILE (cycle derived from order)", st)
	}
	if st := it.cycleState(cyc); st != "NEEDS_RECONCILE" {
		t.Errorf("cycle = %s, want NEEDS_RECONCILE (derived from order, not left unchanged)", st)
	}
	if st := it.lockStateByCycle(cyc); st != "ACTIVE" {
		t.Errorf("lock = %s, want ACTIVE (held)", st)
	}
	if p := it.getOrderProbeCount(t, ord); p != 0 {
		t.Errorf("%d probes created for a NULL-cycle stale row, want 0", p)
	}
}

// TestStaleRecoveryConsistentOwnershipStillProbes: the happy path is preserved — a stale IN_FLIGHT
// PLACE with matching order/cycle/exchange schedules a read-only probe (using the order's
// authoritative cycle) and marks the request DEAD.
func TestStaleRecoveryConsistentOwnershipStillProbes(t *testing.T) {
	it := setup(t)
	it.iocExec(t)
	cyc, ord, _ := it.seedBuyCycle(t, "0.5")
	req := it.seedRawMutation(t, "PLACE_ORDER", &cyc, &ord, "IN_FLIGHT", time.Hour)

	it.exec.recoverStaleMutating(it.ctx, 5)

	if s := reqStatus(t, it.db, req); s != "DEAD" {
		t.Errorf("consistent stale place = %s, want DEAD (converted to a probe)", s)
	}
	if p := it.getOrderProbeCount(t, ord); p != 1 {
		t.Errorf("consistent stale place created %d probes, want 1", p)
	}
	// The order and cycle are NOT force-reconciled on the consistent path (the probe decides).
	if st := it.cycleState(cyc); st == "NEEDS_RECONCILE" {
		t.Errorf("cycle = NEEDS_RECONCILE on the consistent probe path, want left for the probe to resolve")
	}
}
