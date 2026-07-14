package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/queue"
)

// PR20 correction round 9. Production-path authoritative ownership: candidate discovery and mode
// scoping in stale/malformed recovery are derived from the persisted ORDER, never the untrusted
// queue metadata (#1/#2/#3); and Bitpin auth rate limits schedule the queue retry at the real venue
// deadline (#4). These tests exercise the REAL sweep sequence with explicit ExecutionMode.

// --- helpers ---------------------------------------------------------------------------------

// runSweeps runs the executor's production recovery sweep sequence, exactly as Run does.
func runSweeps(ctx context.Context, ex *Executor, q *queue.Queue) {
	ex.recoverStaleMutating(ctx, 5)
	ex.sweepMalformedMutations(ctx, 200)
	_, _ = q.SweepStuck(ctx, 5)
	ex.sweepUnclaimableRecoveryProbes(ctx, 200)
}

func (it *intg) recoverySweeps(ctx context.Context) { runSweeps(ctx, it.exec, it.q) }

// modeExec builds an executor bound to an explicit ExecutionMode (live/dry_run/off), sharing this
// harness's DB, queue and exchange wiring.
func (it *intg) modeExec(t *testing.T, mode string) *Executor {
	t.Helper()
	ex := New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "mode-" + mode, AllowLiveExecution: true, ExecutionMode: mode,
			FinalStatusDelay: 10 * time.Millisecond, Recovery: fastRecovery()})
	if err := ex.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	return ex
}

// seedExchange inserts a second (real but unwired-to-this-executor) exchange row and returns its id.
func (it *intg) seedExchange(t *testing.T) int64 {
	t.Helper()
	code := "other_" + time.Now().Format("150405.000000000")
	res, err := it.db.Exec("INSERT INTO exchanges (code, name, enabled) VALUES (?, 'other', 1)", code)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return id
}

func (it *intg) setReqExchange(t *testing.T, req, exch int64) {
	t.Helper()
	if _, err := it.db.Exec("UPDATE exchange_requests SET exchange_id=? WHERE id=?", exch, req); err != nil {
		t.Fatal(err)
	}
}

func (it *intg) makeDryRun(t *testing.T, cyc int64) {
	t.Helper()
	if _, err := it.db.Exec("UPDATE cycles SET dry_run=1 WHERE id=?", cyc); err != nil {
		t.Fatal(err)
	}
}

// --- #4: Bitpin auth deadline drives the queue retry (executor level) ------------------------

func bitpinAuth429Server(t *testing.T, retryAfter string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/usr/authenticate/" || r.URL.Path == "/api/v1/usr/refresh_token/":
			w.Header().Set("Retry-After", retryAfter)
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"detail":"throttled"}`))
		case strings.HasPrefix(r.URL.Path, "/api/v1/odr/orders/"):
			_, _ = w.Write([]byte(`{"id":"1"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestBitpinAuthRetryAtUsesRealDeadline: with a venue Retry-After of 30s and a configured fallback
// of 1s, the queue retry is scheduled ~30s out (the real deadline), not ~1s.
func TestBitpinAuthRetryAtUsesRealDeadline(t *testing.T) {
	it := setup(t)
	cyc, _, req := it.seedBuyCycle(t, "0.5")
	sym := it.cycleSymbol(t, cyc)
	srv := bitpinAuth429Server(t, "30")
	client, err := exchanges.NewPrivateClient(exchanges.ClientConfig{
		Code: "bitpin", BaseURL: srv.URL, HTTPClient: srv.Client(),
		Symbols: map[string]string{sym: "BTC_IRT"},
		Creds:   exchanges.StaticCredentialProvider{Creds: exchanges.Credentials{APIKey: "k", APISecret: "s"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	it.installClient(t, client)
	// Configured fallback = 1s (retry_backoff_ms). The venue's 30s must win.
	it.exec.cfg.ExchangeTuningFor = func(string) (int, time.Duration) { return 0, time.Second }
	it.liveControls(0, true)
	it.db.Exec("UPDATE exchanges SET live_enabled=1")
	it.db.Exec("UPDATE exchange_markets SET live_enabled=1")
	it.enableAllLive()

	before := time.Now()
	it.exec.handlePlace(it.ctx, it.claimOf(t, req), client)

	// The order was definitely not sent → the request is re-queued (not IN_FLIGHT, not exhausted).
	if s := reqStatus(t, it.db, req); s == "IN_FLIGHT" || s == "DEAD" {
		t.Errorf("request = %s after an auth throttle, want a scheduled retry (RETRY_SCHEDULED/QUEUED)", s)
	}
	var nextRetry time.Time
	if err := it.db.QueryRow("SELECT next_retry_at FROM exchange_requests WHERE id=?", req).Scan(&nextRetry); err != nil {
		t.Fatal(err)
	}
	delay := nextRetry.Sub(before)
	if delay < 20*time.Second {
		t.Errorf("next_retry_at is %v out, want ≈30s (the venue deadline), not the 1s fallback", delay.Round(time.Second))
	}
}

// --- #1: valid order_id + NULL cycle_id, real sweep sequence, live and dry_run ---------------

func TestProdSweepNullCycleValidOrder(t *testing.T) {
	for _, tc := range []struct {
		name, mode, typ string
		dry             bool
	}{
		{"live_place", "live", "PLACE_ORDER", false},
		{"live_cancel", "live", "CANCEL_ORDER", false},
		{"dryrun_place", "dry_run", "PLACE_ORDER", true},
		{"dryrun_cancel", "dry_run", "CANCEL_ORDER", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			it := setup(t)
			cyc, ord, _ := it.seedBuyCycle(t, "0.5")
			if tc.dry {
				it.makeDryRun(t, cyc)
			}
			// Malformed historical row: valid order_id, NULL cycle_id.
			req := it.seedRawMutation(t, tc.typ, nil, &ord, "IN_FLIGHT", time.Hour)
			ex := it.modeExec(t, tc.mode)

			runSweeps(it.ctx, ex, it.q)

			if s := reqStatus(t, it.db, req); s != "DEAD" {
				t.Errorf("request = %s, want DEAD", s)
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
		})
	}
}

// --- #2: order-authoritative discovery (unwired claimed exchange; cross-mode claimed cycle) ---

// A stale mutation whose CLAIMED exchange is unwired/foreign must still be discovered via its order
// and finalized conservatively — never left IN_FLIGHT forever.
func TestStaleUnwiredClaimedExchangeStillRecovered(t *testing.T) {
	for _, typ := range []string{"PLACE_ORDER", "CANCEL_ORDER"} {
		t.Run(typ, func(t *testing.T) {
			it := setup(t)
			cyc, ord, _ := it.seedBuyCycle(t, "0.5") // order on the wired exchange, live cycle
			other := it.seedExchange(t)
			req := it.seedRawMutation(t, typ, &cyc, &ord, "IN_FLIGHT", time.Hour)
			it.setReqExchange(t, req, other) // claimed exchange ≠ the order's (and unwired)
			ex := it.modeExec(t, "live")

			runSweeps(it.ctx, ex, it.q)

			if s := reqStatus(t, it.db, req); s != "DEAD" {
				t.Errorf("request = %s, want DEAD (discovered via the order despite an unwired claimed exchange)", s)
			}
			if st := it.orderState(ord); st != "NEEDS_RECONCILE" {
				t.Errorf("order = %s, want NEEDS_RECONCILE", st)
			}
			if st := it.lockStateByCycle(cyc); st != "ACTIVE" {
				t.Errorf("lock = %s, want ACTIVE (held)", st)
			}
		})
	}
}

// A stale mutation whose order is in a LIVE cycle but whose claimed cycle is a DRY-RUN one: the
// dry-run executor must NOT touch it (the order's cycle is live), and the live executor finalizes
// it conservatively — the unrelated dry-run cycle/lock stay unchanged.
func TestStaleCrossModeClaimedCycleIsolation(t *testing.T) {
	for _, typ := range []string{"PLACE_ORDER", "CANCEL_ORDER"} {
		t.Run(typ, func(t *testing.T) {
			it := setup(t)
			cycA, ordA, _ := it.seedBuyCycle(t, "0.5") // order's real (live) cycle
			cycB, _, _ := it.seedBuyCycle(t, "0.5")
			it.makeDryRun(t, cycB) // unrelated dry-run cycle the row falsely claims
			req := it.seedRawMutation(t, typ, &cycB, &ordA, "IN_FLIGHT", time.Hour)

			// Dry-run executor must not touch a row whose ORDER is in a live cycle.
			dry := it.modeExec(t, "dry_run")
			runSweeps(it.ctx, dry, it.q)
			if s := reqStatus(t, it.db, req); s != "IN_FLIGHT" {
				t.Errorf("dry-run executor changed the row to %s; it must ignore a live-cycle order", s)
			}
			if st := it.cycleState(cycA); st == "NEEDS_RECONCILE" {
				t.Errorf("dry-run executor reconciled live cycle A (%s) — cross-mode violation", st)
			}

			// Live executor finalizes conservatively on the ORDER's cycle A; dry-run cycle B untouched.
			live := it.modeExec(t, "live")
			runSweeps(it.ctx, live, it.q)
			if s := reqStatus(t, it.db, req); s != "DEAD" {
				t.Errorf("request = %s, want DEAD", s)
			}
			if st := it.orderState(ordA); st != "NEEDS_RECONCILE" {
				t.Errorf("order A = %s, want NEEDS_RECONCILE", st)
			}
			if st := it.cycleState(cycA); st != "NEEDS_RECONCILE" {
				t.Errorf("cycle A = %s, want NEEDS_RECONCILE", st)
			}
			if st := it.lockStateByCycle(cycA); st != "ACTIVE" {
				t.Errorf("lock A = %s, want ACTIVE (held)", st)
			}
			if st := it.cycleState(cycB); st != "BUY_REQUEST_QUEUED" {
				t.Errorf("unrelated dry-run cycle B = %s, want unchanged", st)
			}
			if st := it.lockStateByCycle(cycB); st != "ACTIVE" {
				t.Errorf("unrelated dry-run lock B = %s, want unchanged ACTIVE", st)
			}
		})
	}
}

// --- #3: malformed sweep is scoped by authoritative execution mode ---------------------------

// A dry-run executor must not finalize a malformed request whose (authoritative) cycle is live.
func TestMalformedSweepDryRunExecutorSkipsLiveCycle(t *testing.T) {
	it := setup(t)
	cyc, _, _ := it.seedBuyCycle(t, "0.5") // live cycle
	req := it.seedRawMutation(t, "PLACE_ORDER", &cyc, nil, "QUEUED", 0)

	dry := it.modeExec(t, "dry_run")
	runSweeps(it.ctx, dry, it.q)

	if s := reqStatus(t, it.db, req); s != "QUEUED" {
		t.Errorf("dry-run executor changed a live-cycle malformed request to %s, want left QUEUED", s)
	}
	if st := it.cycleState(cyc); st != "BUY_REQUEST_QUEUED" {
		t.Errorf("live cycle = %s, want unchanged (dry-run must not reconcile it)", st)
	}
	if st := it.lockStateByCycle(cyc); st != "ACTIVE" {
		t.Errorf("live lock = %s, want unchanged ACTIVE", st)
	}

	// The live executor owns it and finalizes it.
	live := it.modeExec(t, "live")
	runSweeps(it.ctx, live, it.q)
	if s := reqStatus(t, it.db, req); s != "DEAD" {
		t.Errorf("live executor left the malformed request at %s, want DEAD", s)
	}
	if st := it.cycleState(cyc); st != "NEEDS_RECONCILE" {
		t.Errorf("live cycle = %s, want NEEDS_RECONCILE", st)
	}
}

// A live executor must not finalize a malformed request whose (authoritative) cycle is dry-run.
func TestMalformedSweepLiveExecutorSkipsDryRunCycle(t *testing.T) {
	it := setup(t)
	cyc, _, _ := it.seedBuyCycle(t, "0.5")
	it.makeDryRun(t, cyc)
	req := it.seedRawMutation(t, "PLACE_ORDER", &cyc, nil, "QUEUED", 0)

	live := it.modeExec(t, "live")
	runSweeps(it.ctx, live, it.q)

	if s := reqStatus(t, it.db, req); s != "QUEUED" {
		t.Errorf("live executor changed a dry-run-cycle malformed request to %s, want left QUEUED", s)
	}
	if st := it.cycleState(cyc); st != "BUY_REQUEST_QUEUED" {
		t.Errorf("dry-run cycle = %s, want unchanged", st)
	}
	if st := it.lockStateByCycle(cyc); st != "ACTIVE" {
		t.Errorf("dry-run lock = %s, want unchanged ACTIVE", st)
	}
}

// A valid order in a LIVE cycle whose queue row claims a DRY-RUN cycle: the ORDER's live cycle is
// authoritative — the live executor finalizes it and the unrelated dry-run cycle stays unchanged.
func TestMalformedSweepValidOrderLiveClaimsDryRun(t *testing.T) {
	it := setup(t)
	cycA, ordA, _ := it.seedBuyCycle(t, "0.5") // order's real live cycle
	cycB, _, _ := it.seedBuyCycle(t, "0.5")
	it.makeDryRun(t, cycB)
	// Inconsistent QUEUED row: valid order (cycle A, live) but claims dry-run cycle B.
	req := it.seedRawMutation(t, "PLACE_ORDER", &cycB, &ordA, "QUEUED", 0)

	// Dry-run executor must not touch it (order's cycle is live).
	dry := it.modeExec(t, "dry_run")
	runSweeps(it.ctx, dry, it.q)
	if st := it.cycleState(cycA); st == "NEEDS_RECONCILE" {
		t.Errorf("dry-run executor reconciled live cycle A — cross-mode violation")
	}

	// Live executor: authoritative live cycle A determines ownership; dry-run cycle B untouched.
	live := it.modeExec(t, "live")
	runSweeps(it.ctx, live, it.q)
	if s := reqStatus(t, it.db, req); s != "DEAD" {
		t.Errorf("request = %s, want DEAD", s)
	}
	if st := it.orderState(ordA); st != "NEEDS_RECONCILE" {
		t.Errorf("order A = %s, want NEEDS_RECONCILE", st)
	}
	if st := it.cycleState(cycA); st != "NEEDS_RECONCILE" {
		t.Errorf("cycle A = %s, want NEEDS_RECONCILE (authoritative from the order)", st)
	}
	if st := it.cycleState(cycB); st != "BUY_REQUEST_QUEUED" {
		t.Errorf("unrelated dry-run cycle B = %s, want unchanged", st)
	}
	if st := it.lockStateByCycle(cycB); st != "ACTIVE" {
		t.Errorf("unrelated dry-run lock B = %s, want unchanged ACTIVE", st)
	}
}
