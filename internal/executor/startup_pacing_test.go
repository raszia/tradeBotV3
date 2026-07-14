package executor

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"v3TradeBot/internal/exchanges"
	"v3TradeBot/internal/live"
	"v3TradeBot/internal/queue"

	"v3TradeBot/internal/clock"
)

// PR20 round-3 corrections #4 (synchronous startup loading) and #6 (pacing correctness).

// --- #4: nothing is claimed or sent before the startup load completes -------------------------

// TestStartupLoadBlocksClaimingUntilComplete: a SLOW initial tuning load must finish before
// any request is claimed or sent — otherwise early requests would run with uninitialized zero
// tuning (no pacing, wrong fallback). The first send must then use the configured pacing.
func TestStartupLoadBlocksClaimingUntilComplete(t *testing.T) {
	it := setup(t)
	var loaded atomic.Bool
	var sentBeforeLoad atomic.Bool
	release := make(chan struct{})

	it.fake.onBalances = func() {
		if !loaded.Load() {
			sentBeforeLoad.Store(true)
		}
	}
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "startup", AllowLiveExecution: true, PollInterval: 5 * time.Millisecond,
			Recovery: fastRecovery(),
			StartupLoad: func(ctx context.Context) error {
				<-release // the loader is slow
				loaded.Store(true)
				return nil
			},
		})
	it.seedBalanceRequest(t)

	ctx, cancel := context.WithCancel(it.ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- it.exec.Run(ctx) }()

	// While the loader is blocked, Run must not have claimed anything.
	time.Sleep(60 * time.Millisecond)
	if atomic.LoadInt32(&it.fake.balCount) != 0 {
		t.Error("a request was sent BEFORE the startup load completed")
	}
	close(release)

	// After the load, work proceeds.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt32(&it.fake.balCount) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&it.fake.balCount) == 0 {
		t.Fatal("no request was processed after the startup load completed")
	}
	if sentBeforeLoad.Load() {
		t.Error("a send happened before the tuning load finished")
	}
	cancel()
	<-done
}

// TestStartupLoadFailureStopsExecutor: an initial tuning-load failure aborts Run — in live
// mode that means ZERO real exchange mutations rather than trading on default zero values.
func TestStartupLoadFailureStopsExecutor(t *testing.T) {
	it := setup(t)
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "startup-fail", AllowLiveExecution: true, ExecutionMode: "live",
			Guard: live.NewGuard(it.db, clock.NewSystem(), nil), PollInterval: 5 * time.Millisecond,
			Recovery:    fastRecovery(),
			StartupLoad: func(ctx context.Context) error { return errors.New("exchange_configs unavailable") },
		})
	it.seedBuyCycle(t, "0.5")

	err := it.exec.Run(it.ctx)
	if err == nil {
		t.Fatal("Run must fail when the initial tuning load fails")
	}
	if got := atomic.LoadInt32(&it.fake.placeCount); got != 0 {
		t.Errorf("PlaceOrder called %d times after a failed startup load, want 0", got)
	}
	if got := atomic.LoadInt32(&it.fake.balCount); got != 0 {
		t.Errorf("a read was sent after a failed startup load (%d), want 0", got)
	}
}

// TestCooldownLoadFailureStopsExecutor: if durable cooldowns cannot be read at startup, the
// executor must not start — it would otherwise resume sending to a still-throttled venue.
func TestCooldownLoadFailureStopsExecutor(t *testing.T) {
	it := setup(t)
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "cooldown-load-fail", AllowLiveExecution: true, PollInterval: 5 * time.Millisecond,
			Recovery: fastRecovery()})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	// Break the table so the load errors.
	it.db.Exec("RENAME TABLE exchange_cooldowns TO exchange_cooldowns_hidden")
	defer it.db.Exec("RENAME TABLE exchange_cooldowns_hidden TO exchange_cooldowns")

	if err := it.exec.loadCooldowns(it.ctx); err == nil {
		t.Error("loadCooldowns must return an error when the table is unreadable")
	}
	if err := it.exec.Run(it.ctx); err == nil {
		t.Error("Run must abort when durable cooldowns cannot be loaded")
	}
	if got := atomic.LoadInt32(&it.fake.balCount); got != 0 {
		t.Errorf("a request was sent despite an unreadable cooldown table (%d), want 0", got)
	}
}

// --- #6: pacing correctness -------------------------------------------------------------------

// TestOneGetOrderConsumesOnePacingSlot: a single read-only request must reserve exactly ONE
// pacing slot. A duplicate reservation silently halves the exchange's effective rate budget
// (and makes every such request wait a full extra interval).
//
// The assertion is exact rather than timing-based: with a frozen clock, each reservation
// advances the pacer's next-slot mark by exactly one interval, so the mark's final position
// counts the reservations.
func TestOneGetOrderConsumesOnePacingSlot(t *testing.T) {
	it := setup(t)
	const perSec = 2
	interval := time.Second / perSec
	frozen := time.Now().UTC().Truncate(time.Second)
	it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
		Config{Name: "pace-once", AllowLiveExecution: true, Recovery: fastRecovery(),
			ExchangeTuningFor: func(string) (int, time.Duration) { return perSec, 0 }})
	if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
		t.Fatal(err)
	}
	it.exec.nowFn = func() time.Time { return frozen }

	c := it.seedFollowupGetOrder(t, "EXT-PACE")
	it.exec.process(it.ctx, c)

	it.exec.pacers.mu.Lock()
	next := it.exec.pacers.next[it.code]
	it.exec.pacers.mu.Unlock()
	slots := int(next.Sub(frozen) / interval)
	if slots != 1 {
		t.Errorf("one GET_ORDER consumed %d pacing slots, want exactly 1", slots)
	}
}

// TestPacingCancellationStopsEveryExchangeCall: when the context is cancelled while pacing,
// the request must NOT be marked in-flight and NOTHING may be sent. A request that never left
// the process is definitely unsent — turning it into an ambiguous mutation would be a lie.
func TestPacingCancellationStopsEveryExchangeCall(t *testing.T) {
	cases := []struct {
		name  string
		seed  func(it *intg, t *testing.T) queue.Claimed
		count func(it *intg) int32
	}{
		{"PLACE_ORDER", func(it *intg, t *testing.T) queue.Claimed {
			_, _, req := it.seedBuyCycle(t, "0.5")
			return it.claimOf(t, req)
		}, func(it *intg) int32 { return atomic.LoadInt32(&it.fake.placeCount) }},
		{"CANCEL_ORDER", func(it *intg, t *testing.T) queue.Claimed {
			return it.seedFollowupCancel(t, "EXT-CANCEL")
		}, func(it *intg) int32 { return atomic.LoadInt32(&it.fake.cancelCount) }},
		{"GET_ORDER", func(it *intg, t *testing.T) queue.Claimed {
			return it.seedFollowupGetOrder(t, "EXT-GET")
		}, func(it *intg) int32 { return atomic.LoadInt32(&it.fake.getCount) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			it := setup(t)
			it.exec = New(it.store, it.q, map[string]exchanges.PrivateClient{it.code: it.fake}, nil,
				Config{Name: "pace-cancel", AllowLiveExecution: true, Recovery: fastRecovery(),
					// A slow pacing budget guarantees we are still waiting when cancelled.
					ExchangeTuningFor: func(string) (int, time.Duration) { return 1, 0 }})
			if err := it.exec.resolveExchangeIDs(it.ctx); err != nil {
				t.Fatal(err)
			}
			c := tc.seed(it, t)
			// Burn this exchange's first slot so the next reserve must wait ~1s.
			it.exec.pacers.reserve(it.code, 1, it.exec.nowFn())

			ctx, cancel := context.WithCancel(it.ctx)
			go func() { time.Sleep(20 * time.Millisecond); cancel() }()
			it.exec.process(ctx, c)

			if got := tc.count(it); got != 0 {
				t.Errorf("%s: %d exchange calls after cancellation during pacing, want 0", tc.name, got)
			}
			// The request must not be IN_FLIGHT ("maybe sent"): it definitely never left.
			if s := reqStatus(t, it.db, c.ID); s == "IN_FLIGHT" {
				t.Errorf("%s: request left IN_FLIGHT after a cancelled pace — it was never sent", tc.name)
			}
		})
	}
}

// seedFollowupGetOrder seeds a claimed read-only GET_ORDER for an order with a venue id.
func (it *intg) seedFollowupGetOrder(t *testing.T, extID string) queue.Claimed {
	t.Helper()
	return it.seedFollowup(t, queue.TypeGetOrder, extID)
}

// seedFollowupCancel seeds a claimed CANCEL_ORDER for an order with a venue id.
func (it *intg) seedFollowupCancel(t *testing.T, extID string) queue.Claimed {
	t.Helper()
	return it.seedFollowup(t, queue.TypeCancelOrder, extID)
}

func (it *intg) seedFollowup(t *testing.T, typ queue.RequestType, extID string) queue.Claimed {
	t.Helper()
	_, ord, _ := it.seedBuyCycle(t, "0.5")
	var cyc int64
	it.db.QueryRow("SELECT cycle_id FROM orders WHERE id=?", ord).Scan(&cyc)
	it.db.Exec("UPDATE orders SET state='ACKED', exchange_order_id=? WHERE id=?", extID, ord)
	fp := mustJSON(map[string]any{"exchange_order_id": extID, "purpose": "cancel_remainder"})
	res, err := it.db.Exec(`INSERT INTO exchange_requests (exchange_id, cycle_id, order_id, request_type, status, payload, timeout_ms, max_retries, idempotency_key)
		VALUES (?, ?, ?, ?, 'CLAIMED', ?, 10000, 5, ?)`, it.exID, cyc, ord, string(typ), fp,
		fmt.Sprintf("fu_%s_%d", typ, time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	return queue.Claimed{ID: id, ExchangeID: it.exID, ExchangeCode: it.code, Type: typ,
		Payload: fp, OrderID: &ord, CycleID: &cyc, TimeoutMS: 10000, MaxRetries: 5}
}

// claimOf loads a seeded request row as a Claimed (marking it CLAIMED first).
func (it *intg) claimOf(t *testing.T, id int64) queue.Claimed {
	t.Helper()
	it.db.Exec("UPDATE exchange_requests SET status='CLAIMED' WHERE id=?", id)
	var c queue.Claimed
	var payload []byte
	var typ string
	if err := it.db.QueryRow(`SELECT id, exchange_id, cycle_id, order_id, symbol, request_type, payload, timeout_ms, max_retries
		FROM exchange_requests WHERE id=?`, id).
		Scan(&c.ID, &c.ExchangeID, &c.CycleID, &c.OrderID, &c.Symbol, &typ, &payload, &c.TimeoutMS, &c.MaxRetries); err != nil {
		t.Fatal(err)
	}
	c.Type, c.Payload, c.ExchangeCode = queue.RequestType(typ), payload, it.code
	return c
}

// seedBalanceRequest queues a read-only GET_BALANCE.
func (it *intg) seedBalanceRequest(t *testing.T) int64 {
	t.Helper()
	r, err := it.db.Exec(`INSERT INTO exchange_requests (exchange_id, request_type, status, payload, timeout_ms, max_retries, idempotency_key)
		VALUES (?, 'GET_BALANCE', 'QUEUED', '{}', 10000, 5, ?)`, it.exID, fmt.Sprintf("bal_%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := r.LastInsertId()
	return id
}
