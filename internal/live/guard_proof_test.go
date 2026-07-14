package live

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"v3TradeBot/internal/clock"
)

// PR20 correction rounds — the guard must PROVE what it is about to send, from the database:
// #2 entry-buy identity/ownership/market/symbol (fail closed on every lookup error),
// #3 the exact cancel target, #4 oversell math that counts fills from CANCELLED sells.

// --- #2: entry buy ---------------------------------------------------------------------------

// TestEntryBuyIdentityDenials: every unidentifiable or mismatched entry buy is denied, so no
// PlaceOrder can follow. Market id 0 / a missing row / a DB error are never "safe".
func TestEntryBuyIdentityDenials(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(f *lfix, p *PlaceCheck)
	}{
		{"missing order (no row)", func(f *lfix, p *PlaceCheck) { p.OrderID = 999_000_111 }},
		{"market row deleted under the order", func(f *lfix, p *PlaceCheck) {
			// The schema's FKs make a DANGLING exchange_market_id impossible (a NOT NULL
			// column with fk_orders_market), so the JOIN can only break if the whole
			// relationship goes away. Deleting the order's market simulates that: the
			// identity query then returns NO ROW and must deny rather than resolve a zero.
			f.exec("DELETE FROM orders WHERE id=?", p.OrderID)
		}},
		{"wrong exchange ownership", func(f *lfix, p *PlaceCheck) {
			other, _ := f.seedMarket(1, 1)
			p.ExchangeID = other // request claims an exchange the order does not belong to
		}},
		{"order belongs to another cycle", func(f *lfix, p *PlaceCheck) { p.CycleID = p.CycleID + 100_000 }},
		{"market belongs to another exchange", func(f *lfix, p *PlaceCheck) {
			_, otherEM := f.seedMarket(1, 1) // market of a DIFFERENT exchange
			f.exec("UPDATE orders SET exchange_market_id=? WHERE id=?", otherEM, p.OrderID)
			p.ExchangeMarketID = otherEM
		}},
		{"disabled symbol", func(f *lfix, p *PlaceCheck) {
			f.exec("UPDATE exchange_markets SET live_enabled=0 WHERE id=?", f.em)
		}},
		{"disabled exchange", func(f *lfix, p *PlaceCheck) {
			f.exec("UPDATE exchanges SET live_enabled=0 WHERE id=?", f.ex)
		}},
		{"mismatched payload symbol", func(f *lfix, p *PlaceCheck) { p.Symbol = "SOMETHING/ELSE" }},
		{"empty payload symbol", func(f *lfix, p *PlaceCheck) { p.Symbol = "" }},
		{"order is not an entry buy", func(f *lfix, p *PlaceCheck) {
			f.exec("UPDATE orders SET role='exit_sell' WHERE id=?", p.OrderID)
		}},
		{"order not in a sendable state", func(f *lfix, p *PlaceCheck) {
			f.exec("UPDATE orders SET state='ACKED' WHERE id=?", p.OrderID)
		}},
		{"dry-run cycle must never go live", func(f *lfix, p *PlaceCheck) {
			f.exec("UPDATE cycles SET dry_run=1 WHERE id=?", p.CycleID)
		}},
		{"resolved market disagrees with the order", func(f *lfix, p *PlaceCheck) {
			_, otherEM := f.seedMarket(1, 1)
			p.ExchangeMarketID = otherEM // caller resolved a different market than the order's
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := setupL(t)
			p := f.buy("50", "0.5")
			c.mutate(f, &p)
			if d := f.g.CheckPlace(f.ctx, p); d.Allow {
				t.Errorf("%s must deny the live buy, got allow", c.name)
			}
		})
	}
}

// TestEntryBuyIdentityDBErrorDenies: a database error during the identity lookup denies —
// market id 0 is never a safe fallback that could skip the symbol-level enabled check.
func TestEntryBuyIdentityDBErrorDenies(t *testing.T) {
	f := setupL(t)
	p := f.buy("50", "0.5")
	broken, err := sql.Open("mysql", os.Getenv("V3_TEST_MYSQL_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	broken.Close()
	g := NewGuard(broken, clock.NewSystem(), nil)
	if d := g.checkEntryBuyIdentity(f.ctx, p); d.Allow {
		t.Error("a DB error during entry-buy identity resolution must deny")
	}
	if d := g.CheckPlace(f.ctx, p); d.Allow {
		t.Error("CheckPlace must deny when the identity lookup errors")
	}
}

// --- #3: cancel target -----------------------------------------------------------------------

// TestCancelTargetDenials: the payload's exchange_order_id is never trusted alone — every
// ownership/identity/state mismatch denies, so no real CancelOrder follows.
func TestCancelTargetDenials(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(f *lfix, p *PlaceCheck)
	}{
		{"payload references another order's exchange id", func(f *lfix, p *PlaceCheck) {
			other := f.cancelOf()
			p.ExchangeOrderID = other.ExchangeOrderID // real id, but NOT this order's
		}},
		{"payload exchange id is empty", func(f *lfix, p *PlaceCheck) { p.ExchangeOrderID = "" }},
		{"order belongs to another cycle", func(f *lfix, p *PlaceCheck) { p.CycleID = p.CycleID + 100_000 }},
		{"order belongs to another exchange", func(f *lfix, p *PlaceCheck) {
			other, _ := f.seedMarket(1, 1)
			f.seedCredentialFor(other)
			p.ExchangeID = other
		}},
		{"order already terminal", func(f *lfix, p *PlaceCheck) {
			f.exec("UPDATE orders SET state='FILLED' WHERE id=?", p.OrderID)
		}},
		{"order never reached the venue (QUEUED)", func(f *lfix, p *PlaceCheck) {
			f.exec("UPDATE orders SET state='QUEUED' WHERE id=?", p.OrderID)
		}},
		{"order has no registered exchange id", func(f *lfix, p *PlaceCheck) {
			f.exec("UPDATE orders SET exchange_order_id=NULL WHERE id=?", p.OrderID)
		}},
		{"missing order row", func(f *lfix, p *PlaceCheck) { p.OrderID = 999_000_333 }},
		{"no order context at all", func(f *lfix, p *PlaceCheck) { p.OrderID, p.CycleID = 0, 0 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := setupL(t)
			p := f.cancelOf()
			c.mutate(f, &p)
			if d := f.g.CheckCancel(f.ctx, p); d.Allow {
				t.Errorf("%s must deny the cancel, got allow", c.name)
			}
		})
	}
}

// TestCancelTargetLookupErrorDenies: a DB failure while proving the cancel target denies.
func TestCancelTargetLookupErrorDenies(t *testing.T) {
	f := setupL(t)
	p := f.cancelOf()
	broken, err := sql.Open("mysql", os.Getenv("V3_TEST_MYSQL_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	broken.Close()
	if d := NewGuard(broken, clock.NewSystem(), nil).CheckCancel(f.ctx, p); d.Allow {
		t.Error("a DB error while proving the cancel target must deny")
	}
}

// TestCancelProvenTargetAllowed: the honest path still works (and stays allowed under the
// kill switch — cancels reduce risk).
func TestCancelProvenTargetAllowed(t *testing.T) {
	f := setupL(t)
	f.set("kill_switch", 1)
	if d := f.g.CheckCancel(f.ctx, f.cancelOf()); !d.Allow {
		t.Errorf("a proven cancel target must be allowed: %s", d.Reason)
	}
}

// --- #4: oversell must include fills from CANCELLED sells -------------------------------------

// TestOversellCountsCancelledSellFills is the reviewer's scenario: a sell that was CANCELLED
// after partially filling has permanently removed inventory. Excluding it by final state would
// let the next sell oversell.
func TestOversellCountsCancelledSellFills(t *testing.T) {
	cases := []struct {
		name        string
		bought      string
		cancelled   string // filled_quantity of a CANCELLED sell
		newSell     string
		wantAllowed bool
	}{
		{"cancelled sell filled 0.4, new sell 1.0 of 1.0 bought", "1.0", "0.4", "1.0", false},
		{"cancelled sell filled 0.4, new sell 0.6 of 1.0 bought", "1.0", "0.4", "0.6", true},
		{"cancelled sell filled 0.4, new sell 0.61 of 1.0 bought", "1.0", "0.4", "0.61", false},
		{"cancelled sell filled 0, new sell 1.0 of 1.0 bought", "1.0", "0", "1.0", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := setupL(t)
			p := f.seedProvenExit(c.bought, c.newSell)
			// A prior sell that partially filled and was then CANCELLED.
			f.seedSell(p.CycleID, "0.5", c.cancelled, "CANCELLED")
			d := f.g.CheckPlace(f.ctx, p)
			if d.Allow != c.wantAllowed {
				t.Errorf("allow = %t, want %t (%s)", d.Allow, c.wantAllowed, d.Reason)
			}
		})
	}
}

// TestOversellMultiplePartialAndActiveSells: already-sold units (any state) PLUS the unfilled
// remainder of still-active sells PLUS this request must fit in the acquired inventory.
func TestOversellMultiplePartialAndActiveSells(t *testing.T) {
	f := setupL(t)
	// Bought 1.0. Cancelled sell filled 0.2. A FILLED sell of 0.3. An ACKED sell of 0.4 that
	// has filled 0.1 → its remaining commitment is 0.3.
	//   already_sold = 0.2 + 0.3 + 0.1 = 0.6 ; remaining_active = 0.3 ; free = 1.0-0.9 = 0.1
	p := f.seedProvenExit("1.0", "0.1")
	f.seedSell(p.CycleID, "0.5", "0.2", "CANCELLED")
	f.seedSell(p.CycleID, "0.3", "0.3", "FILLED")
	f.seedSell(p.CycleID, "0.4", "0.1", "ACKED")
	if d := f.g.CheckPlace(f.ctx, p); !d.Allow {
		t.Errorf("a 0.1 sell exactly fits the remaining inventory: %s", d.Reason)
	}

	// The same cycle but asking for 0.2 → total 1.1 > 1.0 → denied.
	f2 := setupL(t)
	p2 := f2.seedProvenExit("1.0", "0.2")
	f2.seedSell(p2.CycleID, "0.5", "0.2", "CANCELLED")
	f2.seedSell(p2.CycleID, "0.3", "0.3", "FILLED")
	f2.seedSell(p2.CycleID, "0.4", "0.1", "ACKED")
	if d := f2.g.CheckPlace(f2.ctx, p2); d.Allow {
		t.Error("0.6 sold + 0.3 committed + 0.2 requested exceeds 1.0 bought — must deny")
	}
}

// TestNeedsReconcileSellCountsAsActiveCommitment: a NEEDS_RECONCILE sell may still be resting
// on the venue, so its remainder must count against inventory (conservative).
func TestNeedsReconcileSellCountsAsActiveCommitment(t *testing.T) {
	f := setupL(t)
	p := f.seedProvenExit("1.0", "0.5")
	f.seedSell(p.CycleID, "0.6", "0", "NEEDS_RECONCILE")
	if d := f.g.CheckPlace(f.ctx, p); d.Allow {
		t.Error("an unresolved sell's remainder must count as committed inventory")
	}
}

// seedSell inserts an additional exit sell on a cycle with an explicit state/filled quantity.
func (f *lfix) seedSell(cycleID int64, qty, filled, st string) int64 {
	lseq++
	r := f.exec(`INSERT INTO orders (cycle_id, exchange_id, exchange_market_id, side, role,
		local_client_order_id, state, quantity, limit_price, filled_quantity)
		VALUES (?, ?, ?, 'sell', 'exit_sell', ?, ?, ?, '100', ?)`,
		cycleID, f.ex, f.em, fmt.Sprintf("xs%d_%d", time.Now().UnixNano(), lseq), st, qty, filled)
	id, _ := r.LastInsertId()
	return id
}

// seedCredentialFor gives another exchange an active credential (so a test isolates the check
// under test rather than tripping the credential gate first).
func (f *lfix) seedCredentialFor(exchangeID int64) {
	f.exec("INSERT INTO exchange_credentials (exchange_id, label, enabled, status) VALUES (?, 'default', 1, 'active')", exchangeID)
}

// --- round 3 #5: the SENT payload must equal the persisted order --------------------------

// TestBuyPayloadMustMatchPersistedOrder: the queued mutation payload and the authoritative DB
// row are two INTERNAL values; any disagreement means we do not know what we are placing, so
// no PlaceOrder may follow. This is not venue-response matching — there is no tolerance.
func TestBuyPayloadMustMatchPersistedOrder(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(f *lfix, p *PlaceCheck)
	}{
		{"payload quantity differs from DB", func(f *lfix, p *PlaceCheck) { p.BaseQty = dec("0.6") }},
		{"payload quantity is zero", func(f *lfix, p *PlaceCheck) { p.BaseQty = dec("0") }},
		{"payload price differs from DB", func(f *lfix, p *PlaceCheck) { p.LimitPrice = dec("99") }},
		{"payload price is zero", func(f *lfix, p *PlaceCheck) { p.LimitPrice = dec("0") }},
		{"payload client id differs from DB", func(f *lfix, p *PlaceCheck) { p.LocalClientOrderID = "someone-elses-id" }},
		{"payload client id is empty", func(f *lfix, p *PlaceCheck) { p.LocalClientOrderID = "" }},
		{"payload order type differs from DB", func(f *lfix, p *PlaceCheck) { p.OrderType = "market" }},
		{"payload order type is empty", func(f *lfix, p *PlaceCheck) { p.OrderType = "" }},
		{"payload side contradicts the order role", func(f *lfix, p *PlaceCheck) { p.Side = "sell" }},
		{"DB order has no limit price", func(f *lfix, p *PlaceCheck) {
			f.exec("UPDATE orders SET limit_price=NULL WHERE id=?", p.OrderID)
		}},
		{"payload TIF differs from the DB's recorded TIF", func(f *lfix, p *PlaceCheck) {
			f.exec("UPDATE orders SET time_in_force='GTC' WHERE id=?", p.OrderID)
			p.TimeInForce = "IOC"
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := setupL(t)
			p := f.buy("50", "0.5")
			c.mutate(f, &p)
			if d := f.g.CheckPlace(f.ctx, p); d.Allow {
				t.Errorf("%s must deny the live buy, got allow", c.name)
			}
		})
	}
}

// TestBuyPayloadMatchAllowsEquivalentDecimalForm: the comparison is by VALUE, so a
// DECIMAL(36,18) round-trip ("0.50" vs "0.5") is not a spurious mismatch.
func TestBuyPayloadMatchAllowsEquivalentDecimalForm(t *testing.T) {
	f := setupL(t)
	p := f.buy("50", "0.5")
	p.BaseQty = dec("0.500")
	p.LimitPrice = dec("100.000")
	if d := f.g.CheckPlace(f.ctx, p); !d.Allow {
		t.Errorf("equivalent decimal forms must match: %s", d.Reason)
	}
}

// TestSellPayloadMustMatchPersistedOrder: same proof on the exit path, plus the market/symbol
// must be where the inventory was actually acquired — a sell in the wrong market is not an exit.
func TestSellPayloadMustMatchPersistedOrder(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(f *lfix, p *PlaceCheck)
	}{
		{"payload price differs from DB", func(f *lfix, p *PlaceCheck) { p.LimitPrice = dec("101") }},
		{"payload quantity differs from DB", func(f *lfix, p *PlaceCheck) { p.BaseQty = dec("0.25") }},
		{"payload client id differs from DB", func(f *lfix, p *PlaceCheck) { p.LocalClientOrderID = "wrong" }},
		{"payload order type differs from DB", func(f *lfix, p *PlaceCheck) { p.OrderType = "market" }},
		{"payload side contradicts the exit role", func(f *lfix, p *PlaceCheck) { p.Side = "buy" }},
		{"payload symbol differs from the DB market", func(f *lfix, p *PlaceCheck) { p.Symbol = "OTHER/IRT" }},
		{"sell market belongs to another exchange", func(f *lfix, p *PlaceCheck) {
			// Re-point the SELL at a market owned by a different exchange.
			_, otherEM := f.seedMarket(1, 1)
			f.exec("UPDATE orders SET exchange_market_id=? WHERE id=?", otherEM, p.OrderID)
			p.ExchangeMarketID = otherEM
		}},
		{"sell market is not where the inventory was acquired", func(f *lfix, p *PlaceCheck) {
			// A second market on the SAME exchange: ownership is fine, but we hold nothing there.
			em2 := f.seedMarketOn(f.ex, 1)
			f.exec("UPDATE orders SET exchange_market_id=? WHERE id=?", em2, p.OrderID)
			p.ExchangeMarketID = em2
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := setupL(t)
			p := f.seedProvenExit("1.0", "0.5")
			c.mutate(f, &p)
			if d := f.g.CheckPlace(f.ctx, p); d.Allow {
				t.Errorf("%s must deny the live sell, got allow", c.name)
			}
		})
	}
}

// seedMarketOn adds another exchange_market to an EXISTING exchange (a different symbol on the
// same venue), used to prove an exit must be routed to the acquired market specifically.
func (f *lfix) seedMarketOn(exchangeID int64, live int) int64 {
	lseq++
	last := func(r sql.Result) int64 { id, _ := r.LastInsertId(); return id }
	u := func(p string) string { return fmt.Sprintf("%s%d_%d", p, time.Now().UnixNano(), lseq) }
	sym := u("N") + "/IRT"
	b := last(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'crypto')", u("B2")))
	qa := last(f.exec("INSERT INTO assets (symbol, kind) VALUES (?, 'fiat')", u("Q2")))
	m := last(f.exec("INSERT INTO markets (canonical_symbol, base_asset_id, quote_asset_id, quote_asset_type) VALUES (?, ?, ?, 'OTHER')", sym, b, qa))
	return last(f.exec("INSERT INTO exchange_markets (exchange_id, market_id, exchange_symbol, canonical_symbol, live_enabled) VALUES (?, ?, ?, ?, ?)",
		exchangeID, m, u("ES2"), sym, live))
}

// --- round 4 #6: complete payload proof (sell symbol mandatory, exact TIF, sent-id) ---

// TestExitSellSymbolMandatory: an exit-sell payload with an empty symbol must be denied — an
// empty symbol is an unproven route, not "no opinion".
func TestExitSellSymbolMandatory(t *testing.T) {
	f := setupL(t)
	p := f.seedProvenExit("1.0", "0.5")
	p.Symbol = ""
	if d := f.g.CheckPlace(f.ctx, p); d.Allow {
		t.Error("an exit sell with an empty payload symbol must be denied")
	}
}

// TestTimeInForceNullSemantics: when the DB records no TIF, a non-empty payload TIF must be
// denied; when the DB records one, the payload must match it exactly.
func TestTimeInForceNullSemantics(t *testing.T) {
	// DB TIF NULL, payload sets FOK → deny.
	f := setupL(t)
	p := f.buy("50", "0.5")
	f.exec("UPDATE orders SET time_in_force=NULL WHERE id=?", p.OrderID)
	p.TimeInForce = "FOK"
	if d := f.g.CheckPlace(f.ctx, p); d.Allow {
		t.Error("DB TIF NULL but payload TIF set must be denied")
	}

	// DB TIF NULL, payload TIF empty → allow.
	f2 := setupL(t)
	p2 := f2.buy("50", "0.5")
	f2.exec("UPDATE orders SET time_in_force=NULL WHERE id=?", p2.OrderID)
	p2.TimeInForce = ""
	if d := f2.g.CheckPlace(f2.ctx, p2); !d.Allow {
		t.Errorf("DB TIF NULL + empty payload TIF must be allowed: %s", d.Reason)
	}

	// DB TIF present, payload mismatches → deny.
	f3 := setupL(t)
	p3 := f3.buy("50", "0.5")
	f3.exec("UPDATE orders SET time_in_force='GTC' WHERE id=?", p3.OrderID)
	p3.TimeInForce = "IOC"
	if d := f3.g.CheckPlace(f3.ctx, p3); d.Allow {
		t.Error("DB TIF present but payload TIF differs must be denied")
	}

	// DB TIF present, payload matches → allow.
	f4 := setupL(t)
	p4 := f4.buy("50", "0.5")
	f4.exec("UPDATE orders SET time_in_force='GTC' WHERE id=?", p4.OrderID)
	p4.TimeInForce = "gtc" // case-insensitive
	if d := f4.g.CheckPlace(f4.ctx, p4); !d.Allow {
		t.Errorf("matching TIF must be allowed: %s", d.Reason)
	}
}

// TestSentClientOrderIDProvenAgainstPersisted: when the payload carries the exact value about
// to be sent (ClientOrderIDSent), the guard proves it equals the persisted client_order_id_sent
// column — a mismatch, or nothing persisted, denies.
func TestSentClientOrderIDProvenAgainstPersisted(t *testing.T) {
	// Match → allow.
	f := setupL(t)
	p := f.buy("50", "0.5")
	f.exec("UPDATE orders SET client_order_id_sent='NORM-1' WHERE id=?", p.OrderID)
	p.ClientOrderIDSent = "NORM-1"
	if d := f.g.CheckPlace(f.ctx, p); !d.Allow {
		t.Errorf("matching sent id must be allowed: %s", d.Reason)
	}

	// Payload sent-id differs from the persisted value → deny.
	f2 := setupL(t)
	p2 := f2.buy("50", "0.5")
	f2.exec("UPDATE orders SET client_order_id_sent='NORM-1' WHERE id=?", p2.OrderID)
	p2.ClientOrderIDSent = "SOMETHING-ELSE"
	if d := f2.g.CheckPlace(f2.ctx, p2); d.Allow {
		t.Error("a sent id that differs from the persisted value must be denied")
	}

	// Payload provides a sent-id but nothing is persisted → deny (fail closed).
	f3 := setupL(t)
	p3 := f3.buy("50", "0.5")
	f3.exec("UPDATE orders SET client_order_id_sent=NULL WHERE id=?", p3.OrderID)
	p3.ClientOrderIDSent = "NORM-1"
	if d := f3.g.CheckPlace(f3.ctx, p3); d.Allow {
		t.Error("a sent id with no persisted value to prove against must be denied")
	}

	// Early guard (no sent-id supplied) does not require the persisted value.
	f4 := setupL(t)
	p4 := f4.buy("50", "0.5")
	p4.ClientOrderIDSent = "" // early guard
	if d := f4.g.CheckPlaceNoAudit(f4.ctx, p4); !d.Allow {
		t.Errorf("the early guard must not require a persisted sent id: %s", d.Reason)
	}
}
