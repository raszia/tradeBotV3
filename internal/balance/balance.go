// Package balance is the continuous balance synchronizer. It polls each enabled
// exchange's READ-ONLY balance API and records the result in MariaDB:
// wallet_balances_current (one row per exchange/asset, upserted) and
// wallet_balance_history (append-only, written ONLY when the balance actually
// changed — hash-deduplicated). MariaDB is the source of truth; Redis is not used.
//
// HARD boundaries (PR13): it uses a narrow read-only client interface (Name +
// GetBalances) — no PlaceOrder/CancelOrder is reachable by construction. It makes no
// trading decisions, creates no cycles, and mutates no queue. One exchange failing
// never wipes/zeros another's balances, and a missing asset is never treated as zero.
package balance

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/clock"
	"v3TradeBot/internal/db"
	"v3TradeBot/internal/domain"
)

// BalanceClient is the READ-ONLY balance surface the syncer needs. It deliberately
// exposes ONLY Name + GetBalances, so a mutating call (PlaceOrder/CancelOrder) is
// not reachable from balance sync by construction. A full exchanges.PrivateClient
// satisfies this interface but is narrowed to these two methods here.
type BalanceClient interface {
	Name() string
	GetBalances(ctx context.Context) ([]domain.Balance, error)
}

// Config tunes the syncer. Defaults are filled by New.
type Config struct {
	// Interval is the poll cadence (default 30s).
	Interval time.Duration
	// Timeout bounds each exchange's GetBalances call (default 5s).
	Timeout time.Duration
	// MaxConcurrent bounds how many exchanges are polled at once (default 4) — never
	// an unbounded fan-out.
	MaxConcurrent int
}

func (c *Config) withDefaults() {
	if c.Interval <= 0 {
		c.Interval = 30 * time.Second
	}
	if c.Timeout <= 0 {
		c.Timeout = 5 * time.Second
	}
	if c.MaxConcurrent <= 0 {
		c.MaxConcurrent = 4
	}
}

// Syncer polls a set of read-only balance clients into the balance tables.
type Syncer struct {
	store   *db.Store
	clients map[string]BalanceClient // exchange code -> read-only client
	exIDs   map[string]int64         // exchange code -> id
	clk     clock.Clock
	log     *slog.Logger
	cfg     Config
}

// New builds a Syncer. With no clients it has nothing to poll and idles safely.
func New(store *db.Store, clients map[string]BalanceClient, clk clock.Clock, log *slog.Logger, cfg Config) *Syncer {
	cfg.withDefaults()
	if clk == nil {
		clk = clock.NewSystem()
	}
	if log == nil {
		log = slog.Default()
	}
	if clients == nil {
		clients = map[string]BalanceClient{}
	}
	return &Syncer{store: store, clients: clients, exIDs: map[string]int64{}, clk: clk, log: log, cfg: cfg}
}

// Run resolves exchange ids then polls every Interval until ctx is cancelled. It is
// safe to start with no clients (it simply idles).
func (s *Syncer) Run(ctx context.Context) error {
	if err := s.resolveExchangeIDs(ctx); err != nil {
		return err
	}
	s.log.Info("balance-sync starting", "exchanges", len(s.clients), "interval", s.cfg.Interval)
	t := time.NewTicker(s.cfg.Interval)
	defer t.Stop()
	s.SyncAll(ctx) // initial pass so balances are recorded promptly
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.SyncAll(ctx)
		}
	}
}

func (s *Syncer) resolveExchangeIDs(ctx context.Context) error {
	for code := range s.clients {
		var id int64
		err := s.store.DB().QueryRowContext(ctx, "SELECT id FROM exchanges WHERE code = ?", code).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			continue // unknown exchange: skip (its balances simply aren't recorded)
		}
		if err != nil {
			return err
		}
		s.exIDs[code] = id
	}
	return nil
}

// SyncAll polls every exchange concurrently (bounded by MaxConcurrent). A single
// exchange's failure is logged and isolated — it never affects the others, and never
// wipes/zeros the failing exchange's already-recorded balances.
func (s *Syncer) SyncAll(ctx context.Context) {
	sem := make(chan struct{}, s.cfg.MaxConcurrent)
	var wg sync.WaitGroup
	for code, client := range s.clients {
		exID, ok := s.exIDs[code]
		if !ok {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(code string, exID int64, client BalanceClient) {
			defer wg.Done()
			defer func() { <-sem }()
			s.syncExchange(ctx, code, exID, client)
		}(code, exID, client)
	}
	wg.Wait()
}

func (s *Syncer) syncExchange(ctx context.Context, code string, exID int64, client BalanceClient) {
	cctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	balances, err := client.GetBalances(cctx)
	if err != nil {
		// Record the failure (no secrets) and STOP here for this exchange: do NOT
		// touch its current balances. A failed read is never proof the balance is zero.
		s.log.Warn("balance fetch failed; keeping previous balances", "exchange", code, "err", err)
		return
	}
	if err := s.recordSnapshot(ctx, exID, balances); err != nil {
		s.log.Warn("balance snapshot record failed", "exchange", code, "err", err)
	}
}

// recordSnapshot upserts current + appends history (only on change) for one
// exchange's observed balances, in one transaction. Only assets PRESENT in the
// response are touched; a previously-known asset that is absent here is left
// untouched (its last_seen_at goes stale) — never zeroed or deleted.
func (s *Syncer) recordSnapshot(ctx context.Context, exID int64, balances []domain.Balance) error {
	return s.store.WithTx(ctx, func(tx *sql.Tx) error {
		for _, b := range balances {
			if b.Asset == "" {
				continue
			}
			total := b.Total
			if !total.IsPositive() {
				total = b.Available.Add(b.Locked) // derive when the venue omits total
			}
			hash := balanceHash(b.Asset, b.Available, b.Locked, total)

			var oldHash sql.NullString
			err := tx.QueryRowContext(ctx,
				"SELECT balance_hash FROM wallet_balances_current WHERE exchange_id=? AND asset=?", exID, b.Asset).Scan(&oldHash)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			changed := !oldHash.Valid || oldHash.String != hash
			if changed {
				if _, err := tx.ExecContext(ctx,
					"INSERT INTO wallet_balance_history (exchange_id, asset, available, locked, total, balance_hash) VALUES (?, ?, ?, ?, ?, ?)",
					exID, b.Asset, b.Available.String(), b.Locked.String(), total.String(), hash); err != nil {
					return err
				}
			}
			// Upsert current; last_seen_at is bumped on EVERY observation, the balance
			// columns/hash only effectively change when the values change.
			if _, err := tx.ExecContext(ctx, `
INSERT INTO wallet_balances_current (exchange_id, asset, available, locked, total, balance_hash, last_seen_at)
VALUES (?, ?, ?, ?, ?, ?, NOW(6))
ON DUPLICATE KEY UPDATE available=VALUES(available), locked=VALUES(locked), total=VALUES(total),
  balance_hash=VALUES(balance_hash), last_seen_at=NOW(6)`,
				exID, b.Asset, b.Available.String(), b.Locked.String(), total.String(), hash); err != nil {
				return err
			}
		}
		return nil
	})
}

// balanceHash is a stable content hash of a balance. decimal.String() is canonical
// (no trailing-zero ambiguity), so equal balances hash equally and any change to
// available/locked/total flips the hash.
func balanceHash(asset string, available, locked, total decimal.Decimal) string {
	h := sha256.Sum256([]byte(asset + "|" + available.String() + "|" + locked.String() + "|" + total.String()))
	return hex.EncodeToString(h[:])
}
