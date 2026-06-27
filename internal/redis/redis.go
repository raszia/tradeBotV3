// Package redis wraps the go-redis client used for LIVE MARKET DATA ONLY:
// latest order books, latest prices, and market-event pub/sub.
//
// HARD RULE: Redis is never the source of truth. Cycles, orders, fills, symbol
// locks, the exchange-request queue and balances live in MariaDB. If Redis is
// flushed or restarted the system must lose nothing authoritative — at worst it
// briefly lacks fresh market data until collectors repopulate it. The key
// schema and pub/sub helpers are added in PR5; this PR only establishes the
// connection with explicit pool sizing.
package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"v3TradeBot/internal/config"
)

// Pool defaults. go-redis otherwise sizes the pool at 10*NumCPU, which on a
// many-core host opens hundreds of idle connections that a single-threaded
// Redis must juggle in its event loop. 30/3 with idle+lifetime recycling has
// ample headroom for our throughput while keeping the connection count sane.
const (
	defaultPoolSize     = 30
	defaultMinIdleConns = 3
	defaultIdleTimeout  = 5 * time.Minute
	defaultMaxLifetime  = 1 * time.Hour

	// defaultMarketTTL bounds how long a cached order book / price snapshot lives
	// in Redis. Because Redis is only a cache (never source of truth), expiry is
	// harmless: collectors repopulate it. A short TTL means stale data disappears
	// rather than lingering after a collector dies.
	defaultMarketTTL = 5 * time.Minute
)

// Client wraps *goredis.Client. Safe for concurrent use.
type Client struct {
	rdb       *goredis.Client
	marketTTL time.Duration
}

// New constructs the client and pings to fail fast on a bad address.
func New(ctx context.Context, cfg config.RedisConfig) (*Client, error) {
	rdb := goredis.NewClient(buildOptions(cfg))
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	return &Client{rdb: rdb, marketTTL: defaultMarketTTL}, nil
}

// SetMarketTTL overrides the TTL applied to cached order-book/price snapshots.
func (c *Client) SetMarketTTL(d time.Duration) {
	if d > 0 {
		c.marketTTL = d
	}
}

// buildOptions is pure (no I/O) so default application is unit-testable without
// a live Redis.
func buildOptions(cfg config.RedisConfig) *goredis.Options {
	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = defaultPoolSize
	}
	return &goredis.Options{
		Addr:            cfg.Addr,
		Password:        cfg.Password,
		DB:              cfg.DB,
		PoolSize:        poolSize,
		MinIdleConns:    defaultMinIdleConns,
		ConnMaxIdleTime: defaultIdleTimeout,
		ConnMaxLifetime: defaultMaxLifetime,
	}
}

// Redis exposes the underlying client for subsystems (collector, dashboard)
// that need direct access. Kept narrow for now.
func (c *Client) Redis() *goredis.Client { return c.rdb }

// Ping verifies connectivity.
func (c *Client) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }

// Close releases the pool.
func (c *Client) Close() error { return c.rdb.Close() }
