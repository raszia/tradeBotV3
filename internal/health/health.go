package health

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"v3TradeBot/internal/clock"
)

// Kind identifies which surface a probe measured. Public and private REST health are
// tracked SEPARATELY — a healthy public API does not prove the private API is healthy.
type Kind string

const (
	KindPublic  Kind = "public"
	KindPrivate Kind = "private"
	KindWS      Kind = "ws"
)

// ProbeResult is one normalized health observation, ready to record.
type ProbeResult struct {
	Kind      Kind
	OK        bool
	LatencyMs int
	Status    Status
	Category  Category
	ErrSafe   string // human-safe error text (adapter errors are pre-masked); truncated on store
}

// Recorder upserts exchange_health_current and appends exchange_health_samples. It is
// the canonical, reusable health-reporting helper (other components can adopt it). It
// resolves+caches exchange code→id; an unknown code is skipped (no row written).
type Recorder struct {
	db  *sql.DB
	mu  sync.Mutex
	ids map[string]int64
}

// NewRecorder builds a Recorder over db.
func NewRecorder(db *sql.DB) *Recorder { return &Recorder{db: db, ids: map[string]int64{}} }

func (r *Recorder) resolveID(ctx context.Context, code string) (int64, bool) {
	r.mu.Lock()
	id, ok := r.ids[code]
	r.mu.Unlock()
	if ok {
		return id, true
	}
	if err := r.db.QueryRowContext(ctx, "SELECT id FROM exchanges WHERE code=?", code).Scan(&id); err != nil {
		return 0, false
	}
	r.mu.Lock()
	r.ids[code] = id
	r.mu.Unlock()
	return id, true
}

// Record persists one probe result: it upserts the per-exchange current row (status
// column for the kind, success/failure timestamps, consecutive-failure count, error
// counters/category/message) and appends a sample. Never stores secrets.
func (r *Recorder) Record(ctx context.Context, code string, res ProbeResult) error {
	id, ok := r.resolveID(ctx, code)
	if !ok {
		return nil
	}
	// Ensure the current row exists so the UPDATE below always matches.
	if _, err := r.db.ExecContext(ctx, "INSERT IGNORE INTO exchange_health_current (exchange_id) VALUES (?)", id); err != nil {
		return err
	}

	set := []string{}
	args := []any{}
	add := func(frag string, a ...any) {
		set = append(set, frag)
		args = append(args, a...)
	}

	switch res.Kind {
	case KindPublic:
		add("public_status=?", string(res.Status))
		add("rest_status=?", boolEnum(res.OK, "up", "down"))
	case KindPrivate:
		add("private_status=?", string(res.Status))
		if res.OK {
			add("api_key_status='ok'")
		} else if res.Category == CatAuth {
			add("api_key_status='invalid'") // only an auth error proves the key is bad
		}
	case KindWS:
		add("ws_status=?", boolEnum(res.OK, "up", "down"))
	}

	if res.LatencyMs > 0 {
		add("latency_ms=?", res.LatencyMs)
	}
	if res.OK {
		add("last_success_at=NOW(6)")
		add("consecutive_failures=0")
	} else {
		add("last_failure_at=NOW(6)")
		add("consecutive_failures=consecutive_failures+1")
		add("error_count=error_count+1")
		add("last_error_category=?", string(res.Category))
		add("last_error_message=?", truncate(res.ErrSafe, 255))
		if res.Category == CatTimeout {
			add("timeout_count=timeout_count+1")
		}
		if res.Category == CatRateLimit {
			add("rate_limit_error_count=rate_limit_error_count+1")
		}
		if res.Category == CatAuth {
			add("auth_error_count=auth_error_count+1")
		}
	}
	args = append(args, id)
	if _, err := r.db.ExecContext(ctx, "UPDATE exchange_health_current SET "+strings.Join(set, ", ")+" WHERE exchange_id=?", args...); err != nil {
		return err
	}

	// Append the sample (high-volume, timestamp-indexed, no FK on the write path).
	_, err := r.db.ExecContext(ctx,
		"INSERT INTO exchange_health_samples (exchange_id, sample_type, ok, latency_ms, error) VALUES (?, ?, ?, ?, ?)",
		id, string(res.Kind), b2i(res.OK), nullIfZeroMs(res.LatencyMs), nullIfEmpty(truncate(res.ErrSafe, 1000)))
	return err
}

// ---- Monitor ----

// ProbeFunc performs ONE read-only health check, returning an error (or nil). It is
// the only thing the Monitor invokes — there is no order-mutating surface here.
type ProbeFunc func(ctx context.Context) error

// Target is one exchange's probes. Private is nil when no authenticated client is
// available (private health then stays UNKNOWN — documented, never a panic).
type Target struct {
	ExchangeCode string
	Public       ProbeFunc
	Private      ProbeFunc
}

// Config tunes the monitor. Defaults are filled by NewMonitor.
type Config struct {
	Interval      time.Duration // poll cadence (default 15s)
	Timeout       time.Duration // per-probe timeout (default 5s)
	MaxConcurrent int           // bound on concurrent exchange probes (default 4)
}

func (c *Config) withDefaults() {
	if c.Interval <= 0 {
		c.Interval = 15 * time.Second
	}
	if c.Timeout <= 0 {
		c.Timeout = 5 * time.Second
	}
	if c.MaxConcurrent <= 0 {
		c.MaxConcurrent = 4
	}
}

// Monitor periodically probes each target and records normalized health.
type Monitor struct {
	rec     *Recorder
	targets []Target
	clk     clock.Clock
	log     *slog.Logger
	cfg     Config
}

// NewMonitor builds a Monitor. With no targets it idles safely.
func NewMonitor(rec *Recorder, targets []Target, clk clock.Clock, log *slog.Logger, cfg Config) *Monitor {
	cfg.withDefaults()
	if clk == nil {
		clk = clock.NewSystem()
	}
	if log == nil {
		log = slog.Default()
	}
	return &Monitor{rec: rec, targets: targets, clk: clk, log: log, cfg: cfg}
}

// Run probes every Interval until ctx is cancelled. Safe with no targets.
func (m *Monitor) Run(ctx context.Context) error {
	m.log.Info("health-monitor starting", "exchanges", len(m.targets), "interval", m.cfg.Interval)
	t := time.NewTicker(m.cfg.Interval)
	defer t.Stop()
	m.Pass(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			m.Pass(ctx)
		}
	}
}

// Pass probes all targets once, bounded by MaxConcurrent. One exchange's failure is
// isolated — it never stops the others.
func (m *Monitor) Pass(ctx context.Context) {
	sem := make(chan struct{}, m.cfg.MaxConcurrent)
	var wg sync.WaitGroup
	for _, t := range m.targets {
		wg.Add(1)
		sem <- struct{}{}
		go func(t Target) {
			defer wg.Done()
			defer func() { <-sem }()
			m.probeTarget(ctx, t)
		}(t)
	}
	wg.Wait()
}

func (m *Monitor) probeTarget(ctx context.Context, t Target) {
	m.runProbe(ctx, t.ExchangeCode, KindPublic, t.Public)
	if t.Private != nil {
		m.runProbe(ctx, t.ExchangeCode, KindPrivate, t.Private)
	}
	// When Private is nil, private health is intentionally left UNKNOWN (no
	// credentials wired yet) — never marked unhealthy on absence.
}

func (m *Monitor) runProbe(ctx context.Context, code string, kind Kind, probe ProbeFunc) {
	if probe == nil {
		return
	}
	pctx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	defer cancel()
	start := m.clk.Now()
	err := probe(pctx)
	latency := m.clk.Now().Sub(start)
	// A cancel caused by OUR shutdown is not a venue-health signal — don't record it.
	if err != nil && errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return
	}
	status, cat := Classify(err)
	if rerr := m.rec.Record(ctx, code, ProbeResult{
		Kind: kind, OK: err == nil, LatencyMs: int(latency.Milliseconds()),
		Status: status, Category: cat, ErrSafe: errText(err),
	}); rerr != nil {
		m.log.Warn("record health failed", "exchange", code, "kind", kind, "err", rerr)
	}
}

// ---- helpers ----

func boolEnum(ok bool, t, f string) string {
	if ok {
		return t
	}
	return f
}
func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func nullIfZeroMs(ms int) any {
	if ms <= 0 {
		return nil
	}
	return ms
}
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
