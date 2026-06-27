package exchanges

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"
)

// IO logging for raw exchange request/response data. This is the layer that
// produces api_call_logs rows. Every field is SECRET-MASKED (mask.go) before it
// is enqueued, so secrets never reach the database. Writes are async and
// non-blocking; the buffer drops on overflow rather than slowing a trading call.

// IOLogConfig controls the async IO logger. Zero value (Enabled=false) yields a
// nil logger that is safe to use (all methods are nil-safe no-ops).
type IOLogConfig struct {
	Enabled      bool
	BufSize      int    // channel depth before drops (default 4000)
	BodyMaxBytes int    // max bytes stored per body field (default 32KiB)
	Source       string // producing binary name, stored for triage
}

// APILogEntry is one already-masked request/response record.
type APILogEntry struct {
	Exchange    string
	Method      string
	URL         string            // already masked (MaskURL)
	ReqHeaders  map[string]string // already masked (MaskHeaders)
	RespHeaders map[string]string // already masked
	ReqBody     string            // already masked (MaskBody), not yet truncated
	RespBody    string            // already masked, not yet truncated
	Status      int
	LatencyMs   int64
	Err         string
	Timeout     bool
}

// IOLogger asynchronously writes APILogEntry rows to api_call_logs. Nil-safe.
type IOLogger struct {
	cfg  IOLogConfig
	db   *sql.DB
	ch   chan APILogEntry
	done chan struct{}
	wg   sync.WaitGroup
}

// NewIOLogger starts the background writer. Returns nil when disabled.
func NewIOLogger(cfg IOLogConfig, db *sql.DB) *IOLogger {
	if !cfg.Enabled {
		return nil
	}
	if cfg.BufSize <= 0 {
		cfg.BufSize = 4000
	}
	if cfg.BodyMaxBytes <= 0 {
		cfg.BodyMaxBytes = 32 * 1024
	}
	l := &IOLogger{
		cfg:  cfg,
		db:   db,
		ch:   make(chan APILogEntry, cfg.BufSize),
		done: make(chan struct{}),
	}
	l.wg.Add(1)
	go l.writer()
	return l
}

// Log enqueues e for async writing. Never blocks; drops silently when full.
func (l *IOLogger) Log(e APILogEntry) {
	if l == nil {
		return
	}
	select {
	case l.ch <- e:
	default:
	}
}

// Close drains the queue and stops the writer.
func (l *IOLogger) Close() {
	if l == nil {
		return
	}
	close(l.done)
	l.wg.Wait()
}

func (l *IOLogger) truncate(s string) string {
	if len(s) <= l.cfg.BodyMaxBytes {
		return s
	}
	return s[:l.cfg.BodyMaxBytes] + "…[truncated]"
}

func (l *IOLogger) writer() {
	defer l.wg.Done()
	for {
		select {
		case e := <-l.ch:
			l.write(e)
		case <-l.done:
			for {
				select {
				case e := <-l.ch:
					l.write(e)
				default:
					return
				}
			}
		}
	}
}

func (l *IOLogger) write(e APILogEntry) {
	reqHeaders := jsonOrNil(e.ReqHeaders)
	respHeaders := jsonOrNil(e.RespHeaders)
	status := sql.NullInt32{}
	if e.Status != 0 {
		status = sql.NullInt32{Int32: int32(e.Status), Valid: true}
	}
	// app-level context for retention (created_at) defaults in the DB.
	_, _ = l.db.Exec(
		`INSERT INTO api_call_logs
		 (exchange_code, method, url, request_headers, request_body,
		  response_status, response_headers, response_body, latency_ms, error, timeout)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		nullStr(e.Exchange), nullStr(e.Method), nullStr(e.URL),
		reqHeaders, nullStr(l.truncate(e.ReqBody)),
		status, respHeaders, nullStr(l.truncate(e.RespBody)),
		nullInt64(e.LatencyMs), nullStr(e.Err), e.Timeout,
	)
}

// loggingTransport is an http.RoundTripper that masks and logs every request and
// response via an IOLogger. It is the single choke point that guarantees raw API
// logs are secret-masked.
type loggingTransport struct {
	base     http.RoundTripper
	exchange string
	logger   *IOLogger
}

// NewLoggingTransport wraps base so that every round-trip is masked and logged.
// If logger is nil it returns base unchanged.
func NewLoggingTransport(base http.RoundTripper, exchange string, logger *IOLogger) http.RoundTripper {
	if logger == nil {
		return base
	}
	if base == nil {
		base = http.DefaultTransport
	}
	return &loggingTransport{base: base, exchange: exchange, logger: logger}
}

func (t *loggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()

	timeout := false
	if dl, ok := req.Context().Deadline(); ok && time.Until(dl) <= 0 {
		timeout = true
	}

	var reqBody string
	if req.Body != nil && req.Method != http.MethodGet && req.Method != http.MethodHead {
		if b, err := io.ReadAll(req.Body); err == nil {
			reqBody = string(b)
			req.Body = io.NopCloser(bytes.NewReader(b))
		}
	}

	resp, err := t.base.RoundTrip(req)
	latency := time.Since(start).Milliseconds()

	entry := APILogEntry{
		Exchange:   t.exchange,
		Method:     req.Method,
		URL:        MaskURL(req.URL.String()), // mask signed query params
		ReqHeaders: MaskHeaders(req.Header),
		ReqBody:    MaskBody(reqBody),
		LatencyMs:  latency,
	}
	if err != nil {
		if req.Context().Err() != nil {
			timeout = true
		}
		entry.Err = err.Error()
		entry.Timeout = timeout
		t.logger.Log(entry)
		return nil, err
	}

	var respBody string
	if resp.Body != nil {
		if b, readErr := io.ReadAll(resp.Body); readErr == nil {
			respBody = string(b)
			resp.Body = io.NopCloser(bytes.NewReader(b))
		}
	}
	entry.Status = resp.StatusCode
	entry.RespHeaders = MaskHeaders(resp.Header)
	entry.RespBody = MaskBody(respBody)
	entry.Timeout = timeout
	t.logger.Log(entry)
	return resp, nil
}

func jsonOrNil(m map[string]string) any {
	if len(m) == 0 {
		return nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return string(b)
}

func nullStr(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func nullInt64(n int64) sql.NullInt64 {
	if n == 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: n, Valid: true}
}
