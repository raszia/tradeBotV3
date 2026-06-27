package exchanges

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/http2"
)

// Tuned HTTP transport defaults, ported from the sibling system. They differ
// from Go's zero-value transport (which leaves several timeouts unbounded and
// caps idle conns per host at 2) and are sized for a few hosts hit repeatedly
// over a sometimes-flaky cross-border path.
const (
	httpDialTimeout           = 5 * time.Second
	httpDialKeepAlive         = 30 * time.Second
	httpTLSHandshakeTimeout   = 5 * time.Second
	httpResponseHeaderTimeout = 7 * time.Second
	httpIdleConnTimeout       = 90 * time.Second
	httpMaxIdleConnsPerHost   = 16
	httpMaxConnsPerHost       = 64
	httpExpectContinueTimeout = 1 * time.Second
	h2ReadIdleTimeout         = 15 * time.Second
	h2PingTimeout             = 5 * time.Second

	defaultClientTimeout  = 30 * time.Second
	defaultRequestTimeout = 3 * time.Second
)

// BuildHTTPClient builds an *http.Client for cfg, wrapping the transport with the
// masking IO logger so every request/response is logged with secrets masked. If
// cfg.HTTPClient is set (tests pointing at a fake server) it is returned as-is
// (still wrapped with the logger if one is provided).
func BuildHTTPClient(cfg ClientConfig, logger *IOLogger) (*http.Client, error) {
	if cfg.HTTPClient != nil {
		client := *cfg.HTTPClient // shallow copy so we can wrap the transport
		base := client.Transport
		if base == nil {
			base = http.DefaultTransport
		}
		client.Transport = NewLoggingTransport(base, cfg.Code, logger)
		return &client, nil
	}

	dialer := &net.Dialer{Timeout: httpDialTimeout, KeepAlive: httpDialKeepAlive}
	if cfg.LocalIP != "" {
		ip := net.ParseIP(cfg.LocalIP)
		if ip == nil {
			return nil, fmt.Errorf("exchange %s: invalid local IP %q", cfg.Code, cfg.LocalIP)
		}
		dialer.LocalAddr = &net.TCPAddr{IP: ip}
	}
	transport := &http.Transport{
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   httpMaxIdleConnsPerHost,
		MaxConnsPerHost:       httpMaxConnsPerHost,
		IdleConnTimeout:       httpIdleConnTimeout,
		TLSHandshakeTimeout:   httpTLSHandshakeTimeout,
		ResponseHeaderTimeout: httpResponseHeaderTimeout,
		ExpectContinueTimeout: httpExpectContinueTimeout,
	}
	if cfg.ProxyURL != "" {
		proxyURL, err := url.Parse(cfg.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("exchange %s: invalid proxy_url %q: %w", cfg.Code, cfg.ProxyURL, err)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
	}
	// HTTP/2 PING-based keepalive: detects silently-dropped idle connections.
	// Non-fatal on failure (HTTP/2 still works via ALPN, just without PINGs).
	if h2t, err := http2.ConfigureTransports(transport); err == nil && h2t != nil {
		h2t.ReadIdleTimeout = h2ReadIdleTimeout
		h2t.PingTimeout = h2PingTimeout
	}

	timeout := cfg.ClientTimeout
	if timeout == 0 {
		timeout = defaultClientTimeout
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: NewLoggingTransport(transport, cfg.Code, logger),
	}, nil
}

// RequestContext returns a child context bounded by cfg.RequestTimeout (default
// 3s). Callers must defer the returned cancel.
func RequestContext(parent context.Context, cfg ClientConfig) (context.Context, context.CancelFunc) {
	d := cfg.RequestTimeout
	if d == 0 {
		d = defaultRequestTimeout
	}
	return context.WithTimeout(parent, d)
}
