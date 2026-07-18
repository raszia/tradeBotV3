package exchanges

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"v3TradeBot/internal/execution"
)

// rotatingCreds is a CredentialProvider whose returned credential can change at runtime, simulating
// a dashboard rotation while a long-lived client keeps running.
type rotatingCreds struct {
	mu sync.Mutex
	c  Credentials
}

func (r *rotatingCreds) Credentials(context.Context, string) (Credentials, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.c, nil
}
func (r *rotatingCreds) set(c Credentials) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.c = c
}

// TestBitpinTokenCacheInvalidatedOnCredentialRotation (PR22 round-2 blocker 3): a long-lived Bitpin
// client authenticates with credential A and caches its token; after the active credential is
// rotated to B, the NEXT private request must NOT reuse A's cached token — it re-authenticates with
// B. The auth server records which api_key it saw.
func TestBitpinTokenCacheInvalidatedOnCredentialRotation(t *testing.T) {
	var lastAuthKey atomic.Value // string
	lastAuthKey.Store("")
	var authCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/usr/authenticate/":
			atomic.AddInt32(&authCount, 1)
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			lastAuthKey.Store(body["api_key"])
			// Token echoes the key so we can tell which credential minted it.
			_, _ = w.Write([]byte(`{"access":"TOK-` + body["api_key"] + `","refresh":"R-` + body["api_key"] + `"}`))
		case r.URL.Path == "/api/v1/wlt/wallets/":
			_, _ = w.Write([]byte(`{"results":[]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	rc := &rotatingCreds{c: Credentials{APIKey: "KEY-A", APISecret: "SEC-A"}}
	priv, err := newBitpinPrivate(ClientConfig{Code: bitpinCode, BaseURL: srv.URL, HTTPClient: srv.Client(), Creds: rc}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// First private request authenticates with credential A and caches TOK-KEY-A.
	if _, err := priv.GetBalances(context.Background()); err != nil {
		t.Fatalf("first GetBalances: %v", err)
	}
	if lastAuthKey.Load().(string) != "KEY-A" {
		t.Fatalf("first auth used %q, want KEY-A", lastAuthKey.Load())
	}
	firstAuths := atomic.LoadInt32(&authCount)

	// Rotate the active credential to B.
	rc.set(Credentials{APIKey: "KEY-B", APISecret: "SEC-B"})

	// The next request must NOT reuse A's cached token — it re-authenticates with B.
	if _, err := priv.GetBalances(context.Background()); err != nil {
		t.Fatalf("second GetBalances: %v", err)
	}
	if k := lastAuthKey.Load().(string); k != "KEY-B" {
		t.Errorf("after rotation the client authenticated with %q, want KEY-B (cached A token must be dropped)", k)
	}
	if atomic.LoadInt32(&authCount) <= firstAuths {
		t.Error("rotation must force a fresh authentication (no auth call happened after rotation)")
	}

	// A third request with the SAME (B) credential reuses the cached token (no extra auth).
	beforeThird := atomic.LoadInt32(&authCount)
	if _, err := priv.GetBalances(context.Background()); err != nil {
		t.Fatalf("third GetBalances: %v", err)
	}
	if atomic.LoadInt32(&authCount) != beforeThird {
		t.Error("an unchanged credential must reuse the cached token (no re-auth)")
	}
}

// bitpinRotationServer serves auth (token echoes the api_key) + a counted order/cancel endpoint.
func bitpinRotationServer(t *testing.T) (*httptest.Server, *int32, *int32) {
	t.Helper()
	var authHits, orderHits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/usr/authenticate/" || r.URL.Path == "/api/v1/usr/refresh_token/":
			atomic.AddInt32(&authHits, 1)
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			_, _ = w.Write([]byte(`{"access":"TOK-` + body["api_key"] + `","refresh":"R-` + body["api_key"] + `"}`))
		case strings.HasPrefix(r.URL.Path, "/api/v1/odr/orders/"):
			atomic.AddInt32(&orderHits, 1)
			_, _ = w.Write([]byte(`{"id":"991","identifier":"c1","state":"active","type":"limit","side":"buy","symbol":"BTC_IRT","price":"1201000","base_amount":"0.01","remain_amount":"0.01","dealed_base_amount":"0"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &authHits, &orderHits
}

// TestBitpinPreparedPlaceRefusedAfterRotation (round-3 blocker 2B): a place prepared with credential
// A must NOT be sent after a rotation to B — Send returns NotSent BEFORE the order endpoint is hit,
// and re-preparing authenticates with B.
func TestBitpinPreparedPlaceRefusedAfterRotation(t *testing.T) {
	srv, _, orderHits := bitpinRotationServer(t)
	rc := &rotatingCreds{c: Credentials{APIKey: "KEY-A", APISecret: "SEC-A"}}
	priv, err := newBitpinPrivate(ClientConfig{Code: bitpinCode, BaseURL: srv.URL, HTTPClient: srv.Client(), Creds: rc}, nil)
	if err != nil {
		t.Fatal(err)
	}
	pm := priv.(MutationPreparer)
	prepared, err := pm.PreparePlace(context.Background(), twoStagePlaceReq()) // authenticates with A
	if err != nil {
		t.Fatalf("PreparePlace: %v", err)
	}
	rc.set(Credentials{APIKey: "KEY-B", APISecret: "SEC-B"}) // rotate before sending

	_, serr := prepared.Send(context.Background())
	if serr == nil || !execution.IsNotSent(serr) {
		t.Fatalf("Send after rotation err = %v, want a NotSent error", serr)
	}
	if n := atomic.LoadInt32(orderHits); n != 0 {
		t.Errorf("order endpoint called %d times after a rotation, want 0 (not sent)", n)
	}
	// Re-preparing now uses credential B.
	if _, err := pm.PreparePlace(context.Background(), twoStagePlaceReq()); err != nil {
		t.Fatalf("re-PreparePlace with the new credential: %v", err)
	}
	if fp := priv.(*bitpinPrivate).tokenFP(); fp != credFingerprint(Credentials{APIKey: "KEY-B", APISecret: "SEC-B"}) {
		t.Error("after re-preparation the client's token must be bound to credential B")
	}
}

// TestBitpinPreparedCancelRefusedAfterRotation (round-3 blocker 2B): same for a prepared cancel.
func TestBitpinPreparedCancelRefusedAfterRotation(t *testing.T) {
	srv, _, orderHits := bitpinRotationServer(t)
	rc := &rotatingCreds{c: Credentials{APIKey: "KEY-A", APISecret: "SEC-A"}}
	priv, err := newBitpinPrivate(ClientConfig{Code: bitpinCode, BaseURL: srv.URL, HTTPClient: srv.Client(), Creds: rc}, nil)
	if err != nil {
		t.Fatal(err)
	}
	pm := priv.(MutationPreparer)
	prepared, err := pm.PrepareCancel(context.Background(), "EXT-1") // authenticates with A
	if err != nil {
		t.Fatalf("PrepareCancel: %v", err)
	}
	rc.set(Credentials{APIKey: "KEY-B", APISecret: "SEC-B"})

	_, serr := prepared.Send(context.Background())
	if serr == nil || !execution.IsNotSent(serr) {
		t.Fatalf("cancel Send after rotation err = %v, want NotSent", serr)
	}
	if n := atomic.LoadInt32(orderHits); n != 0 {
		t.Errorf("cancel endpoint called %d times after a rotation, want 0", n)
	}
}

// TestBitpinAuthResponseRejectedAfterRotation (round-3 blocker 2A): if the active credential is
// rotated while an authentication for the OLD credential is in flight, the returned token must NOT
// be cached; the next request re-authenticates with the new credential.
func TestBitpinAuthResponseRejectedAfterRotation(t *testing.T) {
	release := make(chan struct{})
	firstAuthSeen := make(chan struct{}, 1)
	var authKeys struct {
		mu sync.Mutex
		s  []string
	}
	var walletAuthHeaders struct {
		mu sync.Mutex
		s  []string
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/usr/authenticate/":
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			key := body["api_key"]
			authKeys.mu.Lock()
			authKeys.s = append(authKeys.s, key)
			first := len(authKeys.s) == 1
			authKeys.mu.Unlock()
			if first { // pause the FIRST authentication (credential A) mid-flight
				select {
				case firstAuthSeen <- struct{}{}:
				default:
				}
				<-release
			}
			_, _ = w.Write([]byte(`{"access":"TOK-` + key + `","refresh":"R-` + key + `"}`))
		case r.URL.Path == "/api/v1/wlt/wallets/":
			walletAuthHeaders.mu.Lock()
			walletAuthHeaders.s = append(walletAuthHeaders.s, r.Header.Get("Authorization"))
			walletAuthHeaders.mu.Unlock()
			_, _ = w.Write([]byte(`{"results":[]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	rc := &rotatingCreds{c: Credentials{APIKey: "KEY-A", APISecret: "SEC-A"}}
	priv, err := newBitpinPrivate(ClientConfig{Code: bitpinCode, BaseURL: srv.URL, HTTPClient: srv.Client(), Creds: rc}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// First GetBalances authenticates with A and blocks in the paused auth response.
	firstDone := make(chan error, 1)
	go func() { _, e := priv.GetBalances(context.Background()); firstDone <- e }()
	<-firstAuthSeen
	rc.set(Credentials{APIKey: "KEY-B", APISecret: "SEC-B"}) // rotate while A's auth is paused
	close(release)                                           // let A's auth response return
	<-firstDone                                              // the first call fails (NotSent) — token A not cached

	// The next request must authenticate with B (A's token was never cached).
	if _, err := priv.GetBalances(context.Background()); err != nil {
		t.Fatalf("second GetBalances: %v", err)
	}
	authKeys.mu.Lock()
	last := authKeys.s[len(authKeys.s)-1]
	authKeys.mu.Unlock()
	if last != "KEY-B" {
		t.Errorf("after rotation the client authenticated with %q, want KEY-B", last)
	}
	walletAuthHeaders.mu.Lock()
	defer walletAuthHeaders.mu.Unlock()
	for _, h := range walletAuthHeaders.s {
		if strings.Contains(h, "TOK-KEY-A") {
			t.Error("a request used credential A's token after rotation — the stale auth response was cached")
		}
	}
}
