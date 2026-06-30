package exchanges

import (
	"database/sql/driver"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// noSecret is a sqlmock argument matcher that fails if a logged value contains
// any known secret — this is the rule-#6 guarantee at the DB-write boundary.
type noSecret struct{ field string }

func (n noSecret) Match(v driver.Value) bool {
	s := fmt.Sprintf("%v", v)
	for _, sec := range leakValues {
		if strings.Contains(s, sec) {
			return false
		}
	}
	return true
}

func TestLoggingTransportMasksBeforeDBWrite(t *testing.T) {
	// Server echoes a response body that itself contains a secret, to test
	// response masking too.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Set-Cookie", "sessioncookieval")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"token":"Bearer-token-abc","ok":true}`))
	}))
	defer srv.Close()

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()

	logger := NewIOLogger(IOLogConfig{Enabled: true, BufSize: 4}, mockDB)

	cfg := ClientConfig{Code: "testex", HTTPClient: srv.Client()}
	client, err := BuildHTTPClient(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}

	// url, request_headers, request_body, response_headers, response_body must be
	// secret-free; the rest are unconstrained.
	mock.ExpectExec("INSERT INTO api_call_logs").
		WithArgs(
			sqlmock.AnyArg(),     // exchange_code
			sqlmock.AnyArg(),     // method
			noSecret{"url"},      // url
			noSecret{"req_hdrs"}, // request_headers
			noSecret{"req_body"}, // request_body
			sqlmock.AnyArg(),     // response_status
			noSecret{"res_hdrs"}, // response_headers
			noSecret{"res_body"}, // response_body
			sqlmock.AnyArg(),     // latency_ms
			sqlmock.AnyArg(),     // error
			sqlmock.AnyArg(),     // timeout
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	body := strings.NewReader(`{"apiSecret":"supersecretvalue","passphrase":"passphrase-xyz","symbol":"BTCUSDT"}`)
	req, err := http.NewRequest(http.MethodPost,
		srv.URL+"/order?signature=deadbeefsignature&apiKey=AKIAEXAMPLEKEY123&symbol=BTCUSDT", body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer-token-abc")
	req.Header.Set("X-API-Key", "AKIAEXAMPLEKEY123")
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	logger.Close() // drain the async writer

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("masking violated or write missing: %v", err)
	}
}

// TestLoggingTransportMasksBitpinResponseBody (PR4 correction) — a Bitpin-like auth response
// {"access":...,"refresh":...} flowing through the IO logger must NOT land in
// api_call_logs.response_body in the clear.
func TestLoggingTransportMasksBitpinResponseBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"access":"ACCESS-TOKEN-1","refresh":"REFRESH-TOKEN-1"}`))
	}))
	defer srv.Close()

	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	logger := NewIOLogger(IOLogConfig{Enabled: true, BufSize: 4}, mockDB)
	client, err := BuildHTTPClient(ClientConfig{Code: "bitpin", HTTPClient: srv.Client()}, logger)
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectExec("INSERT INTO api_call_logs").
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), noSecret{"url"},
			noSecret{"req_hdrs"}, noSecret{"req_body"}, sqlmock.AnyArg(),
			noSecret{"res_hdrs"}, noSecret{"res_body"}, sqlmock.AnyArg(),
			noSecret{"error"}, sqlmock.AnyArg(),
		).WillReturnResult(sqlmock.NewResult(1, 1))

	resp, err := client.Get(srv.URL + "/usr/api/v1/usr/authenticate/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	logger.Close()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("bitpin access/refresh token masking violated: %v", err)
	}
}

// erroringRoundTripper always fails with a fixed error — used to drive the IO logger's
// error-logging path with an error string that embeds secrets.
type erroringRoundTripper struct{ err error }

func (e erroringRoundTripper) RoundTrip(*http.Request) (*http.Response, error) { return nil, e.err }

// TestLoggingTransportMasksErrorText (PR4 correction) — when the base transport returns an
// error whose text contains secrets, api_call_logs.error must be masked.
func TestLoggingTransportMasksErrorText(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer mockDB.Close()
	logger := NewIOLogger(IOLogConfig{Enabled: true, BufSize: 4}, mockDB)

	secretErr := fmt.Errorf("token=SECRET-TOKEN&apiKey=SECRET-KEY&signature=SECRET-SIGNATURE: connection refused")
	rt := NewLoggingTransport(erroringRoundTripper{err: secretErr}, "testex", logger)

	mock.ExpectExec("INSERT INTO api_call_logs").
		WithArgs(
			sqlmock.AnyArg(), sqlmock.AnyArg(), noSecret{"url"},
			noSecret{"req_hdrs"}, noSecret{"req_body"}, sqlmock.AnyArg(),
			noSecret{"res_hdrs"}, noSecret{"res_body"}, sqlmock.AnyArg(),
			noSecret{"error"}, sqlmock.AnyArg(),
		).WillReturnResult(sqlmock.NewResult(1, 1))

	req, _ := http.NewRequest(http.MethodGet, "https://api.x.io/order", nil)
	if _, rerr := rt.RoundTrip(req); rerr == nil {
		t.Fatal("expected the round trip to surface the base error")
	}
	logger.Close()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("error-text masking violated: %v", err)
	}
}

func TestIOLoggerNilSafe(t *testing.T) {
	var l *IOLogger // disabled
	l.Log(APILogEntry{Exchange: "x"})
	l.Close()
	// NewIOLogger returns nil when disabled.
	if NewIOLogger(IOLogConfig{Enabled: false}, nil) != nil {
		t.Fatal("disabled IOLogger should be nil")
	}
}

func TestLoggingTransportPassThroughWhenNoLogger(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	defer srv.Close()

	cfg := ClientConfig{Code: "testex", HTTPClient: srv.Client(), ClientTimeout: 2 * time.Second}
	client, err := BuildHTTPClient(cfg, nil) // no logger
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}
