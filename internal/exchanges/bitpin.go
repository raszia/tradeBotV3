package exchanges

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"v3TradeBot/internal/domain"
	"v3TradeBot/internal/execution"
)

// Bitpin adapter — public market data + private trading. Ported/adapted from the
// sibling iranArb bitpin package. Bitpin is an Iranian exchange that quotes in
// toman (IRT); this layer treats IRT as the canonical quote unit (priceMultiplier
// = 1, no rial→toman rescale needed because the REST orderbook is already toman).
//
// Capabilities: GetMarkets (mkt/markets), GetOrderBook (REST mth/orderbook),
// GetBalances, PlaceOrder, CancelOrder, GetOrder, GetOpenOrders. Order status is
// POLLING-ONLY here — Bitpin's order push feed is a Centrifugo WebSocket that is
// deferred (OrderBookWS / OrderUpdatesWS both false; OrderStatusPoll true).
//
// AUTH — JWT access/refresh token flow:
//   - There is no per-request HMAC signature. Instead the adapter exchanges the
//     api_key/secret_key pair for a short-lived JWT access token via
//     POST /api/v1/usr/authenticate/ ({"api_key","secret_key"} -> {"access","refresh"}).
//   - Every authenticated request sends "Authorization: Bearer <access>".
//   - The access token lives ~15 minutes on Bitpin's side; we cache it for 14
//     minutes (bitpinTokenTTL) to keep a 1-minute safety margin, then refresh.
//     Refresh prefers POST /api/v1/usr/refresh_token/ ({"refresh"} -> {"access"})
//     when we hold a refresh token, otherwise re-authenticates from scratch.
//
// Known limitations / quirks:
//   - RATE-LIMIT SENSITIVE. Bitpin aggregates auth + order + wallet calls under a
//     tight RPM budget and returns HTTP 429 ("Request was throttled. Expected
//     available in N seconds.") aggressively — including on the auth endpoints. We
//     honor that: a 429 on the token endpoint arms bitpinAuthThrottledUntil and
//     subsequent callers reuse the cached (slightly-stale, still-valid) token
//     rather than hammering auth. Callers should not place orders in tight loops.
//   - Symbol form is UNDERSCORE: canonical "BTC/IRT" -> venue "BTC_IRT".
//   - Status is POLLING-ONLY (no order WebSocket here): SubscribeOrderUpdates
//     returns ErrUnsupported; use GetOrder / GetOpenOrders.
//   - ClientOrderID: Bitpin accepts an "identifier" field on placement; we pass
//     req.ClientOrderID as the identifier (ClientOrderID capability = true). Cancel
//     and GetOrder accept EITHER a numeric exchange order id OR the identifier
//     string (different URL forms — see CancelOrder/GetOrder).
//   - Error format: non-2xx responses carry a JSON body (often {"detail":"..."}).
//     We surface non-2xx as *NormalizedAPIError + classifyHTTPStatus and wrap the
//     execution sentinels where classifiable (auth/rate-limit/not-found).
//   - NO forced IOC/market/post-only: PlaceOrder passes req.OrderType straight
//     through as the Bitpin "type" and never injects a time-in-force.

const (
	bitpinCode        = "bitpin"
	bitpinDefaultREST = "https://api.bitpin.ir"

	// bitpinTokenTTL is how long a cached access token is treated as fresh
	// before a refresh. Bitpin tokens last ~15m; 14m keeps a 1m margin.
	bitpinTokenTTL = 14 * time.Minute

	// bitpinDefaultThrottle is the fallback backoff when a 429 from the auth
	// endpoint carries no parseable Retry-After / body hint.
	bitpinDefaultThrottle = 30 * time.Second
)

func init() {
	Register(Registration{
		Code: bitpinCode,
		Capabilities: Capabilities{
			MarketMetadata:        true,
			OrderBookREST:         true,
			OrderBookWS:           false, // Centrifugo WS deferred
			BalanceFetch:          true,
			PlaceOrder:            true,
			CancelByOrderID:       true,
			FetchByOrderID:        true,
			FetchOpenOrders:       true,
			RecentFills:           false, // GetRecentFills not implemented in this adapter
			OrderUpdatesWS:        false, // order push feed deferred
			OrderStatusPoll:       true,
			ClientOrderID:         true, // Bitpin accepts an "identifier" field on placement
			LookupByClientOrderID: true, // GET /odr/orders/?identifier=<id> resolves by client id
		},
		NewPublic:  newBitpinPublic,
		NewPrivate: newBitpinPrivate,
	})
}

// bitpinCapabilities returns the registered capability matrix.
func bitpinCapabilities() Capabilities {
	r, _ := Lookup(bitpinCode)
	return r.Capabilities
}

// ─── Public client ─────────────────────────────────────────────────────────

type bitpinPublic struct {
	cfg     ClientConfig
	http    *http.Client
	restURL string
}

func newBitpinPublic(cfg ClientConfig, logger *IOLogger) (PublicClient, error) {
	cfg.Code = bitpinCode
	hc, err := BuildHTTPClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	restURL := cfg.BaseURL
	if restURL == "" {
		restURL = bitpinDefaultREST
	}
	return &bitpinPublic{cfg: cfg, http: hc, restURL: strings.TrimRight(restURL, "/")}, nil
}

func (b *bitpinPublic) Name() string               { return bitpinCode }
func (b *bitpinPublic) Capabilities() Capabilities { return bitpinCapabilities() }

// --- GetMarkets ---

// bitpinMarketSpec mirrors one entry of /api/v1/mkt/markets/. Price/qty precision
// give us tick/step (10^-precision); min notional/qty are not exposed.
type bitpinMarketSpec struct {
	Symbol              string `json:"symbol"` // e.g. "BTC_IRT"
	Code                string `json:"code"`
	Base                string `json:"base"`
	Quote               string `json:"quote"`
	Tradable            bool   `json:"tradable"`
	Suspended           bool   `json:"suspended"`
	PricePrecision      int32  `json:"price_precision"`
	BaseAmountPrecision int32  `json:"base_amount_precision"`
}

func (b *bitpinPublic) GetMarkets(ctx context.Context) ([]NormalizedMarket, error) {
	ctx, cancel := RequestContext(ctx, b.cfg)
	defer cancel()

	raw, err := bitpinGet(ctx, b.http, b.restURL, "/api/v1/mkt/markets/")
	if err != nil {
		return nil, err
	}
	var markets []bitpinMarketSpec
	if err := json.Unmarshal(raw, &markets); err != nil {
		return nil, fmt.Errorf("bitpin markets decode: %w", err)
	}

	out := make([]NormalizedMarket, 0, len(markets))
	for _, m := range markets {
		base := strings.ToUpper(strings.TrimSpace(m.Base))
		quote := strings.ToUpper(strings.TrimSpace(m.Quote))
		venueSym := firstNonEmptyBitpin(m.Symbol, m.Code)
		if (base == "" || quote == "") && venueSym != "" {
			if bb, qq, ok := strings.Cut(venueSym, "_"); ok {
				base, quote = strings.ToUpper(bb), strings.ToUpper(qq)
			}
		}
		if base == "" || quote == "" {
			continue
		}
		// Canonical normalizes IRR->IRT; Bitpin quotes toman so IRT stays IRT.
		canonical := base + "/" + quote
		if norm, err := domain.NormalizeSymbol(canonical); err == nil {
			canonical = norm
		}
		m2 := NormalizedMarket{
			Exchange:        bitpinCode,
			ExchangeSymbol:  strings.ToUpper(firstNonEmptyBitpin(venueSym, base+"_"+quote)),
			CanonicalSymbol: canonical,
			BaseAsset:       base,
			QuoteAsset:      quote,
			QuoteAssetType:  domain.QuoteAssetType(canonical),
			Tradable:        m.Tradable && !m.Suspended,
		}
		if m.PricePrecision >= 0 {
			m2.PricePrecision = int(m.PricePrecision)
			m2.TickSize = decimal.New(1, -m.PricePrecision) // 10^-price_precision
		}
		if m.BaseAmountPrecision >= 0 {
			m2.QuantityPrecision = int(m.BaseAmountPrecision)
			m2.StepSize = decimal.New(1, -m.BaseAmountPrecision) // 10^-base_amount_precision
		}
		out = append(out, m2)
	}
	return out, nil
}

// --- GetOrderBook (REST) ---

func (b *bitpinPublic) GetOrderBook(ctx context.Context, symbol string) (domain.OrderBook, error) {
	venueSym, ok := VenueSymbol(b.cfg, symbol)
	if !ok {
		venueSym = bitpinNativeSymbol(symbol)
	} else {
		venueSym = bitpinNativeSymbol(venueSym)
	}
	canonical, _ := domain.NormalizeSymbol(symbol)
	if canonical == "" {
		canonical = strings.ToUpper(symbol)
	}

	ctx, cancel := RequestContext(ctx, b.cfg)
	defer cancel()
	raw, err := bitpinGet(ctx, b.http, b.restURL, "/api/v1/mth/orderbook/"+url.PathEscape(venueSym)+"/")
	if err != nil {
		return domain.OrderBook{}, err
	}
	var d struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return domain.OrderBook{}, fmt.Errorf("bitpin orderbook decode: %w", err)
	}
	// Bitpin quotes in toman (IRT) already — priceMultiplier = 1, no rescale.
	book := BuildOrderBook(bitpinCode, canonical,
		bitpinLevelsFromPairs(d.Bids), bitpinLevelsFromPairs(d.Asks),
		decimal.NewFromInt(1), "rest", time.Now())
	return book, nil
}

func (b *bitpinPublic) SubscribeOrderBook(ctx context.Context, symbols []string) (<-chan domain.OrderBook, error) {
	// Bitpin's order book arrives over a Centrifugo WebSocket; that transport is
	// deferred (OrderBookWS = false), so this is unsupported here.
	return nil, Unsupported(bitpinCode, "order-book WebSocket")
}

// ─── Private client ────────────────────────────────────────────────────────

type bitpinPrivate struct {
	cfg     ClientConfig
	http    *http.Client
	restURL string

	tokenMu            sync.Mutex
	accessToken        string
	refreshToken       string
	tokenAt            time.Time
	authThrottledUntil time.Time
}

func newBitpinPrivate(cfg ClientConfig, logger *IOLogger) (PrivateClient, error) {
	cfg.Code = bitpinCode
	hc, err := BuildHTTPClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	restURL := cfg.BaseURL
	if restURL == "" {
		restURL = bitpinDefaultREST
	}
	return &bitpinPrivate{cfg: cfg, http: hc, restURL: strings.TrimRight(restURL, "/")}, nil
}

func (b *bitpinPrivate) Name() string               { return bitpinCode }
func (b *bitpinPrivate) Capabilities() Capabilities { return bitpinCapabilities() }

// --- auth / JWT token flow ---

// bitpinAuthThrottleRe extracts N from Bitpin's standard 429 body shape:
// {"detail":"Request was throttled. Expected available in 29 seconds."}.
var bitpinAuthThrottleRe = regexp.MustCompile(`available in (\d+) second`)

// bearerToken returns a valid JWT access token, acquiring or refreshing it as
// needed. Faithful port of iranArb's bitpin token flow: cache for bitpinTokenTTL,
// prefer the cached token while a 429 backoff is active, refresh via
// /usr/refresh_token/ when a refresh token is held, else /usr/authenticate/.
func (b *bitpinPrivate) bearerToken(ctx context.Context) (string, error) {
	b.tokenMu.Lock()
	// Fast path: still-fresh cached token.
	if b.accessToken != "" && time.Since(b.tokenAt) < bitpinTokenTTL {
		tok := b.accessToken
		b.tokenMu.Unlock()
		return tok, nil
	}
	// Cached token past our internal TTL. Honor an active 429 backoff on the
	// auth endpoint: prefer the (slightly-stale, ~1m grace) cached token over an
	// auth attempt we know would 429 again.
	inBackoff := !b.authThrottledUntil.IsZero() && time.Now().Before(b.authThrottledUntil)
	if inBackoff && b.accessToken != "" {
		tok := b.accessToken
		b.tokenMu.Unlock()
		return tok, nil
	}
	if inBackoff {
		wait := time.Until(b.authThrottledUntil)
		b.tokenMu.Unlock()
		return "", b.authError(fmt.Sprintf("auth throttled, retry after %s", wait), http.StatusTooManyRequests)
	}
	refresh := b.refreshToken
	b.tokenMu.Unlock()

	creds, err := b.cfg.Creds.Credentials(ctx, bitpinCode)
	if err != nil {
		return "", err
	}
	if creds.APIKey == "" || creds.APISecret == "" {
		return "", b.authError("missing api credentials", http.StatusUnauthorized)
	}

	path := "/api/v1/usr/authenticate/"
	body, _ := json.Marshal(map[string]string{"api_key": creds.APIKey, "secret_key": creds.APISecret})
	if refresh != "" {
		path = "/api/v1/usr/refresh_token/"
		body, _ = json.Marshal(map[string]string{"refresh": refresh})
	}

	reqCtx, cancel := RequestContext(ctx, b.cfg)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, b.restURL+path, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("bitpin token %s: %w", path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusTooManyRequests {
		// Arm the throttle window so subsequent callers reuse the cached token
		// rather than re-hitting auth. Prefer Retry-After, then the body hint.
		wait := parseBitpinAuthThrottle(resp.Header.Get("Retry-After"), raw)
		if wait <= 0 {
			wait = bitpinDefaultThrottle
		}
		b.tokenMu.Lock()
		b.authThrottledUntil = time.Now().Add(wait)
		tok := b.accessToken
		b.tokenMu.Unlock()
		if tok != "" {
			return tok, nil
		}
		return "", b.apiError(path, resp.StatusCode, raw)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", b.apiError(path, resp.StatusCode, raw)
	}
	var payload struct {
		Access  string `json:"access"`
		Refresh string `json:"refresh"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return "", fmt.Errorf("bitpin token decode: %w", err)
	}
	if payload.Access == "" {
		return "", b.authError("token response missing access token", http.StatusUnauthorized)
	}

	b.tokenMu.Lock()
	b.accessToken = payload.Access
	if payload.Refresh != "" {
		b.refreshToken = payload.Refresh
	}
	b.tokenAt = time.Now()
	b.authThrottledUntil = time.Time{}
	b.tokenMu.Unlock()

	return payload.Access, nil
}

// doJSON performs an authenticated Bitpin request: acquires a bearer token, sends
// the JSON body (if any), and returns the raw response, decoding into out when
// provided. Non-2xx becomes a *NormalizedAPIError.
func (b *bitpinPrivate) doJSON(ctx context.Context, op, method, path string, in, out any) ([]byte, error) {
	tok, err := b.bearerToken(ctx)
	if err != nil {
		return nil, err
	}
	var body io.Reader
	if in != nil {
		buf, err := json.Marshal(in)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(buf)
	}
	reqCtx, cancel := RequestContext(ctx, b.cfg)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, method, b.restURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("bitpin %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return raw, b.apiError(op, resp.StatusCode, raw)
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return raw, fmt.Errorf("bitpin %s decode: %w", op, err)
		}
	}
	return raw, nil
}

// --- GetBalances ---

type bitpinWalletItem struct {
	Asset            string          `json:"asset"`
	Currency         string          `json:"currency"`
	Code             string          `json:"code"`
	Service          string          `json:"service"`
	Balance          json.RawMessage `json:"balance"`
	Available        json.RawMessage `json:"available"`
	AvailableBalance json.RawMessage `json:"available_balance"`
	Frozen           json.RawMessage `json:"frozen"`
	Blocked          json.RawMessage `json:"blocked"`
}

func (b *bitpinPrivate) GetBalances(ctx context.Context) ([]domain.Balance, error) {
	raw, err := b.doJSON(ctx, "balances", http.MethodGet, "/api/v1/wlt/wallets/?limit=300", nil, nil)
	if err != nil {
		return nil, err
	}
	wallets, err := parseBitpinWallets(raw)
	if err != nil {
		return nil, fmt.Errorf("bitpin wallets decode: %w", err)
	}
	now := time.Now().UTC()
	out := make([]domain.Balance, 0, len(wallets))
	for _, w := range wallets {
		if w.Service != "" && w.Service != "main" && w.Service != "spot" {
			continue
		}
		asset := strings.ToUpper(strings.TrimSpace(firstNonEmptyBitpin(w.Asset, w.Currency, w.Code)))
		if asset == "" {
			continue
		}
		switch asset {
		case "RIAL", "IRR", "RLS":
			asset = domain.CurrencyIRT
		}
		available := bitpinDecRaw(w.Available)
		if available.IsZero() {
			available = bitpinDecRaw(w.AvailableBalance)
		}
		if available.IsZero() {
			available = bitpinDecRaw(w.Balance)
		}
		locked := bitpinDecRaw(w.Frozen)
		if locked.IsZero() {
			locked = bitpinDecRaw(w.Blocked)
		}
		total := bitpinDecRaw(w.Balance)
		if total.IsZero() {
			total = available.Add(locked)
		}
		out = append(out, domain.Balance{
			Exchange:  bitpinCode,
			Asset:     asset,
			Available: available,
			Locked:    locked,
			Total:     total,
			UpdatedAt: now,
		})
	}
	return out, nil
}

// --- PlaceOrder ---

type bitpinPlaceOrderRequest struct {
	Symbol      string `json:"symbol"`
	Type        string `json:"type"`
	Side        string `json:"side"`
	BaseAmount  string `json:"base_amount"`
	Price       string `json:"price,omitempty"`
	Identifier  string `json:"identifier,omitempty"`
	TimeInForce string `json:"time_in_force,omitempty"`
}

func (b *bitpinPrivate) PlaceOrder(ctx context.Context, req execution.OrderRequest) (execution.OrderAck, error) {
	venueSym := bitpinNativeSymbol(req.Symbol)
	if v, ok := VenueSymbol(b.cfg, req.Symbol); ok {
		venueSym = bitpinNativeSymbol(v)
	}
	// Order type passes straight through (owner-defined). Default to limit only
	// when the caller left it empty; never force IOC/market/post-only.
	orderType := strings.ToLower(strings.TrimSpace(req.OrderType))
	if orderType == "" {
		orderType = "limit"
	}
	payload := bitpinPlaceOrderRequest{
		Symbol:     venueSym,
		Type:       orderType,
		Side:       strings.ToLower(req.Side),
		BaseAmount: req.Quantity.String(),
		Identifier: req.ClientOrderID, // Bitpin "identifier" = our client order id
	}
	if req.LimitPrice.IsPositive() {
		payload.Price = req.LimitPrice.String()
	}
	// Time-in-force is owner-set; pass it through only when supplied.
	if tif := strings.TrimSpace(req.TimeInForce); tif != "" {
		payload.TimeInForce = tif
	}

	var order bitpinOrder
	raw, err := b.doJSON(ctx, "place", http.MethodPost, "/api/v1/odr/orders/", payload, &order)
	if err != nil {
		return execution.OrderAck{}, err
	}
	// Some responses wrap the order under {"order": {...}}.
	if len(order.ID) == 0 && order.Identifier == "" {
		var wrapped struct {
			Order bitpinOrder     `json:"order"`
			ID    json.RawMessage `json:"id"`
		}
		if jerr := json.Unmarshal(raw, &wrapped); jerr == nil && (len(wrapped.Order.ID) > 0 || wrapped.Order.Identifier != "") {
			order = wrapped.Order
			if len(order.ID) == 0 {
				order.ID = wrapped.ID
			}
		}
	}
	if order.Identifier == "" {
		order.Identifier = req.ClientOrderID
	}
	if order.State == "" {
		order.State = "active"
	}

	status := bitpinOrderToStatus(order, req.Symbol, strings.ToLower(req.Side))
	ack := execution.OrderAck{
		ExchangeOrderID: status.ExchangeOrderID,
		ClientOrderID:   firstNonEmptyBitpin(status.ClientOrderID, req.ClientOrderID),
		Symbol:          req.Symbol,
		Side:            strings.ToLower(req.Side),
		RequestedQty:    req.Quantity,
		LimitPrice:      req.LimitPrice,
		Status:          status.Status,
		RawStatus:       order.State,
		FilledQty:       status.FilledQty,
		AvgPrice:        status.AvgPrice,
		ExecutedQuote:   status.ExecutedQuote,
		Fee:             status.Fee,
		FeeAsset:        status.FeeAsset,
		Active:          bitpinStateActive(order.State),
		AckedAt:         bitpinPickTime(status.UpdatedAt),
		Raw:             MaskBody(string(raw)),
	}
	return ack, nil
}

// --- CancelOrder ---

// CancelOrder accepts EITHER a numeric exchange order id OR an identifier string
// (the ClientOrderID passed at placement); the two use different URL forms.
func (b *bitpinPrivate) CancelOrder(ctx context.Context, exchangeOrderID string) error {
	if exchangeOrderID == "" {
		return fmt.Errorf("bitpin CancelOrder: empty order id")
	}
	path := "/api/v1/odr/orders/" + url.PathEscape(exchangeOrderID) + "/"
	if !bitpinIsDecimalString(exchangeOrderID) {
		path = "/api/v1/odr/orders/identifier/" + url.PathEscape(exchangeOrderID) + "/"
	}
	_, err := b.doJSON(ctx, "cancel", http.MethodDelete, path, nil, nil)
	return err
}

// --- GetOrder ---

func (b *bitpinPrivate) GetOrder(ctx context.Context, exchangeOrderID string) (execution.OrderStatus, error) {
	if exchangeOrderID == "" {
		return execution.OrderStatus{}, fmt.Errorf("bitpin GetOrder: empty order id")
	}
	var order bitpinOrder
	if bitpinIsDecimalString(exchangeOrderID) {
		_, err := b.doJSON(ctx, "order", http.MethodGet, "/api/v1/odr/orders/"+url.PathEscape(exchangeOrderID)+"/", nil, &order)
		if err != nil {
			return execution.OrderStatus{}, err
		}
	} else {
		return b.getOrderByIdentifier(ctx, exchangeOrderID)
	}
	if len(order.ID) == 0 && order.Identifier == "" {
		return execution.OrderStatus{}, fmt.Errorf("bitpin GetOrder %q: %w", exchangeOrderID, execution.ErrOrderUnknown)
	}
	return bitpinOrderToStatus(order, "", ""), nil
}

// GetOrderByClientOrderID implements exchanges.ClientOrderLookup: it resolves an order by the
// "identifier" (our client order id) via GET /odr/orders/?identifier=<id> — ALWAYS the identifier
// endpoint, never the numeric-id path, so an all-digit client id can never be misrouted. Used by
// the ambiguous-place recovery when the exchange order id is unknown.
func (b *bitpinPrivate) GetOrderByClientOrderID(ctx context.Context, clientOrderID string) (execution.OrderStatus, error) {
	if clientOrderID == "" {
		return execution.OrderStatus{}, fmt.Errorf("bitpin GetOrderByClientOrderID: empty identifier")
	}
	return b.getOrderByIdentifier(ctx, clientOrderID)
}

func (b *bitpinPrivate) getOrderByIdentifier(ctx context.Context, identifier string) (execution.OrderStatus, error) {
	// The ?identifier= endpoint returns a top-level array (not {"results":[]}).
	raw, err := b.doJSON(ctx, "order", http.MethodGet, "/api/v1/odr/orders/?identifier="+url.QueryEscape(identifier), nil, nil)
	if err != nil {
		return execution.OrderStatus{}, err
	}
	var order bitpinOrder
	var direct []bitpinOrder
	if jerr := json.Unmarshal(raw, &direct); jerr == nil && len(direct) > 0 {
		order = direct[0]
	} else {
		var payload bitpinOrdersResponse
		if jerr := json.Unmarshal(raw, &payload); jerr == nil && len(payload.Results) > 0 {
			order = payload.Results[0]
		}
	}
	if len(order.ID) == 0 && order.Identifier == "" {
		return execution.OrderStatus{}, fmt.Errorf("bitpin identifier %q: %w", identifier, execution.ErrOrderUnknown)
	}
	return bitpinOrderToStatus(order, "", ""), nil
}

// --- GetOpenOrders ---

func (b *bitpinPrivate) GetOpenOrders(ctx context.Context, symbol string) ([]execution.OrderStatus, error) {
	path := "/api/v1/odr/orders/?state=active&limit=100"
	if symbol != "" {
		venueSym := bitpinNativeSymbol(symbol)
		if v, ok := VenueSymbol(b.cfg, symbol); ok {
			venueSym = bitpinNativeSymbol(v)
		}
		path += "&symbol=" + url.QueryEscape(venueSym)
	}
	raw, err := b.doJSON(ctx, "open_orders", http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, err
	}
	orders := bitpinDecodeOrders(raw)
	out := make([]execution.OrderStatus, 0, len(orders))
	for _, o := range orders {
		st := bitpinOrderToStatus(o, "", "")
		if !bitpinStateActive(o.State) {
			continue
		}
		out = append(out, st)
	}
	return out, nil
}

func (b *bitpinPrivate) SubscribeOrderUpdates(ctx context.Context) (<-chan execution.NormalizedOrderEvent, error) {
	// Bitpin order updates arrive over a Centrifugo WebSocket that is deferred;
	// this adapter is polling-only (OrderStatusPoll = true, OrderUpdatesWS = false).
	return nil, Unsupported(bitpinCode, "order-update WebSocket")
}

// ─── shared order model + parsing ──────────────────────────────────────────

type bitpinOrder struct {
	ID                 json.RawMessage `json:"id"`
	Identifier         string          `json:"identifier"`
	State              string          `json:"state"`
	Type               string          `json:"type"`
	Side               string          `json:"side"`
	Symbol             string          `json:"symbol"`
	Price              json.RawMessage `json:"price"`
	AveragePrice       json.RawMessage `json:"average_price"`
	BaseAmount         json.RawMessage `json:"base_amount"`
	QuoteAmount        json.RawMessage `json:"quote_amount"`
	RemainAmount       json.RawMessage `json:"remain_amount"`
	DealedBaseAmount   json.RawMessage `json:"dealed_base_amount"`
	DealedQuoteAmount  json.RawMessage `json:"dealed_quote_amount"`
	Commission         json.RawMessage `json:"commission"`
	CommissionCurrency string          `json:"commission_currency"`
	Fee                json.RawMessage `json:"fee"`
	CreatedAt          string          `json:"created_at"`
	ClosedAt           string          `json:"closed_at"`
}

type bitpinOrdersResponse struct {
	Results []bitpinOrder `json:"results"`
}

// bitpinOrderToStatus maps a raw Bitpin order onto a normalized OrderStatus.
// fallbackSymbol/fallbackSide are used when the order body omits them.
func bitpinOrderToStatus(o bitpinOrder, fallbackSymbol, fallbackSide string) execution.OrderStatus {
	numericID := bitpinStringFromRaw(o.ID)
	identifier := strings.TrimSpace(o.Identifier)
	exchangeID := numericID
	if exchangeID == "" {
		exchangeID = identifier
	}
	symbol := fallbackSymbol
	if symbol == "" && o.Symbol != "" {
		symbol = bitpinInternalSymbol(o.Symbol)
	}
	side := strings.ToLower(fallbackSide)
	if o.Side != "" {
		side = strings.ToLower(o.Side)
	}

	requested := bitpinDecRaw(o.BaseAmount)
	filled := bitpinDecRaw(o.DealedBaseAmount)
	executedQuote := bitpinDecRaw(o.DealedQuoteAmount)
	remaining := bitpinDecRaw(o.RemainAmount)
	if requested.IsZero() && filled.IsPositive() {
		requested = filled.Add(remaining)
	}
	if remaining.IsZero() && requested.IsPositive() {
		remaining = requested.Sub(filled)
		if remaining.IsNegative() {
			remaining = decimal.Zero
		}
	}
	avg := bitpinDecRaw(o.AveragePrice)
	if avg.IsZero() {
		avg = bitpinDecRaw(o.Price)
	}
	if avg.IsZero() && filled.IsPositive() && executedQuote.IsPositive() {
		avg = executedQuote.Div(filled)
	}
	fee := bitpinDecRaw(o.Commission)
	if fee.IsZero() {
		fee = bitpinDecRaw(o.Fee)
	}
	feeAsset := bitpinInternalAsset(o.CommissionCurrency)
	at := bitpinParseTime(firstNonEmptyBitpin(o.ClosedAt, o.CreatedAt))
	created := bitpinParseTime(o.CreatedAt)

	return execution.OrderStatus{
		ExchangeOrderID: exchangeID,
		ClientOrderID:   identifier,
		Symbol:          symbol,
		Side:            side,
		Status:          mapBitpinOrderState(o.State, filled, requested),
		IntendedQty:     requested,
		FilledQty:       filled,
		RemainingQty:    remaining,
		AvgPrice:        avg,
		ExecutedQuote:   executedQuote,
		Fee:             fee,
		FeeAsset:        feeAsset,
		CreatedAt:       created,
		UpdatedAt:       at,
	}
}

// mapBitpinOrderState maps Bitpin's raw "state" string (plus fill amounts) onto a
// normalized execution.State*. Faithful port of iranArb's mapBitpinOrderState.
func mapBitpinOrderState(status string, filled, requested decimal.Decimal) execution.NormalizedOrderState {
	status = strings.ToLower(strings.TrimSpace(status))
	if requested.IsPositive() && filled.Equal(requested) {
		return execution.StateFilled
	}
	switch status {
	case "closed", "done", "filled":
		if filled.IsPositive() {
			if requested.IsPositive() && filled.LessThan(requested) {
				return execution.StatePartiallyCanceled
			}
			return execution.StateFilled
		}
		return execution.StateCanceled
	case "active", "open":
		if filled.IsPositive() {
			return execution.StatePartiallyFilled
		}
		return execution.StateOpen
	case "canceled", "cancelled", "cancel_requested":
		if filled.IsPositive() {
			return execution.StatePartiallyCanceled
		}
		return execution.StateCanceled
	default:
		if filled.IsPositive() {
			return execution.StatePartiallyCanceled
		}
		return execution.StateCanceled
	}
}

func bitpinStateActive(status string) bool {
	s := strings.ToLower(strings.TrimSpace(status))
	return s == "active" || s == "open"
}

// bitpinDecodeOrders tolerates both {"results":[...]} and a bare [...] array.
func bitpinDecodeOrders(raw []byte) []bitpinOrder {
	var payload bitpinOrdersResponse
	if err := json.Unmarshal(raw, &payload); err == nil && len(payload.Results) > 0 {
		return payload.Results
	}
	var direct []bitpinOrder
	if err := json.Unmarshal(raw, &direct); err == nil {
		return direct
	}
	return nil
}

// parseBitpinWallets tolerates a bare array or a {results|wallets|data:[...]} envelope.
func parseBitpinWallets(raw []byte) ([]bitpinWalletItem, error) {
	var direct []bitpinWalletItem
	if err := json.Unmarshal(raw, &direct); err == nil {
		return direct, nil
	}
	var env struct {
		Results []bitpinWalletItem `json:"results"`
		Wallets []bitpinWalletItem `json:"wallets"`
		Data    []bitpinWalletItem `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, err
	}
	switch {
	case env.Results != nil:
		return env.Results, nil
	case env.Wallets != nil:
		return env.Wallets, nil
	case env.Data != nil:
		return env.Data, nil
	default:
		return nil, fmt.Errorf("bitpin wallets response has no wallets/results/data")
	}
}

// ─── error helpers ─────────────────────────────────────────────────────────

// apiError builds a *NormalizedAPIError from a non-2xx response, wrapping the
// classifiable execution sentinel. The body is secret-masked before storage.
func (b *bitpinPrivate) apiError(op string, status int, body []byte) error {
	return bitpinAPIError(op, status, body)
}

func (b *bitpinPrivate) authError(msg string, status int) error {
	return &NormalizedAPIError{
		Exchange: bitpinCode, Op: "auth", StatusCode: status,
		Category: classifyHTTPStatus(status), Retryable: status == http.StatusTooManyRequests,
		Message: msg, Err: bitpinSentinelFor(status),
	}
}

func bitpinAPIError(op string, status int, body []byte) error {
	return &NormalizedAPIError{
		Exchange:   bitpinCode,
		Op:         op,
		StatusCode: status,
		Category:   classifyHTTPStatus(status),
		Retryable:  status >= 500 || status == http.StatusTooManyRequests,
		Message:    MaskBody(string(body)),
		Err:        bitpinSentinelFor(status),
	}
}

// bitpinSentinelFor maps an HTTP status to an execution sentinel for errors.Is.
func bitpinSentinelFor(status int) error {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return execution.ErrAuthFailed
	case status == http.StatusTooManyRequests:
		return execution.ErrRateLimited
	case status == http.StatusNotFound:
		return execution.ErrOrderUnknown
	default:
		return nil
	}
}

// parseBitpinAuthThrottle returns how long to wait before retrying the auth
// endpoint after a 429. Prefers the Retry-After header (seconds), then the
// integer in Bitpin's body. Returns 0 if neither yields a usable duration.
func parseBitpinAuthThrottle(retryAfterHeader string, body []byte) time.Duration {
	if v := strings.TrimSpace(retryAfterHeader); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	if m := bitpinAuthThrottleRe.FindSubmatch(body); len(m) == 2 {
		if n, err := strconv.Atoi(string(m[1])); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 0
}

// ─── small shared helpers ──────────────────────────────────────────────────

// bitpinGet performs an unauthenticated GET, returning *NormalizedAPIError on non-2xx.
func bitpinGet(ctx context.Context, hc *http.Client, baseURL, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("bitpin GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, bitpinAPIError(path, resp.StatusCode, body)
	}
	return body, nil
}

// bitpinLevelsFromPairs parses [[price, qty], ...] rows into levels, skipping
// malformed/zero rows. (binance.levelsFromPairs takes [2]string; Bitpin's REST
// uses variable-length []string rows, hence a dedicated parser.)
func bitpinLevelsFromPairs(rows [][]string) []domain.Level {
	out := make([]domain.Level, 0, len(rows))
	for _, row := range rows {
		if len(row) < 2 {
			continue
		}
		price := parseDec(row[0])
		qty := parseDec(row[1])
		if price.IsZero() && qty.IsZero() {
			continue
		}
		out = append(out, domain.Level{Price: price, Quantity: qty})
	}
	return out
}

// bitpinNativeSymbol converts canonical "BTC/IRT" (or any underscore/dash form)
// to Bitpin's underscore venue symbol "BTC_IRT".
func bitpinNativeSymbol(s string) string {
	n := strings.ToUpper(strings.TrimSpace(s))
	n = strings.ReplaceAll(n, "/", "_")
	n = strings.ReplaceAll(n, "-", "_")
	for strings.Contains(n, "__") {
		n = strings.ReplaceAll(n, "__", "_")
	}
	return strings.Trim(n, "_")
}

// bitpinInternalSymbol reverses bitpinNativeSymbol: "BTC_IRT" -> "BTC/IRT".
func bitpinInternalSymbol(native string) string {
	n := bitpinNativeSymbol(native)
	parts := strings.SplitN(n, "_", 2)
	if len(parts) != 2 {
		return n
	}
	canonical := parts[0] + "/" + parts[1]
	if norm, err := domain.NormalizeSymbol(canonical); err == nil {
		return norm
	}
	return canonical
}

// bitpinInternalAsset normalizes a Bitpin asset/commission currency code.
func bitpinInternalAsset(asset string) string {
	a := strings.ToUpper(strings.TrimSpace(asset))
	a = strings.ReplaceAll(a, "_", "")
	switch a {
	case "TOMAN", "IRT", "IRR", "RLS", "RIAL":
		return domain.CurrencyIRT
	default:
		return a
	}
}

func bitpinIsDecimalString(s string) bool {
	if s == "" {
		return false
	}
	_, err := decimal.NewFromString(s)
	return err == nil
}

// bitpinStringFromRaw extracts a string from a json.RawMessage that may be a
// quoted string or a bare number.
func bitpinStringFromRaw(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return ""
	}
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		var out string
		if err := json.Unmarshal(raw, &out); err == nil {
			return out
		}
		return strings.Trim(s, `"`)
	}
	return s
}

// bitpinDecRaw parses a json.RawMessage (quoted or bare number) into a decimal,
// returning zero on any problem.
func bitpinDecRaw(raw json.RawMessage) decimal.Decimal {
	s := bitpinStringFromRaw(raw)
	return parseDec(s)
}

func bitpinParseTime(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000000Z"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func bitpinPickTime(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now().UTC()
	}
	return t
}

func firstNonEmptyBitpin(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
