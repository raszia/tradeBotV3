package exchanges

import (
	"context"
	"io"
	"net/http"
)

// This file holds the machinery shared by every private adapter's two-stage mutation boundary
// (PR20 correction round 8 #1/#2/#3):
//
//   - doPreparedRequest performs the SINGLE remaining network round-trip for an already-built,
//     immutable *http.Request. Everything fallible — credential load/decrypt, symbol
//     normalization, payload/body construction, and http.Request construction — happens during
//     preparation (before the executor commits MarkInFlight). The only thing that happens at the
//     real send boundary is binding the send context and calling http.Client.Do.
//
//   - WithNetworkPacer / paceNetwork let an adapter reserve a per-exchange pacing slot for the
//     network calls it makes AUTONOMOUSLY during preparation (e.g. a Bitpin auth/refresh call),
//     so pacing reservations equal actual HTTP calls rather than counting preparation-method
//     invocations. The order/cancel send is paced separately by the executor.

// doPreparedRequest binds ctx to the pre-built request and performs the single network
// round-trip, returning the response status, headers and body. It constructs NOTHING: the
// request was fully built during preparation, so there is no fallible pre-network step here. A
// transport error is returned verbatim (it occurred at/after the send boundary and is therefore
// AMBIGUOUS — the caller must not classify it definitely-not-sent).
func doPreparedRequest(ctx context.Context, hc *http.Client, req *http.Request) (status int, hdr http.Header, body []byte, err error) {
	resp, err := hc.Do(req.WithContext(ctx))
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, raw, nil
}

type networkPacerKey struct{}

// WithNetworkPacer attaches a pacing hook that a two-stage adapter MUST invoke immediately
// before each network request it initiates on its own during preparation (e.g. a Bitpin
// auth/refresh HTTP call). The hook blocks until the caller's per-exchange rate-limit slot is
// free and returns ctx.Err() if cancelled. Adapters that make NO preparation network call never
// invoke it and so consume no slot (round 8 #3). A nil hook is ignored.
func WithNetworkPacer(ctx context.Context, pace func(context.Context) error) context.Context {
	if pace == nil {
		return ctx
	}
	return context.WithValue(ctx, networkPacerKey{}, pace)
}

// paceNetwork invokes the pacing hook carried by ctx (if any) and returns its error. When no
// hook is present (non-executor callers, reads) it is a no-op returning nil.
func paceNetwork(ctx context.Context) error {
	if p, ok := ctx.Value(networkPacerKey{}).(func(context.Context) error); ok && p != nil {
		return p(ctx)
	}
	return nil
}
