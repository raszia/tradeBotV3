package exchanges

import (
	"context"
	"errors"
	"testing"

	"v3TradeBot/internal/domain"
)

// fakePublic / fakePrivate are minimal in-package adapters for factory tests.
type fakePublic struct{ caps Capabilities }

func (f fakePublic) Name() string               { return "fakepub" }
func (f fakePublic) Capabilities() Capabilities { return f.caps }
func (f fakePublic) GetMarkets(context.Context) ([]NormalizedMarket, error) {
	return nil, nil
}
func (f fakePublic) GetOrderBook(context.Context, string) (domain.OrderBook, error) {
	return domain.OrderBook{}, nil
}
func (f fakePublic) SubscribeOrderBook(context.Context, []string) (<-chan domain.OrderBook, error) {
	return nil, Unsupported("fakepub", "SubscribeOrderBook")
}

func TestFactoryRegisterAndConstruct(t *testing.T) {
	const code = "factorytest_ex"
	Register(Registration{
		Code:         code,
		Capabilities: Capabilities{MarketMetadata: true, OrderBookREST: true},
		NewPublic: func(cfg ClientConfig, _ *IOLogger) (PublicClient, error) {
			return fakePublic{caps: Capabilities{MarketMetadata: true}}, nil
		},
		// NewPrivate intentionally nil.
	})

	if _, ok := Lookup(code); !ok {
		t.Fatal("registration not found")
	}
	caps, ok := CapabilitiesFor(code)
	if !ok || !caps.MarketMetadata {
		t.Fatalf("capabilities lookup failed: %+v ok=%v", caps, ok)
	}

	pub, err := NewPublicClient(ClientConfig{Code: code}, nil)
	if err != nil {
		t.Fatalf("NewPublicClient: %v", err)
	}
	if pub.Name() != "fakepub" {
		t.Errorf("name = %q", pub.Name())
	}

	// No private constructor registered -> typed unsupported error.
	_, err = NewPrivateClient(ClientConfig{Code: code, Creds: StaticCredentialProvider{}}, nil)
	if !errors.Is(err, ErrUnsupported) {
		t.Errorf("NewPrivateClient err = %v, want ErrUnsupported", err)
	}

	// Unknown code.
	if _, err := NewPublicClient(ClientConfig{Code: "nope_nope"}, nil); err == nil {
		t.Error("unknown code should error")
	}

	// All() includes our registration and is sorted.
	found := false
	for _, r := range All() {
		if r.Code == code {
			found = true
		}
	}
	if !found {
		t.Error("All() missing registration")
	}
}

func TestFactoryPrivateRequiresCreds(t *testing.T) {
	const code = "factorytest_priv"
	Register(Registration{
		Code:         code,
		Capabilities: Capabilities{PlaceOrder: true},
		NewPrivate: func(cfg ClientConfig, _ *IOLogger) (PrivateClient, error) {
			return nil, nil // never reached: creds check happens first
		},
	})
	_, err := NewPrivateClient(ClientConfig{Code: code, Creds: nil}, nil)
	if err == nil {
		t.Fatal("private client without creds should error")
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	const code = "factorytest_dup"
	Register(Registration{Code: code})
	defer func() {
		if recover() == nil {
			t.Error("duplicate registration should panic")
		}
	}()
	Register(Registration{Code: code})
}
