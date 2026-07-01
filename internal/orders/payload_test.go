package orders

import "testing"

// TestBuyIntentValidate (PR10 #5) covers the pre-send guard: a well-formed intent passes;
// every unsafe field (bad/zero price or quantity, wrong side/type, non-IOC, empty client id)
// is rejected so it can never reach PlaceOrder.
func TestBuyIntentValidate(t *testing.T) {
	valid := BuyIntentPayload{
		Side: "buy", OrderType: "limit", SimulatedIOC: true,
		IntendedPrice: "100", IntendedQuantity: "0.5", LocalClientOrderID: "c1-buy",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid intent rejected: %v", err)
	}

	bad := map[string]func(*BuyIntentPayload){
		"not simulated ioc": func(p *BuyIntentPayload) { p.SimulatedIOC = false },
		"wrong side":        func(p *BuyIntentPayload) { p.Side = "sell" },
		"wrong order type":  func(p *BuyIntentPayload) { p.OrderType = "market" },
		"unparseable price": func(p *BuyIntentPayload) { p.IntendedPrice = "abc" },
		"zero price":        func(p *BuyIntentPayload) { p.IntendedPrice = "0" },
		"negative price":    func(p *BuyIntentPayload) { p.IntendedPrice = "-1" },
		"empty price":       func(p *BuyIntentPayload) { p.IntendedPrice = "" },
		"unparseable qty":   func(p *BuyIntentPayload) { p.IntendedQuantity = "x" },
		"zero qty":          func(p *BuyIntentPayload) { p.IntendedQuantity = "0" },
		"negative qty":      func(p *BuyIntentPayload) { p.IntendedQuantity = "-0.5" },
		"empty client id":   func(p *BuyIntentPayload) { p.LocalClientOrderID = "" },
		"blank client id":   func(p *BuyIntentPayload) { p.LocalClientOrderID = "   " },
	}
	for name, mutate := range bad {
		p := valid
		mutate(&p)
		if err := p.Validate(); err == nil {
			t.Errorf("%s: Validate() = nil, want an error (must not be sent)", name)
		}
	}
}

// TestSellIntentValidate (PR10 #2) — the sell handler's equivalent pre-send validation.
func TestSellIntentValidate(t *testing.T) {
	valid := SellIntentPayload{Side: "sell", OrderType: "limit", Price: "110", Quantity: "1", LocalClientOrderID: "c1-sell"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid sell intent rejected: %v", err)
	}
	bad := map[string]func(*SellIntentPayload){
		"wrong side":        func(p *SellIntentPayload) { p.Side = "buy" },
		"wrong order type":  func(p *SellIntentPayload) { p.OrderType = "market" },
		"unparseable price": func(p *SellIntentPayload) { p.Price = "x" },
		"zero price":        func(p *SellIntentPayload) { p.Price = "0" },
		"zero qty":          func(p *SellIntentPayload) { p.Quantity = "0" },
		"empty client id":   func(p *SellIntentPayload) { p.LocalClientOrderID = "" },
	}
	for name, mutate := range bad {
		p := valid
		mutate(&p)
		if err := p.Validate(); err == nil {
			t.Errorf("%s: Validate() = nil, want an error", name)
		}
	}
}
