package normalizer

import "testing"

const realTradeFrame = `{
  "stream": "btcusdt@trade",
  "data": {
    "e": "trade",
    "E": 1730000000000,
    "s": "BTCUSDT",
    "t": 123456789,
    "p": "97234.56000000",
    "q": "0.00125000",
    "b": 888000111,
    "a": 555000222,
    "T": 1729999999990,
    "m": true,
    "M": true
  }
}`

const bareTradeFrame = `{
  "e": "trade",
  "E": 1730000000001,
  "s": "ETHUSDT",
  "t": 987654321,
  "p": "3100.50",
  "q": "1.5",
  "b": 1,
  "a": 2,
  "T": 1729999999995,
  "m": false,
  "M": true
}`

func TestExtractTradePayload_CombinedEnvelope(t *testing.T) {
	payload, err := ExtractTradePayload([]byte(realTradeFrame))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tr, err := NormalizeTrade(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tr.GetSymbol() != "BTCUSDT" || tr.GetTradeId() != 123456789 {
		t.Fatalf("wrong trade: %+v", tr)
	}
	if tr.GetPrice() != "97234.56000000" {
		t.Fatalf("price must be preserved as string, got %q", tr.GetPrice())
	}
	if tr.GetEventTime() != 1730000000000 {
		t.Fatalf("event_time: got %d", tr.GetEventTime())
	}
	if !tr.GetIsBuyerMarketMaker() {
		t.Fatal("is_buyer_market_maker must be true")
	}
}

func TestExtractTradePayload_BareFrame(t *testing.T) {
	payload, err := ExtractTradePayload([]byte(bareTradeFrame))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tr, err := NormalizeTrade(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tr.GetSymbol() != "ETHUSDT" || tr.GetBuyerOrderId() != 1 || tr.GetSellerOrderId() != 2 {
		t.Fatalf("wrong trade: %+v", tr)
	}
}

func TestNormalizeTrade_OrderIDsVariants(t *testing.T) {
	// spot: b/a ausentes -> 0
	spot := `{"e":"trade","E":1,"s":"BTCUSDT","t":10,"p":"1","q":"1","T":1,"m":true,"M":true}`
	tr, err := NormalizeTrade([]byte(spot))
	if err != nil {
		t.Fatal(err)
	}
	if tr.GetBuyerOrderId() != 0 || tr.GetSellerOrderId() != 0 {
		t.Fatalf("expected 0 ids, got %d/%d", tr.GetBuyerOrderId(), tr.GetSellerOrderId())
	}
	// futuros: b/a como string
	futures := `{"e":"trade","E":1,"s":"BTCUSDT","t":10,"p":"1","q":"1","b":"777","a":"555","T":1,"m":true,"M":true}`
	tr, err = NormalizeTrade([]byte(futures))
	if err != nil {
		t.Fatal(err)
	}
	if tr.GetBuyerOrderId() != 777 || tr.GetSellerOrderId() != 555 {
		t.Fatalf("expected 777/555, got %d/%d", tr.GetBuyerOrderId(), tr.GetSellerOrderId())
	}
	// futuros: b/a como número
	numeric := `{"e":"trade","E":1,"s":"BTCUSDT","t":10,"p":"1","q":"1","b":777,"a":555,"T":1,"m":true,"M":true}`
	tr, err = NormalizeTrade([]byte(numeric))
	if err != nil {
		t.Fatal(err)
	}
	if tr.GetBuyerOrderId() != 777 || tr.GetSellerOrderId() != 555 {
		t.Fatalf("expected 777/555, got %d/%d", tr.GetBuyerOrderId(), tr.GetSellerOrderId())
	}
}

func TestNormalizeTrade_ValidationErrors(t *testing.T) {
	cases := map[string]string{
		"not json":       `{invalid`,
		"wrong event":    `{"e":"aggTrade","E":1,"s":"BTCUSDT","t":1,"p":"1","q":"1"}`,
		"empty symbol":   `{"e":"trade","E":1,"s":"","t":1,"p":"1","q":"1"}`,
		"bad trade id":   `{"e":"trade","E":1,"s":"BTCUSDT","t":0,"p":"1","q":"1"}`,
		"bad price":      `{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"abc","q":"1"}`,
		"negative qty":   `{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"1","q":"-3"}`,
		"zero price":     `{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"0","q":"1"}`,
		"missing fields": `{"e":"trade"}`,
	}
	for name, input := range cases {
		if _, err := NormalizeTrade([]byte(input)); err == nil {
			t.Errorf("%s: expected error, got nil", name)
		}
	}
}
