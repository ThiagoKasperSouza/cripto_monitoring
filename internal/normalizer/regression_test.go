package normalizer

import (
	"strings"
	"testing"
)

// Regression tests for bugs found during Fase 1 implementation
// (2026-11-18). Each test names the failure mode it guards against.

// Bug: a spot frame carries no "b"/"a" at all, and futures may encode them
// as strings or numbers. Worse: `"b": null` used to fall through to
// strconv.ParseInt("null") and the WHOLE trade frame was discarded silently.
// JSON null / absence must be treated as 0, never as a parse error.
func TestRegression_NullOrMissingOrderIDsDoNotDropFrame(t *testing.T) {
	cases := map[string]struct {
		frame      string
		wantBuyer  int64
		wantSeller int64
	}{
		"both null":          {`{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"1","q":"1","b":null,"a":null,"T":1,"m":true,"M":true}`, 0, 0},
		"buyer null":         {`{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"1","q":"1","b":null,"a":55,"T":1,"m":true,"M":true}`, 0, 55},
		"seller null":        {`{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"1","q":"1","b":"77","a":null,"T":1,"m":true,"M":true}`, 77, 0},
		"both absent (spot)": {`{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"1","q":"1","T":1,"m":true,"M":true}`, 0, 0},
		"string encoded":     {`{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"1","q":"1","b":"77","a":"55","T":1,"m":true,"M":true}`, 77, 55},
		"number encoded":     {`{"e":"trade","E":1,"s":"BTCUSDT","t":1,"p":"1","q":"1","b":77,"a":55,"T":1,"m":true,"M":true}`, 77, 55},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tr, err := NormalizeTrade([]byte(tc.frame))
			if err != nil {
				t.Fatalf("frame must NOT be dropped: %v", err)
			}
			if tr.GetBuyerOrderId() != tc.wantBuyer || tr.GetSellerOrderId() != tc.wantSeller {
				t.Fatalf("buyer/seller: got %d/%d, want %d/%d",
					tr.GetBuyerOrderId(), tr.GetSellerOrderId(), tc.wantBuyer, tc.wantSeller)
			}
		})
	}
}

// Bug: `"data": null` in the combined-stream envelope passed the len()>0
// check and reached NormalizeTrade as the literal bytes "null", relying on
// downstream validation to reject it by accident.
func TestRegression_EnvelopeWithNullDataIsRejected(t *testing.T) {
	_, err := ExtractTradePayload([]byte(`{"stream":"btcusdt@trade","data":null}`))
	if err == nil {
		t.Fatal("expected error for null envelope payload")
	}
	if !strings.Contains(err.Error(), "null") {
		t.Fatalf("error should mention null payload, got: %v", err)
	}
}

// Defensive: a literal JSON `null` frame must not produce a zero-valued
// trade. Extraction must reject it (it is neither an envelope with data nor
// an object with the trade "e" field).
func TestRegression_LiteralNullFrameIsRejected(t *testing.T) {
	if _, err := ExtractTradePayload([]byte(`null`)); err == nil {
		t.Fatal("literal null frame must be rejected by ExtractTradePayload")
	}
}

// Defensive: `"data": ""` is not valid JSON. It must never yield a trade,
// whether rejected at extraction time or at normalization time — but it must
// be an error, not a silent zero-valued message.
func TestRegression_EnvelopeWithEmptyDataStringNeverYieldsTrade(t *testing.T) {
	payload, err := ExtractTradePayload([]byte(`{"stream":"btcusdt@trade","data":""}`))
	if err == nil {
		if _, nerr := NormalizeTrade(payload); nerr == nil {
			t.Fatal("empty data string must not produce a trade")
		}
	}
}
