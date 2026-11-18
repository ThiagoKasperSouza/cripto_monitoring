package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCountersAppearInPrometheusFormat(t *testing.T) {
	r := NewRegistry()
	r.MessagesReceived()
	r.MessagesReceived()
	r.MessagesPublished()
	r.ParseErrors()
	r.Reconnects()
	r.SetConnected(true)
	r.SetLastEventTimeMs(1730000000000)

	req := httptest.NewRequest("GET", "/metrics", nil)
	nr := httptest.NewRecorder()
	r.Handler().ServeHTTP(nr, req)

	if nr.Code != 200 {
		t.Fatalf("expected 200, got %d", nr.Code)
	}
	body, _ := io.ReadAll(nr.Body)
	out := string(body)

	expected := []string{
		"gateway_messages_received_total 2",
		"gateway_messages_published_total 1",
		"gateway_parse_errors_total 1",
		"gateway_reconnects_total 1",
		"gateway_ws_connected 1",
		"gateway_last_event_time_ms 1730000000000",
	}
	for _, want := range expected {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q\noutput:\n%s", want, out)
		}
	}
	ct := nr.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("unexpected Content-Type %q", ct)
	}
}

func TestConnectedTogglesBackToZero(t *testing.T) {
	r := NewRegistry()
	r.SetConnected(true)
	r.SetConnected(false)
	req := httptest.NewRequest("GET", "/metrics", nil)
	nr := httptest.NewRecorder()
	r.Handler().ServeHTTP(nr, req)
	body, _ := io.ReadAll(nr.Body)
	if !strings.Contains(string(body), "gateway_ws_connected 0") {
		t.Fatal("expected gateway_ws_connected 0")
	}
}

func TestHealthz(t *testing.T) {
	r := NewRegistry()
	req := httptest.NewRequest("GET", "/healthz", nil)
	nr := httptest.NewRecorder()
	r.Handler().ServeHTTP(nr, req)
	if nr.Code != 200 {
		t.Fatalf("expected 200, got %d", nr.Code)
	}
}
