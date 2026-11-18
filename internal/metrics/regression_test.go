package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

// Regression (Fase 1): *_total metrics were exposed with `# TYPE ... gauge`.
// Prometheus treats any metric ending in _total as a counter; when the TYPE
// line says gauge, rate()/increase() and the PromQL type checks misbehave.
// TYPE must match the semantics: counters for totals, gauges for state.
func TestRegression_TotalsAreTypedAsCounters(t *testing.T) {
	r := NewRegistry()
	req := httptest.NewRequest("GET", "/metrics", nil)
	nr := httptest.NewRecorder()
	r.Handler().ServeHTTP(nr, req)
	body, _ := io.ReadAll(nr.Body)
	out := string(body)

	counters := []string{
		"# TYPE gateway_messages_received_total counter",
		"# TYPE gateway_messages_published_total counter",
		"# TYPE gateway_parse_errors_total counter",
		"# TYPE gateway_reconnects_total counter",
		"# TYPE gateway_publish_errors_total counter",
	}
	for _, want := range counters {
		if !strings.Contains(out, want) {
			t.Errorf("missing counter TYPE line %q\noutput:\n%s", want, out)
		}
	}

	gauges := []string{
		"# TYPE gateway_ws_connected gauge",
		"# TYPE gateway_last_event_time_ms gauge",
	}
	for _, want := range gauges {
		if !strings.Contains(out, want) {
			t.Errorf("missing gauge TYPE line %q\noutput:\n%s", want, out)
		}
	}

	// And no *_total metric may be typed as gauge (the original bug):
	for _, bad := range counters {
		badLine := strings.Replace(bad, " counter", " gauge", 1)
		if strings.Contains(out, badLine) {
			t.Errorf("regression: *_total metric exposed as gauge: %q", badLine)
		}
	}
}
