// Package metrics provides a minimal, dependency-free metrics registry for
// the gateway. It exposes counters/gauges in Prometheus text exposition
// format on an HTTP endpoint (default :9090/metrics).
//
// Phase 3 will replace this with the official prometheus/client_golang
// client; the metric names are already chosen to be compatible.
package metrics

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
)

// Registry is a simple atomic counter/gauge store.
type Registry struct {
	messagesReceived  atomic.Int64
	messagesPublished atomic.Int64
	parseErrors       atomic.Int64
	reconnects        atomic.Int64
	publishErrors     atomic.Int64
	wsConnected       atomic.Int64
	lastEventTimeMs   atomic.Int64
}

func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) MessagesReceived()  { r.messagesReceived.Add(1) }
func (r *Registry) MessagesPublished() { r.messagesPublished.Add(1) }
func (r *Registry) ParseErrors()       { r.parseErrors.Add(1) }
func (r *Registry) Reconnects()        { r.reconnects.Add(1) }
func (r *Registry) PublishErrors()     { r.publishErrors.Add(1) }
func (r *Registry) SetConnected(v bool) {
	if v {
		r.wsConnected.Store(1)
	} else {
		r.wsConnected.Store(0)
	}
}
func (r *Registry) SetLastEventTimeMs(v int64) { r.lastEventTimeMs.Store(v) }

// Handler returns an http.Handler exposing /metrics in Prometheus text format.
func (r *Registry) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		fmt.Fprint(w, r.render())
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

type metricKind struct {
	name string
	kind string // "counter" or "gauge", per Prometheus exposition format
}

var metricKinds = []metricKind{
	{"gateway_messages_received_total", "counter"},
	{"gateway_messages_published_total", "counter"},
	{"gateway_parse_errors_total", "counter"},
	{"gateway_reconnects_total", "counter"},
	{"gateway_publish_errors_total", "counter"},
	{"gateway_ws_connected", "gauge"},
	{"gateway_last_event_time_ms", "gauge"},
}

func (r *Registry) render() string {
	values := map[string]int64{
		"gateway_messages_received_total":  r.messagesReceived.Load(),
		"gateway_messages_published_total": r.messagesPublished.Load(),
		"gateway_parse_errors_total":       r.parseErrors.Load(),
		"gateway_reconnects_total":         r.reconnects.Load(),
		"gateway_publish_errors_total":     r.publishErrors.Load(),
		"gateway_ws_connected":             r.wsConnected.Load(),
		"gateway_last_event_time_ms":       r.lastEventTimeMs.Load(),
	}
	// Regression (Fase 1): *_total metrics were exposed as `gauge`.
	// Prometheus clients treat *_total as counters; the TYPE line must say
	// counter or rate()/increase() queries silently misbehave.
	var sb strings.Builder
	for _, m := range metricKinds {
		fmt.Fprintf(&sb, "# TYPE %s %s\n%s %d\n", m.name, m.kind, m.name, values[m.name])
	}
	return sb.String()
}
