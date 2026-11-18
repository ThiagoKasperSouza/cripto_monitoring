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
	"sort"
	"strings"
	"sync/atomic"
)

// Registry is a simple atomic counter/gauge store.
type Registry struct {
	messagesReceived atomic.Int64
	messagesPublished atomic.Int64
	parseErrors      atomic.Int64
	reconnects       atomic.Int64
	wsConnected      atomic.Int64
	lastEventTimeMs  atomic.Int64
}

func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) MessagesReceived()  { r.messagesReceived.Add(1) }
func (r *Registry) MessagesPublished() { r.messagesPublished.Add(1) }
func (r *Registry) ParseErrors()       { r.parseErrors.Add(1) }
func (r *Registry) Reconnects()        { r.reconnects.Add(1) }
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

func (r *Registry) render() string {
	values := map[string]int64{
		"gateway_messages_received_total":  r.messagesReceived.Load(),
		"gateway_messages_published_total": r.messagesPublished.Load(),
		"gateway_parse_errors_total":       r.parseErrors.Load(),
		"gateway_reconnects_total":         r.reconnects.Load(),
		"gateway_ws_connected":             r.wsConnected.Load(),
		"gateway_last_event_time_ms":       r.lastEventTimeMs.Load(),
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&sb, "# TYPE %s gauge\n%s %d\n", k, k, values[k])
	}
	return sb.String()
}
