// Package metrics exposes the gateway's operational metrics in Prometheus
// text exposition format on an HTTP endpoint (default :9090/metrics).
//
// Fase 3: backed by the official prometheus/client_golang, including
// histograms used for p50/p95/p99(9) latency panels in Grafana.
package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry groups all gateway collectors. It is safe for concurrent use.
type Registry struct {
	registry *prometheus.Registry

	messagesReceived  prometheus.Counter
	messagesPublished prometheus.Counter
	parseErrors       prometheus.Counter
	reconnects        prometheus.Counter
	publishErrors     prometheus.Counter

	wsConnected     prometheus.Gauge
	lastEventTimeMs prometheus.Gauge

	publishLatencyMs prometheus.Histogram
	eventAgeMs       prometheus.Histogram
}

func NewRegistry() *Registry {
	latencyBuckets := []float64{0.1, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000}
	r := &Registry{
		registry: prometheus.NewRegistry(),
		messagesReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_messages_received_total",
			Help: "Trades recebidos do WebSocket da Binance.",
		}),
		messagesPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_messages_published_total",
			Help: "Trades publicados com sucesso no barramento (Kafka/Redpanda ou stdout).",
		}),
		parseErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_parse_errors_total",
			Help: "Frames malformados ou trades inválidos descartados.",
		}),
		reconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_reconnects_total",
			Help: "Tentativas de reconexão com a Binance.",
		}),
		publishErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_publish_errors_total",
			Help: "Falhas ao publicar no barramento.",
		}),
		wsConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gateway_ws_connected",
			Help: "1 quando o WebSocket com a Binance está conectado, 0 caso contrário.",
		}),
		lastEventTimeMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gateway_last_event_time_ms",
			Help: "event_time do último trade processado (ms UTC).",
		}),
		publishLatencyMs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gateway_publish_latency_ms",
			Help:    "Latência de publicação no barramento (ms) — ack de broker.",
			Buckets: latencyBuckets,
		}),
		eventAgeMs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gateway_event_age_ms",
			Help:    "Idade do evento da exchange no momento da publicação (now - event_time).",
			Buckets: latencyBuckets,
		}),
	}
	r.registry.MustRegister(
		r.messagesReceived, r.messagesPublished, r.parseErrors, r.reconnects, r.publishErrors,
		r.wsConnected, r.lastEventTimeMs, r.publishLatencyMs, r.eventAgeMs,
	)
	return r
}

func (r *Registry) MessagesReceived()                { r.messagesReceived.Inc() }
func (r *Registry) MessagesPublished()               { r.messagesPublished.Inc() }
func (r *Registry) ParseErrors()                     { r.parseErrors.Inc() }
func (r *Registry) Reconnects()                      { r.reconnects.Inc() }
func (r *Registry) PublishErrors()                   { r.publishErrors.Inc() }
func (r *Registry) ObservePublishLatency(ms float64) { r.publishLatencyMs.Observe(ms) }
func (r *Registry) ObserveEventAge(ms float64)       { r.eventAgeMs.Observe(ms) }

func (r *Registry) SetConnected(v bool) {
	if v {
		r.wsConnected.Set(1)
	} else {
		r.wsConnected.Set(0)
	}
}

func (r *Registry) SetLastEventTimeMs(v int64) { r.lastEventTimeMs.Set(float64(v)) }

// Handler returns an http.Handler exposing /metrics (Prometheus format) and
// /healthz.
func (r *Registry) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "ok")
	})
	return mux
}
