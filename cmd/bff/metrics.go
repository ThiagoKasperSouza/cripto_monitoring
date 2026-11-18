package main

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

type bffMetrics struct {
	registry       *prometheus.Registry
	wsConnections  prometheus.Gauge
	wsConnected    prometheus.Gauge
	wsActive       atomic.Int64
	messagesSent   prometheus.Counter
	upstreamErrors prometheus.Counter
}

func newBffMetrics() *bffMetrics {
	reg := prometheus.NewRegistry()
	m := &bffMetrics{
		registry: reg,
		wsConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "bff_ws_connections",
			Help: "Clientes WebSocket atualmente conectados ao BFF.",
		}),
		wsConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "bff_ws_connected",
			Help: "1 se pelo menos um cliente WebSocket está conectado.",
		}),
		messagesSent: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "bff_messages_sent_total",
			Help: "Mensagens de trade enviadas do BFF para os browsers.",
		}),
		upstreamErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "bff_upstream_errors_total",
			Help: "Erros no stream gRPC upstream (core de mercado).",
		}),
	}
	reg.MustRegister(m.wsConnections, m.wsConnected, m.messagesSent, m.upstreamErrors)
	return m
}
