// Package tracing configura o OpenTelemetry (OTLP/HTTP) para os serviços Go
// do ecossistema: gateway (WS→Kafka) e BFF (gRPC→WebSocket).
//
// Use RTD_OTLP_ENDPOINT para apontar o coletor (ex: "localhost:4318") e
// RTD_OTEL_DISABLED=true para desligar export (mantém o wiring sem custo de export).
package tracing

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Setup inicializa o tracer provider global. Falhas de export não derrubam o
// serviço: o batch processor apenas descarta e loga.
func Setup(serviceName string) (func(context.Context) error, error) {
	endpoint := os.Getenv("RTD_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4318"
	}
	if os.Getenv("RTD_OTEL_DISABLED") == "true" {
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, err
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithResource(res),
		trace.WithSampler(trace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp.Shutdown, nil
}
