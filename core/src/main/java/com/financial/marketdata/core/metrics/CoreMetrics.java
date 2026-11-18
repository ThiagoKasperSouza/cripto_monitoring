package com.financial.marketdata.core.metrics;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Métricas do core gRPC expostas via Micrometer → endpoint HTTP de scraping
 * para o Prometheus (porta default 8081).
 *
 * <p>Métricas emitidas:
 * <ul>
 *   <li>{@code core_trades_consumed_total} — registros lidos do Kafka;</li>
 *   <li>{@code core_dispatched_total} — trades despachados p/ listeners;</li>
 *   <li>{@code core_duplicates_dropped_total} — duplicados descartados pela dedup;</li>
 *   <li>{@code core_parse_errors_total} — records com payload inválido;</li>
 *   <li>{@code core_grpc_streams_active} — streams StreamTicker ativos agora (gauge);</li>
 *   <li>{@code core_slow_client_disconnects_total} — clientes desconectados por fila cheia;</li>
 *   <li>{@code core_rebalances_total} — rebalanceamentos observados no consumidor.</li>
 * </ul>
 */
public final class CoreMetrics {

    private static final Logger LOGGER = Logger.getLogger(CoreMetrics.class.getName());

    public static final PrometheusMeterRegistry REGISTRY =
            new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    private static final Counter tradesConsumed = Counter.builder("core_trades_consumed_total")
            .description("Trades consumidos do Kafka/Redpanda").register(REGISTRY);
    private static final Counter dispatched = Counter.builder("core_dispatched_total")
            .description("Trades despachados para os listeners gRPC após dedup").register(REGISTRY);
    private static final Counter duplicatesDropped = Counter.builder("core_duplicates_dropped_total")
            .description("Trades descartados por {symbol}:{trade_id} já visto").register(REGISTRY);
    private static final Counter parseErrors = Counter.builder("core_parse_errors_total")
            .description("Records com payload protobuf inválido").register(REGISTRY);
    private static final Counter slowClientDisconnects = Counter.builder("core_slow_client_disconnects_total")
            .description("Clientes desconectados por fila bounded cheia (RESOURCE_EXHAUSTED)").register(REGISTRY);
    private static final Counter rebalances = Counter.builder("core_rebalances_total")
            .description("Eventos de rebalance (revoked + assigned) observados pelo consumidor").register(REGISTRY);

    private static final AtomicInteger grpcStreamsActive = new AtomicInteger(0);
    static {
        Gauge.builder("core_grpc_streams_active", grpcStreamsActive, Number::doubleValue)
                .description("Streams StreamTicker ativos").register(REGISTRY);
    }

    private CoreMetrics() {}

    public static void tradesConsumed(long n)   { tradesConsumed.increment(n); }
    public static void dispatched()              { dispatched.increment(); }
    public static void duplicatesDropped()       { duplicatesDropped.increment(); }
    public static void parseErrors()             { parseErrors.increment(); }
    public static void slowClientDisconnects()   { slowClientDisconnects.increment(); }
    public static void rebalances(long n)        { rebalances.increment(n); }

    public static void streamOpened()  { grpcStreamsActive.incrementAndGet(); }
    public static void streamClosed()  { grpcStreamsActive.updateAndGet(v -> Math.max(0, v - 1)); }

    /** Sobe um HTTP server simples servindo /metrics no formato text exposition. */
    public static void startHttpServer(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = REGISTRY.scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.createContext("/healthz", exchange -> {
            byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
        LOGGER.info("core metrics endpoint listening on :" + port + "/metrics");
    }
}
