package com.financial.marketdata.core;

import com.financial.marketdata.core.dedup.DedupStore;
import com.financial.marketdata.core.dedup.InMemoryDedupStore;
import com.financial.marketdata.core.dedup.RedisDedupStore;
import com.financial.marketdata.core.source.KafkaTradeSource;
import com.financial.marketdata.core.source.SimulatedTradeSource;
import com.financial.marketdata.core.source.TradeSource;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

/**
 * Entry point do serviço core de mercado (Camada 3 — gRPC sobre HTTP/2).
 *
 * <p>Uso:
 * <pre>{@code
 *   # Fase 1 (dev): fonte sintética, sem infra
 *   java -jar core.jar --port 50051
 *
 *   # Fase 2: consome o tópico market.trades (Redpanda/Kafka) com dedup Redis
 *   java -jar core.jar --port 50051 --source kafka \
 *        --brokers localhost:9092 --topic market.trades --group market-data-core \
 *        --dedup redis --redis-addr localhost:6379
 * }</pre>
 *
 * <p>Flags:
 * <ul>
 *   <li>{@code --port} (ou env {@code PORT}) — porta gRPC. Default 50051.</li>
 *   <li>{@code --source} — {@code simulated} (default) ou {@code kafka}.</li>
 *   <li>{@code --brokers} — lista Kafka/Redpanda comma-separated. Default localhost:9092.</li>
 *   <li>{@code --topic} — tópico de consumo. Default market.trades.</li>
 *   <li>{@code --group} — consumer group. Default market-data-core.</li>
 *   <li>{@code --dedup} — {@code redis} ou {@code memory} (default).</li>
 *   <li>{@code --redis-addr} — host:port do Redis. Default localhost:6379.</li>
 *   <li>{@code --dedup-ttl-ms} — TTL da chave {symbol}:{trade_id}. Default 300000 (5min).</li>
 * </ul>
 */
public final class MarketDataServer {

    private static final Logger LOGGER = Logger.getLogger(MarketDataServer.class.getName());

    private MarketDataServer() {}

    public static void main(String[] args) throws IOException, InterruptedException {
        Map<String, String> opts = parseArgs(args);
        int port = Integer.parseInt(opts.getOrDefault("port", System.getenv().getOrDefault("PORT", "50051")));
        String sourceType = opts.getOrDefault("source", "simulated");

        TradeSource source = buildSource(sourceType, opts);
        source.start();

        Server server = NettyServerBuilder.forPort(port)
                .addService(new MarketDataGrpcService(source))
                .build()
                .start();

        LOGGER.info("core service started on gRPC port " + port + " (source=" + sourceType + ")");

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("shutdown requested; draining gRPC server");
            server.shutdown();
            source.close();
            shutdownLatch.countDown();
        }, "shutdown"));

        server.awaitTermination();
        shutdownLatch.await();
    }

    private static TradeSource buildSource(String sourceType, Map<String, String> opts) {
        switch (sourceType) {
            case "simulated":
                return new SimulatedTradeSource();
            case "kafka": {
                String brokers = opts.getOrDefault("brokers", "localhost:9092");
                String topic = opts.getOrDefault("topic", "market.trades");
                String group = opts.getOrDefault("group", "market-data-core");
                DedupStore dedup = buildDedupStore(opts);
                LOGGER.info("kafka source configured: brokers=" + brokers + " topic=" + topic + " group=" + group);
                return new KafkaTradeSource(brokers, topic, group, dedup);
            }
            default:
                throw new IllegalArgumentException("unknown --source: " + sourceType + " (expected simulated|kafka)");
        }
    }

    private static DedupStore buildDedupStore(Map<String, String> opts) {
        String dedup = opts.getOrDefault("dedup", "memory");
        long ttlMs = Long.parseLong(opts.getOrDefault("dedup-ttl-ms", "300000"));
        switch (dedup) {
            case "memory":
                return new InMemoryDedupStore(ttlMs, 100_000);
            case "redis": {
                String addr = opts.getOrDefault("redis-addr", "localhost:6379");
                LOGGER.info("redis dedup configured: addr=" + addr + " ttl_ms=" + ttlMs);
                return RedisDedupStore.fromAddress(addr, ttlMs, 8);
            }
            default:
                throw new IllegalArgumentException("unknown --dedup: " + dedup + " (expected redis|memory)");
        }
    }

    /** Parse args no formato {@code --k v} (ou {@code --k=v}). */
    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> opts = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                continue;
            }
            String body = arg.substring(2);
            int eq = body.indexOf('=');
            if (eq >= 0) {
                opts.put(body.substring(0, eq), body.substring(eq + 1));
            } else if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                opts.put(body, args[++i]);
            } else {
                opts.put(body, "true");
            }
        }
        // compat: --port 50051 continua válido (era o único flag da Fase 1)
        String sysProp = System.getProperty("port");
        if (sysProp != null) {
            opts.putIfAbsent("port", sysProp);
        }
        return opts;
    }
}
