package com.financial.marketdata.core;

import com.financial.marketdata.core.source.SimulatedTradeSource;
import com.financial.marketdata.core.source.TradeSource;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

/**
 * Entry point do serviço core de mercado (Camada 3 — gRPC sobre HTTP/2).
 *
 * <p>Uso:
 * <pre>{@code
 *   mvn -q package
 *   java -jar target/core-1.0.0-SNAPSHOT.jar [--port 50051]
 * }</pre>
 *
 * <p>Fase 1: servidor sobe contra o {@link SimulatedTradeSource} (dados
 * sintéticos, apenas para desenvolvimento). Fase 2 substituirá pela
 * {@code KafkaTradeSource} sem mudar o contrato gRPC nem este entry point.
 */
public final class MarketDataServer {

    private static final Logger LOGGER = Logger.getLogger(MarketDataServer.class.getName());

    private MarketDataServer() {}

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = parsePort(args);

        TradeSource source = new SimulatedTradeSource();
        source.start();

        Server server = NettyServerBuilder.forPort(port)
                .addService(new MarketDataGrpcService(source))
                .build()
                .start();

        LOGGER.info("core service started on gRPC port " + port + " (source=SimulatedTradeSource, Fase 1)");

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

    private static int parsePort(String[] args) {
        // suporta: --port 50051 | -Dport=50051 | env PORT
        for (int i = 0; i < args.length - 1; i++) {
            if ("--port".equals(args[i])) {
                return Integer.parseInt(args[i + 1]);
            }
        }
        String sysProp = System.getProperty("port");
        if (sysProp != null) {
            return Integer.parseInt(sysProp);
        }
        String env = System.getenv("PORT");
        if (env != null && !env.isBlank()) {
            return Integer.parseInt(env.trim());
        }
        return 50051;
    }
}
