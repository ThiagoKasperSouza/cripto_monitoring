package com.financial.marketdata.core;

import com.financial.marketdata.core.source.TradeSource;
import com.financial.marketdata.v1.MarketDataServiceGrpc;
import com.financial.marketdata.v1.TickerRequest;
import com.financial.marketdata.v1.TickerResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.grpc.stub.ServerCallStreamObserver;

import java.util.Locale;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * Implementação do {@code MarketDataService.StreamTicker} (Server-Side
 * Streaming sobre gRPC/HTTP/2).
 *
 * <p>Comportamento:
 * <ul>
 *   <li>{@code symbol} vazio/ausente → {@code INVALID_ARGUMENT};</li>
 *   <li>{@code aggregation_interval_ms < 0} → {@code INVALID_ARGUMENT};</li>
 *   <li>{@code aggregation_interval_ms = 0} (default) → cada trade é repassado imediatamente;</li>
 *   <li>{@code aggregation_interval_ms &gt; 0} → trades são agregados em janelas
 *       (VWAP) e emitidos ao fim de cada janela.</li>
 * </ul>
 *
 * <p>Backpressure: cada assinatura tem uma fila limitada
 * ({@value StreamSession#DEFAULT_CAPACITY} por default). Quando o cliente não
 * drena rápido o bastante e a fila enche, o cliente é desconectado com
 * {@code RESOURCE_EXHAUSTED} e a subscription para. O consumidor nunca trava;
 * clientes lentos são isolados — nunca há descarte silencioso nem vazamento
 * de trades. A drenagem obedece o HTTP/2 flow control ({@code isReady()}).
 */
public final class MarketDataGrpcService extends MarketDataServiceGrpc.MarketDataServiceImplBase {

    private static final Logger LOGGER = Logger.getLogger(MarketDataGrpcService.class.getName());

    private final TradeSource source;
    private final int queueCapacity;

    public MarketDataGrpcService(TradeSource source) {
        this(source, StreamSession.DEFAULT_CAPACITY);
    }

    /** Capacidade da fila por assinatura — overload para testes. */
    public MarketDataGrpcService(TradeSource source, int queueCapacity) {
        this.source = Objects.requireNonNull(source, "source");
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queueCapacity must be > 0");
        }
        this.queueCapacity = queueCapacity;
    }

    @Override
    public void streamTicker(TickerRequest request, StreamObserver<TickerResponse> responseObserver) {
        String symbol = request.getSymbol() == null ? "" : request.getSymbol().trim().toUpperCase(Locale.ROOT);
        if (symbol.isEmpty()) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription("symbol must not be empty").asRuntimeException());
            return;
        }
        int intervalMs = request.getAggregationIntervalMs();
        if (intervalMs < 0) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription("aggregation_interval_ms must be >= 0").asRuntimeException());
            return;
        }

        ServerCallStreamObserver<TickerResponse> serverObserver =
                (ServerCallStreamObserver<TickerResponse>) responseObserver;
        StreamSession session = new StreamSession(serverObserver, queueCapacity);

        TradeAggregator aggregator = intervalMs > 0
                ? new TradeAggregator(symbol, intervalMs, session::enqueue)
                : null;

        TradeSource.Subscription subscription = source.subscribe(symbol, trade -> {
            if (aggregator != null) {
                aggregator.accept(trade);
            } else {
                session.enqueue(trade);
            }
        });

        // Handlers antes de liberar tráfego; teardown libera subscription+aggregator
        session.setOnTeardown(() -> {
            subscription.close();
            if (aggregator != null) {
                aggregator.close();
            }
        });
        serverObserver.setOnCancelHandler(() -> {
            subscription.close();
            if (aggregator != null) {
                aggregator.close();
            }
            session.close();
            LOGGER.info("client cancelled stream for " + symbol);
        });
        serverObserver.setOnReadyHandler(session::drain);

        // Se o cliente já estiver pronto, drena imediatamente o que já estiver enfileirado
        session.drain();
    }
}
