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
 * <p>O stream fica aberto indefinidamente; o cliente cancela para encerrar.
 * Backpressure é propagado pelo HTTP/2 flow control quando o consumidor é lento.
 */
public final class MarketDataGrpcService extends MarketDataServiceGrpc.MarketDataServiceImplBase {

    private final TradeSource source;

    public MarketDataGrpcService(TradeSource source) {
        this.source = Objects.requireNonNull(source, "source");
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

        // Serializa onNext: uma janela de agregação pode ser despachada por
        // outra thread que não a do TradeSource.
        Object writeLock = new Object();
        TradeAggregator aggregator = intervalMs > 0
                ? new TradeAggregator(symbol, intervalMs, trade -> emit(serverObserver, writeLock, trade))
                : null;

        TradeSource.Subscription subscription = source.subscribe(symbol, trade -> {
            if (aggregator != null) {
                aggregator.accept(trade);
            } else {
                emit(serverObserver, writeLock, trade);
            }
        });

        serverObserver.setOnCancelHandler(() -> {
            subscription.close();
            if (aggregator != null) {
                aggregator.close();
            }
        });
        serverObserver.setOnReadyHandler(() -> { /* backpressure é aplicado pelo HTTP/2 flow control */ });
    }

    private static void emit(ServerCallStreamObserver<TickerResponse> observer, Object lock, TickerResponse trade) {
        synchronized (lock) {
            if (observer.isCancelled()) {
                return;
            }
            try {
                observer.onNext(trade);
            } catch (RuntimeException e) {
                // Cliente desconectou entre o isCancelled() e o onNext(); nada a fazer.
            }
        }
    }
}
