package com.financial.marketdata.core;

import com.financial.marketdata.v1.TickerResponse;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Sessão de stream com backpressure bounded por assinatura.
 *
 * <p>Política escolhida para consumidores lentos (audit-grade):
 * cada {@code StreamTicker} tem uma fila limitada; quando ela enche, o
 * cliente é desconectado com {@code RESOURCE_EXHAUSTED} ("subscriber too
 * slow") em vez de (a) descartar trades silenciosamente ou (b) travar o
 * consumidor Kafka para todos os demais clientes. O consumidor nunca trava:
 * {@code enqueue} é non-blocking e o gRPC flow-control para de remover
 * mensagens do Kafka apenas no sentido de que aquele cliente é removido do
 * feed — os outros símbolos/clientes continuam intactos.
 *
 * <p>A drenagem para o cliente obedece o HTTP/2 flow control via
 * {@link ServerCallStreamObserver#isReady()}: só emitimos quando o cliente
 * confirma janela disponível; o {@code onReadyHandler} drena o restante.
 */
final class StreamSession {

    static final int DEFAULT_CAPACITY = 1024;
    private static final Logger LOGGER = Logger.getLogger(StreamSession.class.getName());

    private final LinkedBlockingQueue<TickerResponse> queue;
    private final ServerCallStreamObserver<TickerResponse> observer;
    private final int capacity;
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Chamado quando o cliente é desconectado por lentidão (para derrubar subscription/aggregator). */
    private volatile Runnable onTeardown;

    /** Callback opcional chamado quando a sessão é desconectada por fila cheia (métricas). */
    private volatile Runnable onSlowClientDisconnect;

    StreamSession(ServerCallStreamObserver<TickerResponse> observer) {
        this(observer, DEFAULT_CAPACITY);
    }

    StreamSession(ServerCallStreamObserver<TickerResponse> observer, int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.observer = observer;
        this.capacity = capacity;
        this.queue = new LinkedBlockingQueue<>(capacity);
    }

    void setOnTeardown(Runnable onTeardown) {
        this.onTeardown = onTeardown;
    }

    /** Define um callback chamado exatamente uma vez quando o cliente é desconectado por fila cheia. */
    void setOnSlowClientDisconnect(Runnable onSlowClientDisconnect) {
        this.onSlowClientDisconnect = onSlowClientDisconnect;
    }

    /**
     * Enfileira o trade; drenagem oportunista acontece se o cliente estiver ready.
     * Nunca bloqueia a thread da fonte (poll do Kafka ou tick do simulador).
     */
    void enqueue(TickerResponse trade) {
        if (closed.get()) {
            return; // sessão já encerrada (cancel ou disconnect) — silêncio total
        }
        if (!queue.offer(trade)) {
            disconnectSlowClient();
            return;
        }
        drain();
    }

    /** Drena o máximo que o flow control do cliente permite. Thread-safe. */
    void drain() {
        if (closed.get()) {
            return;
        }
        synchronized (this) {
            while (observer.isReady() && !observer.isCancelled()) {
                TickerResponse trade = queue.poll();
                if (trade == null) {
                    return;
                }
                try {
                    observer.onNext(trade);
                } catch (RuntimeException e) {
                    LOGGER.warning("onNext failed; closing session: " + e);
                    closed.set(true);
                    queue.clear();
                    return;
                }
            }
        }
    }

    private void disconnectSlowClient() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        LOGGER.warning("client too slow: subscriber queue full (capacity=" + capacity
                + "); closing stream with RESOURCE_EXHAUSTED and tearing down subscription");
        try {
            observer.onError(Status.RESOURCE_EXHAUSTED
                    .withDescription("subscriber too slow (queue full, capacity=" + capacity + ")")
                    .asRuntimeException());
        } catch (RuntimeException e) {
            LOGGER.warning("error while closing slow client stream: " + e);
        } finally {
            queue.clear();
            Runnable teardown = onTeardown;
            if (teardown != null) {
                try {
                    teardown.run();
                } catch (Exception e) {
                    LOGGER.warning("teardown of slow client failed: " + e);
                }
            }
            Runnable slowClient = onSlowClientDisconnect;
            if (slowClient != null) {
                try {
                    slowClient.run();
                } catch (Exception e) {
                    LOGGER.warning("slow-client callback failed: " + e);
                }
            }
        }
    }

    /** Encerra sem notificar (cancel do lado cliente ou shutdown). */
    void close() {
        if (closed.compareAndSet(false, true)) {
            queue.clear();
        }
    }

    boolean isClosed() {
        return closed.get();
    }

    /** Mensagens pendentes na fila (para testes/diagnóstico). */
    int pending() {
        return queue.size();
    }
}
