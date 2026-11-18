package com.financial.marketdata.core;

import com.financial.marketdata.v1.TickerResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class StreamSessionTest {

    /** Fake determinístico do ServerCallStreamObserver para testes de sessão. */
    static final class FakeServerCallStreamObserver extends ServerCallStreamObserver<TickerResponse> {
        volatile boolean ready = false;
        volatile boolean cancelled = false;
        final List<TickerResponse> onNext = new CopyOnWriteArrayList<>();
        final List<Throwable> errors = new CopyOnWriteArrayList<>();
        volatile Runnable readyHandler;
        volatile Runnable cancelHandler;

        @Override public boolean isCancelled() { return cancelled; }
        @Override public void setOnCancelHandler(Runnable r) { this.cancelHandler = r; }
        @Override public void setCompression(String value) {}
        @Override public boolean isReady() { return ready; }
        @Override public void setOnReadyHandler(Runnable r) { this.readyHandler = r; }
        @Override public void disableAutoInboundFlowControl() {}
        @Override public void disableAutoRequest() {}
        @Override public void request(int count) {}
        @Override public void setMessageCompression(boolean enable) {}
        @Override public void onNext(TickerResponse value) { onNext.add(value); }
        @Override public void onError(Throwable t) { errors.add(t); cancelled = true; }
        @Override public void onCompleted() { cancelled = true; }

        void flipReady(boolean value) {
            ready = value;
            Runnable handler = readyHandler;
            if (value && handler != null) {
                handler.run();
            }
        }
    }

    private static TickerResponse trade(long id) {
        return TickerResponse.newBuilder()
                .setSymbol("BTCUSDT")
                .setPrice("100.0")
                .setQuantity("1")
                .setEventTime(1000 + id)
                .setTradeId(id)
                .build();
    }

    @Test
    void slowClientIsDisconnectedWithResourceExhausted() {
        FakeServerCallStreamObserver observer = new FakeServerCallStreamObserver();
        StreamSession session = new StreamSession(observer, 3);
        AtomicBoolean teardownRan = new AtomicBoolean(false);
        session.setOnTeardown(() -> teardownRan.set(true));

        session.enqueue(trade(1));
        session.enqueue(trade(2));
        session.enqueue(trade(3));
        assertEquals(3, session.pending());
        assertTrue(observer.onNext.isEmpty(), "client not ready: nothing drained");

        session.enqueue(trade(4)); // estoura a fila

        assertEquals(1, observer.errors.size());
        StatusRuntimeException err = assertInstanceOf(StatusRuntimeException.class, observer.errors.get(0));
        assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), err.getStatus().getCode());
        assertTrue(teardownRan.get(), "teardown must run to free the subscription");
        assertTrue(session.isClosed());
        assertTrue(observer.onNext.isEmpty(), "client must never receive a partial batch");
    }

    @Test
    void readyClientReceivesEverythingImmediatelyInOrder() {
        FakeServerCallStreamObserver observer = new FakeServerCallStreamObserver();
        observer.ready = true;
        StreamSession session = new StreamSession(observer, 10);

        session.enqueue(trade(1));
        session.enqueue(trade(2));
        session.enqueue(trade(3));

        assertEquals(List.of(1L, 2L, 3L), observer.onNext.stream().map(TickerResponse::getTradeId).toList());
        assertEquals(0, session.pending());
        assertTrue(observer.errors.isEmpty());
    }

    @Test
    void onReadyHandlerDrainsBacklogAfterWindowOpens() {
        FakeServerCallStreamObserver observer = new FakeServerCallStreamObserver();
        StreamSession session = new StreamSession(observer, 10);
        StreamSession wired = session; // session registers this::drain via service; emulate here
        observer.setOnReadyHandler(wired::drain);

        session.enqueue(trade(1));
        session.enqueue(trade(2));
        session.enqueue(trade(3));
        assertEquals(3, session.pending());

        observer.flipReady(true); // window abre → onReadyHandler drena

        assertEquals(List.of(1L, 2L, 3L), observer.onNext.stream().map(TickerResponse::getTradeId).toList());
        assertEquals(0, session.pending());
    }

    @Test
    void enqueueAfterDisconnectIsSilentlyIgnored() {
        FakeServerCallStreamObserver observer = new FakeServerCallStreamObserver();
        StreamSession session = new StreamSession(observer, 1);
        session.enqueue(trade(1));
        session.enqueue(trade(2)); // estoura → disconnect
        int errors = observer.errors.size();
        session.enqueue(trade(3)); // já desconectado
        assertEquals(errors, observer.errors.size());
        assertTrue(observer.onNext.isEmpty());
    }

    @Test
    void cancelledClientStopsReceivingOnNext() {
        FakeServerCallStreamObserver observer = new FakeServerCallStreamObserver();
        observer.ready = true;
        StreamSession session = new StreamSession(observer, 10);

        session.enqueue(trade(1));
        observer.cancelled = true; // cliente cancelou
        session.enqueue(trade(2));

        assertEquals(1, observer.onNext.size(), "must stop emitting after client cancel");
    }
}
