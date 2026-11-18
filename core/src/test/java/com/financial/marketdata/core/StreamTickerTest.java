package com.financial.marketdata.core;

import com.financial.marketdata.core.source.SimulatedTradeSource;
import com.financial.marketdata.v1.MarketDataServiceGrpc;
import com.financial.marketdata.v1.TickerRequest;
import com.financial.marketdata.v1.TickerResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class StreamTickerTest {

    private SimulatedTradeSource source;
    private Server server;
    private ManagedChannel channel;

    @BeforeEach
    void setUp() throws Exception {
        source = new SimulatedTradeSource(10, new Random(42)); // 10ms, seed fixa -> determinístico
        source.start();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .addService(new MarketDataGrpcService(source))
                .directExecutor()
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name)
                .directExecutor()
                .build();
    }

    @AfterEach
    void tearDown() throws Exception {
        channel.shutdownNow();
        server.shutdownNow();
        source.close();
    }

    @Test
    void streamsTradesForSymbol() throws Exception {
        MarketDataServiceGrpc.MarketDataServiceStub stub = MarketDataServiceGrpc.newStub(channel);
        List<TickerResponse> received = new ArrayList<>();

        stub.streamTicker(TickerRequest.newBuilder().setSymbol("BTCUSDT").build(),
                new StreamObserver<TickerResponse>() {
                    @Override public void onNext(TickerResponse value) { received.add(value); }
                    @Override public void onError(Throwable t) { t.printStackTrace(); }
                    @Override public void onCompleted() {}
                });

        Thread.sleep(300); // ~30 trades a 10ms
        assertFalse(received.isEmpty(), "expected trades received within 300ms");
        assertTrue(received.stream().allMatch(t -> t.getSymbol().equals("BTCUSDT")));
        assertTrue(received.stream().allMatch(t -> new java.math.BigDecimal(t.getPrice()).signum() > 0));
        long last = -1;
        for (TickerResponse t : received) {
            assertTrue(t.getTradeId() > last, "trade_id must be monotonically increasing");
            last = t.getTradeId();
        }
    }

    @Test
    void rejectsEmptySymbol() {
        MarketDataServiceGrpc.MarketDataServiceStub stub = MarketDataServiceGrpc.newStub(channel);
        CountDownLatch errorLatch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        stub.streamTicker(TickerRequest.newBuilder().setSymbol("  ").build(),
                new StreamObserver<TickerResponse>() {
                    @Override public void onNext(TickerResponse value) {}
                    @Override public void onError(Throwable t) { error.set(t); errorLatch.countDown(); }
                    @Override public void onCompleted() {}
                });

        assertDoesNotThrow(() -> assertTrue(errorLatch.await(2, TimeUnit.SECONDS)));
        assertInstanceOf(StatusRuntimeException.class, error.get());
        assertEquals(Status.INVALID_ARGUMENT.getCode(),
                ((StatusRuntimeException) error.get()).getStatus().getCode());
    }

    @Test
    void rejectsNegativeAggregationInterval() {
        MarketDataServiceGrpc.MarketDataServiceStub stub = MarketDataServiceGrpc.newStub(channel);
        CountDownLatch errorLatch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        stub.streamTicker(TickerRequest.newBuilder().setSymbol("ETHUSDT").setAggregationIntervalMs(-1).build(),
                new StreamObserver<TickerResponse>() {
                    @Override public void onNext(TickerResponse value) {}
                    @Override public void onError(Throwable t) { error.set(t); errorLatch.countDown(); }
                    @Override public void onCompleted() {}
                });

        assertDoesNotThrow(() -> assertTrue(errorLatch.await(2, TimeUnit.SECONDS)));
        assertEquals(Status.INVALID_ARGUMENT.getCode(),
                ((StatusRuntimeException) error.get()).getStatus().getCode());
    }

    @Test
    void aggregationIntervalEmitsVwapBuckets() throws Exception {
        MarketDataServiceGrpc.MarketDataServiceStub stub = MarketDataServiceGrpc.newStub(channel);
        List<TickerResponse> received = new ArrayList<>();

        stub.streamTicker(
                TickerRequest.newBuilder().setSymbol("SOLUSDT").setAggregationIntervalMs(100).build(),
                new StreamObserver<TickerResponse>() {
                    @Override public void onNext(TickerResponse value) { received.add(value); }
                    @Override public void onError(Throwable t) { t.printStackTrace(); }
                    @Override public void onCompleted() {}
                });

        Thread.sleep(550); // ~5 janelas de 100ms, trades a cada 10ms
        assertFalse(received.isEmpty(), "expected at least one aggregated bucket");
        for (TickerResponse bucket : received) {
            assertEquals("SOLUSDT", bucket.getSymbol());
            assertTrue(new java.math.BigDecimal(bucket.getPrice()).signum() > 0);
            assertTrue(new java.math.BigDecimal(bucket.getQuantity()).signum() > 0);
        }
    }
}
