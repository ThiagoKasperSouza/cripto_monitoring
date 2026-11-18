package com.financial.marketdata.core.source;

import com.financial.marketdata.v1.TickerResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class SimulatedTradeSourceTest {

    private SimulatedTradeSource source;

    @BeforeEach
    void setUp() {
        source = new SimulatedTradeSource(10, new Random(42)); // 10ms, seed fixa
        source.start();
    }

    @AfterEach
    void tearDown() {
        source.close();
    }

    @Test
    void emitsTradesWithMonotonicTradeIds() throws Exception {
        List<TickerResponse> received = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(5);
        source.subscribe("BTCUSDT", t -> { received.add(t); latch.countDown(); });

        assertTrue(latch.await(5, TimeUnit.SECONDS), "expected 5 trades within 5s");
        long last = -1;
        for (TickerResponse t : received) {
            assertEquals("BTCUSDT", t.getSymbol());
            assertTrue(t.getTradeId() > last, "trade_id must increase");
            last = t.getTradeId();
            assertTrue(new BigDecimal(t.getPrice()).signum() > 0);
            assertTrue(new BigDecimal(t.getQuantity()).signum() > 0);
            assertTrue(t.getEventTime() > 0);
        }
    }

    @Test
    void unsubscribedListenerStopsReceiving() throws Exception {
        List<TickerResponse> received = new CopyOnWriteArrayList<>();
        var subscription = source.subscribe("BTCUSDT", received::add);
        Thread.sleep(100);
        subscription.close();
        int countAtUnsubscribe = received.size();
        Thread.sleep(200);
        assertEquals(countAtUnsubscribe, received.size(), "no trades expected after unsubscribe");
    }

    @Test
    void multipleSymbolsAreIsolatedPerListener() throws Exception {
        List<TickerResponse> btc = new CopyOnWriteArrayList<>();
        List<TickerResponse> eth = new CopyOnWriteArrayList<>();
        source.subscribe("BTCUSDT", btc::add);
        source.subscribe("ETHUSDT", eth::add);

        Thread.sleep(250);
        assertFalse(btc.isEmpty());
        assertFalse(eth.isEmpty());
        assertTrue(btc.stream().allMatch(t -> t.getSymbol().equals("BTCUSDT")));
        assertTrue(eth.stream().allMatch(t -> t.getSymbol().equals("ETHUSDT")));
    }

    @Test
    void closeStopsEmission() throws Exception {
        List<TickerResponse> received = new CopyOnWriteArrayList<>();
        source.subscribe("BTCUSDT", received::add);
        source.close();
        int countAtClose = received.size();
        Thread.sleep(150);
        assertEquals(countAtClose, received.size());
    }
}
