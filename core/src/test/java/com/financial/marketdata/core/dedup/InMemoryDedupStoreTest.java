package com.financial.marketdata.core.dedup;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryDedupStoreTest {

    @Test
    void firstInsertTrueDuplicateFalse() {
        try (DedupStore store = new InMemoryDedupStore(60_000, 1_000)) {
            assertTrue(store.markProcessed("BTCUSDT:1"));
            assertFalse(store.markProcessed("BTCUSDT:1"));
            assertTrue(store.markProcessed("BTCUSDT:2"));
            assertFalse(store.markProcessed("BTCUSDT:1"), "original key must stay deduplicated");
        }
    }

    @Test
    void keysAreScopedPerSymbol() {
        try (DedupStore store = new InMemoryDedupStore(60_000, 1_000)) {
            assertTrue(store.markProcessed("BTCUSDT:1"));
            assertTrue(store.markProcessed("ETHUSDT:1"), "same trade_id in another symbol is a different key");
        }
    }

    @Test
    void entriesExpireAfterTtl() throws Exception {
        try (DedupStore store = new InMemoryDedupStore(80, 1_000)) {
            assertTrue(store.markProcessed("BTCUSDT:42"));
            assertFalse(store.markProcessed("BTCUSDT:42"));
            Thread.sleep(200); // > TTL de 80ms
            assertTrue(store.markProcessed("BTCUSDT:42"), "expired key must be insertable again");
        }
    }

    @Test
    void evictionKeepsSizeUnderCap() {
        try (InMemoryDedupStore store = new InMemoryDedupStore(600_000, 10)) {
            for (int i = 1; i <= 100; i++) {
                store.markProcessed("BTCUSDT:" + i);
            }
            assertTrue(store.size() <= 10, "expected sliding window to cap size, got " + store.size());
            // Mais recentes continuam válidas
            assertFalse(store.markProcessed("BTCUSDT:100"));
        }
    }

    @Test
    void rejectsInvalidConfig() {
        assertThrows(IllegalArgumentException.class, () -> new InMemoryDedupStore(0, 10));
        assertThrows(IllegalArgumentException.class, () -> new InMemoryDedupStore(100, 0));
    }
}
