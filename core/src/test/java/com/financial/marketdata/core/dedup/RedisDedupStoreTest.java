package com.financial.marketdata.core.dedup;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Testa o RedisDedupStore contra um Redis real. Requer REDIS_ADDR no
 * ambiente (ex: REDIS_ADDR=localhost:6379). Sem a variável, os testes são
 * PULADOS — CI não sobe Redis (ver .github/workflows/ci.yml).
 */
class RedisDedupStoreTest {

    private static final String REDIS_ADDR = System.getenv("REDIS_ADDR");

    private static RedisDedupStore store(long ttlMillis) {
        return RedisDedupStore.fromAddress(REDIS_ADDR, ttlMillis, 4);
    }

    @Test
    void putIfAbsentSemanticsAndScopePerKey() {
        assumeTrue(REDIS_ADDR != null && !REDIS_ADDR.isBlank(), "REDIS_ADDR not set");
        String keyBase = "test:" + System.nanoTime() + ":";
        try (RedisDedupStore store = store(60_000)) {
            assertTrue(store.markProcessed(keyBase + "1"));
            assertFalse(store.markProcessed(keyBase + "1"), "duplicate must be rejected");
            assertTrue(store.markProcessed(keyBase + "2"), "different key must be accepted");
        }
    }

    @Test
    void keysExpireAfterTtl() throws Exception {
        assumeTrue(REDIS_ADDR != null && !REDIS_ADDR.isBlank(), "REDIS_ADDR not set");
        String key = "test:" + System.nanoTime() + ":ttl";
        try (RedisDedupStore store = store(500)) {
            assertTrue(store.markProcessed(key));
            assertFalse(store.markProcessed(key));
            Thread.sleep(800); // TTL 500ms + folga
            assertTrue(store.markProcessed(key), "expired key must be insertable again");
        }
    }
}
