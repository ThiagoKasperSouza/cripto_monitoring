package com.financial.marketdata.core.dedup;

import java.time.Clock;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Deduplicação em memória com janela deslizante (TTL + teto de entradas).
 * Adequada para desenvolvimento e testes — não é compartilhada entre nós.
 *
 * <p>Internamente cada entrada carrega um timestamp de inserção (para TTL) e
 * uma sequência monotônica (para FIFO determinístico no estouro do teto),
 * cobrindo o caso de muitas inserções no mesmo milissegundo.
 */
public final class InMemoryDedupStore implements DedupStore {

    private static final class Entry {
        final long insertedAt;
        final long seq;

        Entry(long insertedAt, long seq) {
            this.insertedAt = insertedAt;
            this.seq = seq;
        }
    }

    private final long ttlMillis;
    private final int maxEntries;
    private final Clock clock;
    private final ConcurrentHashMap<String, Entry> seen = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong();

    public InMemoryDedupStore(long ttlMillis, int maxEntries) {
        this(ttlMillis, maxEntries, Clock.systemUTC());
    }

    public InMemoryDedupStore(long ttlMillis, int maxEntries, Clock clock) {
        if (ttlMillis <= 0) throw new IllegalArgumentException("ttlMillis must be > 0");
        if (maxEntries <= 0) throw new IllegalArgumentException("maxEntries must be > 0");
        this.ttlMillis = ttlMillis;
        this.maxEntries = maxEntries;
        this.clock = clock;
    }

    @Override
    public boolean markProcessed(String key) {
        long now = clock.millis();
        evictExpired(now);
        Entry previous = seen.putIfAbsent(key, new Entry(now, sequence.incrementAndGet()));
        if (previous != null) {
            return false;
        }
        evictOverflow();
        return true;
    }

    public int size() {
        return seen.size();
    }

    private void evictExpired(long now) {
        Iterator<Map.Entry<String, Entry>> it = seen.entrySet().iterator();
        while (it.hasNext()) {
            if (now - it.next().getValue().insertedAt > ttlMillis) {
                it.remove();
            }
        }
    }

    /** Se estourar o teto, remove as entradas com menor sequência primeiro. */
    private void evictOverflow() {
        while (seen.size() > maxEntries) {
            long oldestSeq = Long.MAX_VALUE;
            String oldestKey = null;
            Entry oldestEntry = null;
            for (Map.Entry<String, Entry> e : seen.entrySet()) {
                if (e.getValue().seq < oldestSeq) {
                    oldestSeq = e.getValue().seq;
                    oldestKey = e.getKey();
                    oldestEntry = e.getValue();
                }
            }
            if (oldestKey != null && oldestEntry != null) {
                seen.remove(oldestKey, oldestEntry);
            } else {
                break;
            }
        }
    }

    @Override
    public void close() {
        seen.clear();
    }
}
