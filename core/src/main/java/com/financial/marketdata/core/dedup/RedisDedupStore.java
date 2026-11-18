package com.financial.marketdata.core.dedup;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.logging.Logger;

/**
 * Deduplicação via Redis com TTL — compartilhada entre nós consumidores.
 * Usa SET key 1 NX PX ttl (operação atômica).
 *
 * <p>Decisão de falha (documentada): se o Redis estiver indisponível, o
 * consumidor faz <b>fail-open</b> — aceita o evento e loga um warning — ao
 * invés de descartar todo o fluxo de mercado. A alternativa fail-closed
 * congelaria os dashboards; trade-offs de exactly-once exigiriam broker com
 * transações, fora do escopo desta fase. Se preferir fail-closed, mude a
 * política aqui e nos testes correspondentes.
 */
public final class RedisDedupStore implements DedupStore {

    private static final Logger LOGGER = Logger.getLogger(RedisDedupStore.class.getName());
    private static final String KEY_PREFIX = "dedup:trade:";

    private final JedisPool pool;
    private final long ttlMillis;
    private long lastWarnAtMillis = 0;

    public RedisDedupStore(String host, int port, long ttlMillis, int poolSize) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(poolSize);
        config.setMaxIdle(poolSize);
        config.setMinIdle(1);
        config.setBlockWhenExhausted(true);
        config.setMaxWait(Duration.ofMillis(250));
        this.pool = new JedisPool(config, host, port);
        this.ttlMillis = ttlMillis;
    }

    /** Conveniência para "host:port" (formato do flag --redis-addr). */
    public static RedisDedupStore fromAddress(String address, long ttlMillis, int poolSize) {
        String[] parts = address.split(":", 2);
        String host = parts[0].trim();
        int port = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 6379;
        return new RedisDedupStore(host, port, ttlMillis, poolSize);
    }

    @Override
    public boolean markProcessed(String key) {
        try (Jedis jedis = pool.getResource()) {
            String result = jedis.set(KEY_PREFIX + key, "1",
                    redis.clients.jedis.params.SetParams.setParams().nx().px(ttlMillis));
            return "OK".equals(result);
        } catch (Exception e) {
            warnThrottled(e);
            return true; // fail-open, ver javadoc da classe
        }
    }

    private void warnThrottled(Exception e) {
        long now = System.currentTimeMillis();
        if (now - lastWarnAtMillis > 5_000) { // 1 warning por 5s no máximo
            lastWarnAtMillis = now;
            LOGGER.warning("Redis dedup indisponível; aceitando evento sem deduplicar (fail-open): " + e);
        }
    }

    @Override
    public void close() {
        pool.close();
    }
}
