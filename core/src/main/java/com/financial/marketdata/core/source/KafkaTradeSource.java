package com.financial.marketdata.core.source;

import com.financial.marketdata.core.dedup.DedupStore;
import com.financial.marketdata.v1.TickerResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Fonte real de trades para a Fase 2: consome do tópico Kafka/Redpanda
 * (mensagens protobuf {@link TickerResponse}, chave = symbol) e deduplica por
 * {@code {symbol}:{trade_id}} via {@link DedupStore}.
 *
 * <p>Ordem de processamento:
 * <ol>
 *   <li>lê o record (chave = symbol);</li>
 *   <li>desserializa o protobuf (falha → descarta, conta {@link #parseErrors});</li>
 *   <li>dedup: chave já vista → descarta silenciosamente ({@link #duplicatesDropped()});</li>
 *   <li>despacha para os listeners do símbolo.</li>
 * </ol>
 *
 * <p>A conexão com o broker só é aberta em {@link #start()}; {@link #handleRecord}
 * é chamado pelo loop de poll e é package-private para testes unitários.
 */
public final class KafkaTradeSource implements TradeSource {

    private static final Logger LOGGER = Logger.getLogger(KafkaTradeSource.class.getName());

    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final DedupStore dedup;

    private final Map<String, List<Consumer<TickerResponse>>> listeners = new ConcurrentHashMap<>();

    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong duplicatesDropped = new AtomicLong();
    private final AtomicLong parseErrors = new AtomicLong();
    private final AtomicLong dispatched = new AtomicLong();

    private volatile boolean closed = false;
    private KafkaConsumer<String, byte[]> consumer;
    private Thread pollThread;

    public KafkaTradeSource(String bootstrapServers, String topic, String groupId, DedupStore dedup) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.dedup = dedup;
    }

    @Override
    public void start() {
        synchronized (this) {
            if (pollThread != null) return;

            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                    OffsetResetStrategy.EARLIEST.name().toLowerCase());
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(List.of(topic));

            pollThread = new Thread(this::pollLoop, "kafka-trade-source");
            pollThread.setDaemon(true);
            pollThread.start();
            LOGGER.info("KafkaTradeSource started: brokers=" + bootstrapServers
                    + " topic=" + topic + " group=" + groupId);
        }
    }

    private void pollLoop() {
        while (!closed && consumer != null) {
            try {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, byte[]> record : records) {
                    consumed.incrementAndGet();
                    handleRecord(record.key(), record.value());
                }
            } catch (Exception e) {
                if (!closed) {
                    LOGGER.warning("poll error: " + e);
                    sleepQuietly(500);
                }
            }
        }
    }

    /** Processa um record cru; retorna true se despacha para listeners. */
    boolean handleRecord(String key, byte[] value) {
        TickerResponse trade;
        try {
            trade = TickerResponse.parseFrom(value);
        } catch (Exception e) {
            parseErrors.incrementAndGet();
            LOGGER.warning("dropping record with unparseable protobuf payload: " + e);
            return false;
        }

        String symbol = (key != null && !key.isBlank()) ? key : trade.getSymbol();
        if (symbol.isBlank()) {
            parseErrors.incrementAndGet();
            LOGGER.warning("dropping record without symbol (key and payload both empty)");
            return false;
        }

        if (!dedup.markProcessed(symbol + ":" + trade.getTradeId())) {
            duplicatesDropped.incrementAndGet();
            return false;
        }

        List<Consumer<TickerResponse>> registered = listeners.get(symbol);
        if (registered != null) {
            for (Consumer<TickerResponse> listener : new ArrayList<>(registered)) {
                try {
                    listener.accept(trade);
                } catch (Throwable t) {
                    LOGGER.warning("listener failed for symbol " + symbol + ": " + t);
                }
            }
        }
        dispatched.incrementAndGet();
        return true;
    }

    @Override
    public Subscription subscribe(String symbol, Consumer<TickerResponse> listener) {
        listeners.computeIfAbsent(symbol, k -> new CopyOnWriteArrayList<>()).add(listener);
        return () -> {
            List<Consumer<TickerResponse>> registered = listeners.get(symbol);
            if (registered != null) {
                registered.remove(listener);
            }
        };
    }

    public long consumed() { return consumed.get(); }
    public long duplicatesDropped() { return duplicatesDropped.get(); }
    public long parseErrors() { return parseErrors.get(); }
    public long dispatched() { return dispatched.get(); }

    @Override
    public void close() {
        closed = true;
        Thread t = pollThread;
        if (t != null) {
            try {
                t.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (consumer != null) {
            try { consumer.wakeup(); } catch (Exception ignored) { /* já fechando */ }
            try { consumer.close(Duration.ofSeconds(2)); } catch (Exception e) { LOGGER.warning("consumer close: " + e); }
        }
        dedup.close();
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
