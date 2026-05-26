package ru.lab08.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;

public class Lab08StreamJob {
    private static final DateTimeFormatter CLICKHOUSE_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter CANCEL_IN = DateTimeFormatter.ofPattern("yyyy MMM dd HH:mm", Locale.ENGLISH);

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka.ijklmn.xyz:9092");
        String topic = env("KAFKA_TOPIC", "lab08_transactions");
        String groupId = env("KAFKA_GROUP_ID", "lab08-flink-http-rt");
        String startupMode = env("KAFKA_STARTUP_MODE", "latest-offset");
        int parallelism = Integer.parseInt(env("FLINK_PARALLELISM", "1"));

        StreamExecutionEnvironment flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        flinkEnv.setParallelism(parallelism);
        flinkEnv.enableCheckpointing(30_000L);

        OffsetsInitializer offsets = "earliest-offset".equalsIgnoreCase(startupMode)
                ? OffsetsInitializer.earliest()
                : OffsetsInitializer.latest();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(offsets)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> raw = flinkEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-lab08-events");

        raw.flatMap(new TransactionMapper())
                .name("map-rt-transactions")
                .addSink(new ClickHouseJsonEachRowSink("analytics.rt_transactions"))
                .name("sink-clickhouse-rt-transactions");

        raw.flatMap(new CancellationMapper())
                .name("map-rt-cancellations")
                .addSink(new ClickHouseJsonEachRowSink("analytics.rt_cancellations"))
                .name("sink-clickhouse-rt-cancellations");

        raw.flatMap(new ExchangeRateMapper())
                .name("map-rt-exchange-rates")
                .addSink(new ClickHouseJsonEachRowSink("analytics.rt_exchange_rates"))
                .name("sink-clickhouse-rt-exchange-rates");

        flinkEnv.execute("lab08-kafka-to-clickhouse-rt");
    }

    public static class TransactionMapper implements FlatMapFunction<String, String> {
        private transient ObjectMapper mapper;

        @Override
        public void flatMap(String value, Collector<String> out) {
            try {
                ObjectMapper m = mapper();
                JsonNode n = m.readTree(value);
                if (!"transaction".equals(text(n, "_source"))) return;

                Long transactionId = longVal(n, "transaction_id");
                Long createdAt = longVal(n, "created_at");
                if (transactionId == null || createdAt == null) return;

                BigDecimal amount = decimal(n, "amount", BigDecimal.ZERO);
                String currency = text(n, "currency");
                String transactionType = text(n, "transaction_type");
                String status = text(n, "status");
                String userUuid = nullableText(n, "user_uuid");
                Long userId = longVal(n, "user_id");
                Long promoCodeId = longVal(n, "promo_code_id");
                String publishedAtRaw = nullableText(n, "published_at");

                String eventId = md5(join(transactionId, createdAt, amount, currency, transactionType, status, publishedAtRaw));

                ObjectNode o = m.createObjectNode();
                o.put("event_id", eventId);
                o.put("transaction_id", transactionId);
                putNullableLong(o, "user_id", userId);
                putNullableText(o, "user_uuid", userUuid);
                o.put("amount", amount);
                o.put("currency", currency);
                o.put("transaction_type", transactionType);
                putNullableLong(o, "promo_code_id", promoCodeId);
                o.put("status", status);
                o.put("created_at", epochSecondsToCh(createdAt));
                o.put("source", "kafka");
                o.put("loaded_at", nowCh());
                putNullableText(o, "published_at", parsePublishedAt(publishedAtRaw));
                o.put("transaction_dup_rn", 1);
                o.put("transaction_dup_cnt", 1);
                o.put("is_transaction_duplicate", 0);
                o.put("is_empty_user", (userUuid == null || userUuid.isBlank()) ? 1 : 0);
                o.put("is_negative_amount", amount.compareTo(BigDecimal.ZERO) < 0 ? 1 : 0);
                o.put("is_zero_amount", amount.compareTo(BigDecimal.ZERO) == 0 ? 1 : 0);
                o.put("is_completed", "completed".equals(status) ? 1 : 0);
                o.put("is_purchase", "purchase".equals(transactionType) ? 1 : 0);
                out.collect(m.writeValueAsString(o));
            } catch (Exception ignored) {
            }
        }

        private ObjectMapper mapper() {
            if (mapper == null) mapper = new ObjectMapper();
            return mapper;
        }
    }

    public static class CancellationMapper implements FlatMapFunction<String, String> {
        private transient ObjectMapper mapper;

        @Override
        public void flatMap(String value, Collector<String> out) {
            try {
                ObjectMapper m = mapper();
                JsonNode n = m.readTree(value);
                if (!"cancellation".equals(text(n, "_source"))) return;

                Long originalTransactionId = longVal(n, "original_transaction_id");
                String cancelledAtRaw = text(n, "cancelled_at");
                String cancelledAt = parseCancelledAt(cancelledAtRaw);
                if (originalTransactionId == null || cancelledAt == null) return;

                BigDecimal refundAmount = decimal(n, "refund_amount", BigDecimal.ZERO);
                String reason = text(n, "reason");
                String publishedAtRaw = nullableText(n, "published_at");

                String eventId = md5(join(originalTransactionId, reason, cancelledAtRaw, refundAmount, publishedAtRaw));

                ObjectNode o = m.createObjectNode();
                o.put("event_id", eventId);
                o.put("original_transaction_id", originalTransactionId);
                o.put("reason", reason);
                o.put("cancelled_at", cancelledAt);
                o.put("refund_amount", refundAmount);
                o.put("source", "kafka");
                o.put("loaded_at", nowCh());
                putNullableText(o, "published_at", parsePublishedAt(publishedAtRaw));
                out.collect(m.writeValueAsString(o));
            } catch (Exception ignored) {
            }
        }

        private ObjectMapper mapper() {
            if (mapper == null) mapper = new ObjectMapper();
            return mapper;
        }
    }

    public static class ExchangeRateMapper implements FlatMapFunction<String, String> {
        private transient ObjectMapper mapper;

        @Override
        public void flatMap(String value, Collector<String> out) {
            try {
                ObjectMapper m = mapper();
                JsonNode n = m.readTree(value);
                if (!"exchange_rate".equals(text(n, "_source"))) return;

                Long updateId = longVal(n, "update_id");
                Long timestamp = longVal(n, "timestamp");
                if (updateId == null || timestamp == null) return;

                String ts = epochSecondsToCh(timestamp);
                String publishedAtRaw = nullableText(n, "published_at");
                String publishedAt = parsePublishedAt(publishedAtRaw);

                emitRate(m, out, updateId, timestamp, ts, "PUNK", decimal(n, "rate_tgrk_punk", BigDecimal.ZERO), publishedAt);
                emitRate(m, out, updateId, timestamp, ts, "RUB", decimal(n, "rate_tgrk_rub", BigDecimal.ZERO), publishedAt);
            } catch (Exception ignored) {
            }
        }

        private void emitRate(ObjectMapper m, Collector<String> out, Long updateId, Long timestamp, String ts,
                              String currency, BigDecimal rate, String publishedAt) throws Exception {
            String eventId = md5(join(updateId, timestamp, currency, rate, publishedAt));
            ObjectNode o = m.createObjectNode();
            o.put("event_id", eventId);
            o.put("update_id", updateId);
            o.put("ts", ts);
            o.put("currency", currency);
            o.put("rate", rate);
            o.put("source", "kafka");
            o.put("loaded_at", nowCh());
            putNullableText(o, "published_at", publishedAt);
            out.collect(m.writeValueAsString(o));
        }

        private ObjectMapper mapper() {
            if (mapper == null) mapper = new ObjectMapper();
            return mapper;
        }
    }

    public static class ClickHouseJsonEachRowSink extends RichSinkFunction<String> {
        private final String tableName;
        private transient HttpClient client;
        private transient List<String> buffer;
        private transient String url;
        private transient String user;
        private transient String password;
        private transient int batchSize;
        private transient long flushIntervalMs;
        private transient long lastFlushAtMs;

        public ClickHouseJsonEachRowSink(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public void open(Configuration parameters) {
            this.client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
            this.buffer = new ArrayList<>();
            this.url = env("CLICKHOUSE_HTTP_URL", "http://clickhouse:8123").replaceAll("/+$", "");
            this.user = env("CLICKHOUSE_USER", "default");
            this.password = env("CLICKHOUSE_PASSWORD", "");
            this.batchSize = Integer.parseInt(env("CLICKHOUSE_STREAM_BATCH_SIZE", "1000"));
            this.flushIntervalMs = Long.parseLong(env("CLICKHOUSE_STREAM_FLUSH_INTERVAL_MS", "2000"));
            this.lastFlushAtMs = System.currentTimeMillis();
        }

        @Override
        public synchronized void invoke(String value, Context context) throws Exception {
            buffer.add(value);
            long now = System.currentTimeMillis();
            if (buffer.size() >= batchSize || now - lastFlushAtMs >= flushIntervalMs) {
                flush();
            }
        }

        @Override
        public synchronized void close() throws Exception {
            flush();
        }

        private void flush() throws IOException, InterruptedException {
            if (buffer == null || buffer.isEmpty()) return;

            String query = "INSERT INTO " + tableName + " FORMAT JSONEachRow";
            String endpoint = url + "/?query=" + URLEncoder.encode(query, StandardCharsets.UTF_8);
            String body = String.join("\n", buffer) + "\n";

            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));

            if (user != null && !user.isBlank()) {
                requestBuilder.header("X-ClickHouse-User", user);
                requestBuilder.header("X-ClickHouse-Key", password == null ? "" : password);
            }

            HttpResponse<String> response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() >= 300) {
                throw new RuntimeException("ClickHouse insert failed for " + tableName + ": HTTP "
                        + response.statusCode() + " body=" + response.body());
            }

            buffer.clear();
            lastFlushAtMs = System.currentTimeMillis();
        }
    }

    private static String env(String name, String defaultValue) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? defaultValue : v;
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull()) return "";
        return v.asText("");
    }

    private static String nullableText(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull()) return null;
        String s = v.asText(null);
        if (s == null || s.isBlank()) return null;
        return s;
    }

    private static Long longVal(JsonNode n, String field) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull()) return null;
        if (v.isNumber()) return v.longValue();
        String s = v.asText(null);
        if (s == null || s.isBlank()) return null;
        return Long.parseLong(s);
    }

    private static BigDecimal decimal(JsonNode n, String field, BigDecimal defaultValue) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull()) return defaultValue;
        if (v.isNumber()) return v.decimalValue();
        String s = v.asText(null);
        if (s == null || s.isBlank()) return defaultValue;
        return new BigDecimal(s);
    }

    private static void putNullableLong(ObjectNode o, String field, Long value) {
        if (value == null) o.putNull(field);
        else o.put(field, value);
    }

    private static void putNullableText(ObjectNode o, String field, String value) {
        if (value == null) o.putNull(field);
        else o.put(field, value);
    }

    private static String epochSecondsToCh(long epochSeconds) {
        return CLICKHOUSE_TS.format(Instant.ofEpochSecond(epochSeconds));
    }

    private static String nowCh() {
        return CLICKHOUSE_TS.format(Instant.now());
    }

    private static String parseCancelledAt(String s) {
        if (s == null || s.isBlank()) return null;
        try {
            LocalDateTime ldt = LocalDateTime.parse(s, CANCEL_IN);
            return CLICKHOUSE_TS.format(ldt.toInstant(ZoneOffset.UTC));
        } catch (Exception e) {
            return null;
        }
    }

    private static String parsePublishedAt(String s) {
        if (s == null || s.isBlank()) return null;
        try {
            return CLICKHOUSE_TS.format(OffsetDateTime.parse(s).toInstant());
        } catch (Exception e) {
            try {
                return CLICKHOUSE_TS.format(Instant.parse(s));
            } catch (Exception ignored) {
                return null;
            }
        }
    }

    private static String join(Object... parts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) sb.append('|');
            sb.append(parts[i] == null ? "" : parts[i].toString());
        }
        return sb.toString();
    }

    private static String md5(String s) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(s.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digest);
    }
}
