package co.vexo.server;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The vexo-sdk-conformance driver: reads a scenario as JSON on stdin, runs its steps against the SDK, and prints JSON
 * lines on stdout. In the SDK's package only to reuse its internal JSON reader and writer; it uses the public API.
 * See the README's "Conformance" section for how to build and run it.
 */
public final class ConformanceDriver {
    public static void main(String[] args) throws Exception {
        Map<?, ?> scenario = (Map<?, ?>) Json.parse(new String(System.in.readAllBytes(), StandardCharsets.UTF_8));
        Map<?, ?> options = (Map<?, ?>) scenario.get("options");
        Vexo.Builder builder = Vexo.builder((String) scenario.get("apiKey"))
                .host((String) scenario.get("host"))
                .onError(error -> print("type", "error", "message", error.getMessage(),
                        "status", error.getStatus() == 0 ? null : error.getStatus()));
        if (options.containsKey("flushAt")) {
            builder.flushAt(number(options, "flushAt").intValue());
        }
        if (options.containsKey("flushIntervalMs")) {
            builder.flushInterval(Duration.ofMillis(number(options, "flushIntervalMs").longValue()));
        }
        if (options.containsKey("maxRetries")) {
            builder.maxRetries(number(options, "maxRetries").intValue());
        }
        if (options.containsKey("maxQueueSize")) {
            builder.maxQueueSize(number(options, "maxQueueSize").intValue());
        }
        if (options.containsKey("timeoutMs")) {
            builder.timeout(Duration.ofMillis(number(options, "timeoutMs").longValue()));
        }
        if (options.containsKey("shutdownTimeoutMs")) {
            builder.shutdownTimeout(Duration.ofMillis(number(options, "shutdownTimeoutMs").longValue()));
        }
        Vexo vexo = builder.build();

        for (Object item : (List<?>) scenario.get("steps")) {
            Map<?, ?> step = (Map<?, ?>) item;
            String op = (String) step.get("op");
            long started = System.nanoTime();
            switch (op) {
                case "capture":
                    vexo.capture((String) step.get("distinctId"), (String) step.get("event"), properties(step),
                            timestamp(step), (String) step.get("messageId"));
                    break;
                case "identify":
                    vexo.identify((String) step.get("distinctId"), map(step.get("traits")));
                    break;
                case "flush":
                    vexo.flush();
                    break;
                case "shutdown":
                    vexo.close();
                    break;
                case "sleep":
                    Thread.sleep(number(step, "ms").longValue());
                    break;
                case "exit":
                    Runtime.getRuntime().halt(0);
                    break;
                default:
                    throw new IllegalArgumentException("unknown step " + op);
            }
            print("type", "step", "op", op, "ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
        }
        // Return without close(): the exit_hook scenario checks that the shutdown hook sends the queue.
    }

    private static Map<String, Object> properties(Map<?, ?> step) {
        Map<String, Object> properties = map(step.get("properties"));
        if (Boolean.TRUE.equals(step.get("poison"))) {
            properties = properties == null ? new LinkedHashMap<>() : properties;
            // The SDK's JSON writer refuses a map that contains itself.
            Map<String, Object> circular = new LinkedHashMap<>();
            circular.put("self", circular);
            properties.put("poison", circular);
        }
        if (Boolean.TRUE.equals(step.get("nan"))) {
            properties = properties == null ? new LinkedHashMap<>() : properties;
            properties.put("nan", Double.NaN);
        }
        if (step.get("dateTime") != null) {
            properties = properties == null ? new LinkedHashMap<>() : properties;
            properties.put("dateTime", OffsetDateTime.parse((String) step.get("dateTime")));
        }
        if (step.get("decimal") != null) {
            properties = properties == null ? new LinkedHashMap<>() : properties;
            properties.put("decimal", new BigDecimal((String) step.get("decimal")));
        }
        if (Boolean.TRUE.equals(step.get("invalidUnicode"))) {
            properties = properties == null ? new LinkedHashMap<>() : properties;
            properties.put("invalidUnicode", "ok\uD800"); // a lone surrogate
        }
        return properties;
    }

    private static Instant timestamp(Map<?, ?> step) {
        if (step.get("at") != null) {
            return OffsetDateTime.parse((String) step.get("at")).toInstant();
        }
        return step.get("timestamp") == null ? null : Instant.ofEpochMilli(number(step, "timestamp").longValue());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value) {
        return value == null ? null : new LinkedHashMap<>((Map<String, Object>) value);
    }

    private static Number number(Map<?, ?> map, String key) {
        return (Number) map.get(key);
    }

    // One JSON line per call, flushed at once; the error callback can run on the background thread.
    private static synchronized void print(Object... keysAndValues) {
        Map<String, Object> line = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            line.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        System.out.println(Json.write(line));
        System.out.flush();
    }
}
