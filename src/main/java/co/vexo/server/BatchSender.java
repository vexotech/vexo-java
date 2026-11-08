package co.vexo.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Delivers one batch to {@code POST /v1/batch}, or reports why it couldn't: retries with backoff, Retry-After, the
 * circuit breaker, and the per-message result. Network errors, 429 and 5xx are retried; so are the messages the API
 * reports as failed. Dropped messages are reported, never resent.
 */
final class BatchSender {
    static final long MAX_BACKOFF_NANOS = TimeUnit.SECONDS.toNanos(30);
    static final long MAX_RETRY_AFTER_NANOS = TimeUnit.SECONDS.toNanos(60);
    private static final byte[] BATCH_START = "{\"batch\":[".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BATCH_END = "]}".getBytes(StandardCharsets.UTF_8);

    private final HttpClient http;
    private final URI uri;
    private final String apiKey;
    private final String userAgent;
    private final long timeoutNanos;
    private final int maxRetries;
    private final Consumer<VexoException> onError;
    // Time left before the shutdown deadline: Long.MAX_VALUE until close() starts.
    private final LongSupplier remainingNanos;
    // Set by a batch that used up its retries: later batches get one attempt until a request succeeds.
    private volatile boolean circuitOpen;

    BatchSender(URI uri, String apiKey, String userAgent, Duration timeout, int maxRetries,
                Consumer<VexoException> onError, LongSupplier remainingNanos) {
        this.http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(timeout).build();
        this.uri = uri;
        this.apiKey = apiKey;
        this.userAgent = userAgent;
        this.timeoutNanos = timeout.toNanos();
        this.maxRetries = maxRetries;
        this.onError = onError;
        this.remainingNanos = remainingNanos;
    }

    /** Sends the batch; every failure goes to the error callback. Throws only when the thread is interrupted. */
    void send(List<Message> batch) throws InterruptedException {
        try {
            deliver(batch);
        } catch (VexoException e) {
            onError.accept(e);
        } catch (IOException | RuntimeException e) {
            onError.accept(new VexoException("vexo: batch failed: " + e, e, 0, batch));
        } catch (InterruptedException e) {
            onError.accept(new VexoException("vexo: interrupted while sending a batch", e, 0, batch));
            throw e;
        }
    }

    private void deliver(List<Message> batch) throws IOException, InterruptedException {
        int retries = circuitOpen ? 0 : maxRetries;
        for (int attempt = 0; ; attempt++) {
            HttpResponse<String> response;
            try {
                response = post(batch);
            } catch (IOException e) {
                if (attempt >= retries || remainingNanos.getAsLong() <= 0) {
                    circuitOpen = true;
                    throw e;
                }
                sleep(backoffNanos(attempt));
                continue;
            }
            boolean lastAttempt = attempt >= retries || remainingNanos.getAsLong() <= 0;
            int status = response.statusCode();
            if (status >= 200 && status < 300) {
                circuitOpen = false;
                Map<?, ?> data = data(response.body());
                reportResults(batch, results(data, "dropped", batch.size()), "the server dropped");
                List<Map<?, ?>> failed = results(data, "failed", batch.size());
                if (failed.isEmpty()) {
                    return;
                }
                if (lastAttempt) {
                    reportResults(batch, failed, "the server could not store");
                    return;
                }
                List<Message> retry = new ArrayList<>();
                for (Map<?, ?> result : failed) {
                    retry.add(batch.get(index(result)));
                }
                batch = retry;
                sleep(backoffNanos(attempt));
                continue;
            }
            boolean retryable = status == 429 || status >= 500;
            if (!retryable || lastAttempt) {
                if (retryable) {
                    circuitOpen = true;
                }
                throw new VexoException("vexo: batch rejected with HTTP " + status + ": " + response.body(), null,
                        status, batch);
            }
            sleep(retryAfterNanos(response.headers().firstValue("Retry-After").orElse(null))
                    .orElse(backoffNanos(attempt)));
        }
    }

    private HttpResponse<String> post(List<Message> batch) throws IOException, InterruptedException {
        // Never wait past the shutdown deadline, and never pass a zero timeout, which HttpRequest rejects.
        long requestTimeout = Math.max(TimeUnit.MILLISECONDS.toNanos(1), Math.min(timeoutNanos, remainingNanos.getAsLong()));
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofNanos(requestTimeout))
                .header("Content-Type", "application/json")
                .header("X-Api-Key", apiKey)
                .header("User-Agent", userAgent)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body(batch)))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    // {"batch":[...]} from the JSON each message got at enqueue, so a retry sends the same bytes.
    static byte[] body(List<Message> batch) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(BATCH_START);
        for (int i = 0; i < batch.size(); i++) {
            if (i > 0) {
                out.write(',');
            }
            out.writeBytes(batch.get(i).json);
        }
        out.writeBytes(BATCH_END);
        return out.toByteArray();
    }

    private void reportResults(List<Message> batch, List<Map<?, ?>> results, String what) {
        if (results.isEmpty()) {
            return;
        }
        List<VexoException.Dropped> dropped = new ArrayList<>();
        for (Map<?, ?> result : results) {
            dropped.add(new VexoException.Dropped(batch.get(index(result)).fields, String.valueOf(result.get("reason"))));
        }
        onError.accept(new VexoException(String.format("vexo: %s %d message(s): %s", what, dropped.size(),
                dropped.get(0).getReason()), dropped));
    }

    // The response envelope's data object: {"success":true,"code":200,"data":{"accepted":1,"dropped":[],"failed":[]}}.
    // A 2xx body that isn't that counts as a success with nothing dropped or failed.
    static Map<?, ?> data(String body) {
        Object envelope;
        try {
            envelope = Json.parse(body);
        } catch (IllegalArgumentException e) {
            return Map.of();
        }
        Object data = envelope instanceof Map ? ((Map<?, ?>) envelope).get("data") : null;
        return data instanceof Map ? (Map<?, ?>) data : Map.of();
    }

    // The {index, reason} entries of data[key] whose index is a position in the batch.
    private static List<Map<?, ?>> results(Map<?, ?> data, String key, int batchSize) {
        List<Map<?, ?>> results = new ArrayList<>();
        Object entries = data.get(key);
        if (entries instanceof List) {
            for (Object entry : (List<?>) entries) {
                if (entry instanceof Map && ((Map<?, ?>) entry).get("index") instanceof Number) {
                    double index = ((Number) ((Map<?, ?>) entry).get("index")).doubleValue();
                    if (index >= 0 && index < batchSize && index == Math.rint(index)) {
                        results.add((Map<?, ?>) entry);
                    }
                }
            }
        }
        return results;
    }

    private static int index(Map<?, ?> result) {
        return ((Number) result.get("index")).intValue();
    }

    private void sleep(long nanos) throws InterruptedException {
        TimeUnit.NANOSECONDS.sleep(Math.min(nanos, remainingNanos.getAsLong()));
    }

    // Exponential backoff with jitter: half of min(2^attempt seconds, 30 s), plus a random part of the other half.
    static long backoffNanos(int attempt) {
        long base = Math.min(TimeUnit.SECONDS.toNanos(1L << Math.min(attempt, 30)), MAX_BACKOFF_NANOS);
        return base / 2 + ThreadLocalRandom.current().nextLong(base / 2 + 1);
    }

    // Retry-After in seconds, capped at 60 s. Empty when absent or not a number of seconds (an HTTP date, for example).
    static OptionalLong retryAfterNanos(String header) {
        if (header == null) {
            return OptionalLong.empty();
        }
        try {
            double seconds = Double.parseDouble(header.trim());
            return seconds >= 0
                    ? OptionalLong.of((long) Math.min(seconds * 1e9, MAX_RETRY_AFTER_NANOS))
                    : OptionalLong.empty();
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
    }
}
