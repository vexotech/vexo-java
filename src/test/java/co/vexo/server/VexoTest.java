package co.vexo.server;

import co.vexo.server.FakeApi.Reply;
import co.vexo.server.FakeApi.Request;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static co.vexo.server.FakeApi.DISCONNECT;
import static co.vexo.server.FakeApi.HANG;
import static co.vexo.server.FakeApi.ok;
import static co.vexo.server.FakeApi.reply;
import static co.vexo.server.FakeApi.times;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VexoTest {
    private final List<VexoException> errors = new CopyOnWriteArrayList<>();
    private final Deque<AutoCloseable> cleanup = new ArrayDeque<>();

    @AfterEach
    void closeEverything() throws Exception {
        while (!cleanup.isEmpty()) {
            cleanup.pop().close();
        }
    }

    // 1. capture() and identify() only enqueue, and never throw on bad input.

    @Test
    void neverThrowsOnBadInputAndReportsEachDroppedMessage() throws IOException {
        FakeApi api = api();
        Vexo vexo = start(client(api));

        vexo.capture(null, "e");
        vexo.capture("u", null);
        vexo.capture(" ", "e");
        vexo.capture("u", "");
        vexo.identify(null);
        vexo.identify("");
        vexo.capture("u", "far_future", null, Instant.MAX);
        vexo.flush();

        assertEquals(0, api.requests.size());
        assertEquals(7, errors.size());
        errors.forEach(e -> assertTrue(e.getMessage().startsWith("vexo: message dropped"), e.getMessage()));
    }

    @Test
    void captureOnlyQueuesWhileTheApiHangs() throws IOException {
        FakeApi api = api(new Reply(HANG, null, null));
        Vexo vexo = start(client(api).flushAt(1).timeout(Duration.ofSeconds(30)).shutdownTimeout(Duration.ofMillis(200)));
        vexo.capture("u", "first");
        waitFor(() -> api.requests.size() == 1);

        long started = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            vexo.capture("u", "e" + i);
            vexo.identify("u");
        }

        assertTrue(elapsedMillis(started) < 1000, "capture() waited for the network");
    }

    // 2. A messageId per message, reused on retries; timestamps in integer epoch milliseconds.

    @Test
    void sendsQueuedMessagesAsOneBatchWithTheHeaders() throws IOException {
        FakeApi api = api();
        Vexo vexo = start(client(api));

        vexo.identify("user_1", Map.of("plan", "pro"));
        vexo.capture("user_1", "purchase", Map.of("amount", 42), Instant.parse("2026-11-07T12:00:00Z"));
        vexo.close();

        assertEquals(1, api.requests.size());
        Request request = api.requests.get(0);
        assertEquals("POST", request.method);
        assertEquals("/v1/batch", request.path);
        assertEquals("test-key", request.header("X-Api-Key"));
        assertEquals("application/json", request.header("Content-Type"));
        assertTrue(request.header("User-Agent").matches("vexo-java/\\d+\\.\\d+\\.\\d+.*"), request.header("User-Agent"));
        Map<String, Object> identify = request.batch.get(0);
        Map<String, Object> capture = request.batch.get(1);
        assertEquals("identify", identify.get("type"));
        assertEquals("user_1", identify.get("distinctId"));
        assertEquals(Map.of("plan", "pro"), identify.get("traits"));
        assertEquals("capture", capture.get("type"));
        assertEquals("purchase", capture.get("event"));
        assertEquals(Map.of("amount", 42.0), capture.get("properties"));
        assertTrue(request.rawBody.contains("\"timestamp\":1794052800000,"), request.rawBody);
        UUID.fromString((String) identify.get("messageId"));
        assertNotEquals(identify.get("messageId"), capture.get("messageId"));
        assertEquals(List.of(), errors);
    }

    @Test
    void removesATrailingSlashFromTheHost() throws IOException {
        FakeApi api = api();
        Vexo vexo = start(client(api).host(api.host + "//"));

        vexo.capture("u", "a");
        vexo.flush();

        assertEquals("/v1/batch", api.requests.get(0).path);
    }

    @Test
    void defaultsTheTimestampToNowAndKeepsTheCallersMessageId() throws IOException {
        FakeApi api = api();
        Vexo vexo = start(client(api));
        long before = System.currentTimeMillis();

        vexo.capture("u", "a");
        vexo.capture("u", "b", null, null, "order-42-paid");
        vexo.flush();

        List<Map<String, Object>> batch = api.requests.get(0).batch;
        double timestamp = (Double) batch.get(0).get("timestamp");
        assertTrue(timestamp >= before && timestamp <= System.currentTimeMillis());
        assertEquals(Map.of(), batch.get(0).get("properties"));
        assertEquals("order-42-paid", batch.get(1).get("messageId"));
    }

    // 3. Serialization at enqueue: strings for BigDecimal, UUID and dates; one bad message never poisons a batch.

    @Test
    void sendsValuesJsonCannotEncodeAsStrings() throws IOException {
        FakeApi api = api();
        Vexo vexo = start(client(api));
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("amount", new BigDecimal("9.90"));
        properties.put("orderId", UUID.fromString("5b0c8f0e-6d1e-4f62-9d55-3f1f0f6a2b71"));
        properties.put("paidAt", Instant.parse("2026-01-02T03:04:05Z"));
        properties.put("day", LocalDate.of(2026, 1, 2));
        properties.put("created", new Date(0));
        properties.put("file", Paths.get("/tmp/receipt.pdf"));
        properties.put("tags", Set.of("vip"));
        properties.put("scores", new int[] {1, 2});
        properties.put("coupon", null);

        vexo.capture("u", "priced", properties);
        vexo.flush();

        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("amount", "9.90");
        expected.put("orderId", "5b0c8f0e-6d1e-4f62-9d55-3f1f0f6a2b71");
        expected.put("paidAt", "2026-01-02T03:04:05.000Z");
        expected.put("day", "2026-01-02");
        expected.put("created", "1970-01-01T00:00:00.000Z");
        expected.put("file", "/tmp/receipt.pdf");
        expected.put("tags", List.of("vip"));
        expected.put("scores", List.of(1.0, 2.0));
        expected.put("coupon", null);
        assertEquals(expected, api.requests.get(0).batch.get(0).get("properties"));
        assertEquals(List.of(), errors);
    }

    @Test
    void dropsOnlyTheMessageThatCannotBeSerialized() throws IOException {
        FakeApi api = api();
        Vexo vexo = start(client(api));
        Map<String, Object> circular = new HashMap<>();
        circular.put("self", circular);
        Object brokenToString = new Object() {
            @Override
            public String toString() {
                throw new IllegalStateException("broken toString");
            }
        };

        vexo.capture("u", "ok");
        vexo.capture("u", "circular", circular);
        vexo.capture("u", "nan", Map.of("ratio", Double.NaN));
        vexo.capture("u", "broken", Map.of("value", brokenToString));
        vexo.capture("u", "ok_too");
        vexo.flush();

        assertEquals(1, api.requests.size());
        assertEquals(List.of("ok", "ok_too"), api.requests.get(0).field("event"));
        assertEquals(3, errors.size());
        assertTrue(errors.get(0).getMessage().contains("JSON"), errors.get(0).getMessage());
        assertTrue(errors.get(1).getMessage().contains("JSON"), errors.get(1).getMessage());
        assertTrue(errors.get(2).getMessage().contains("broken toString"), errors.get(2).getMessage());
    }

    @Test
    void dropsAMessageOver5MbWhenItIsQueued() throws IOException {
        FakeApi api = api();
        Vexo vexo = start(client(api));

        vexo.capture("u", "huge", Map.of("blob", "x".repeat(Vexo.MAX_BODY_BYTES)));
        vexo.capture("u", "ok");
        vexo.flush();

        assertEquals(List.of("ok"), api.requests.get(0).field("event"));
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).getMessage().contains("5 MB"));
    }

    // 4. Batching on size and interval, under 5 MB, from a bounded queue.

    @Test
    void flushesWhenTheQueueReachesFlushAt() throws IOException {
        FakeApi api = api();
        Vexo vexo = start(client(api).flushAt(2));

        vexo.capture("u", "a");
        vexo.capture("u", "b");

        waitFor(() -> api.requests.size() == 1);
        assertEquals(2, api.requests.get(0).batch.size());
    }

    @Test
    void flushesOnTheInterval() throws IOException {
        FakeApi api = api();
        Vexo vexo = start(client(api).flushInterval(Duration.ofMillis(50)));

        vexo.capture("u", "a");

        waitFor(() -> api.requests.size() == 1);
    }

    @Test
    void aZeroFlushIntervalTurnsTheTimerOff() throws Exception {
        FakeApi api = api();
        Vexo vexo = start(client(api).flushInterval(Duration.ZERO));

        vexo.capture("u", "a");
        Thread.sleep(200);
        assertEquals(0, api.requests.size(), "a zero flushInterval sent without flushAt or flush()");
        vexo.flush();

        assertEquals(List.of("a"), api.requests.get(0).field("event"));
    }

    @Test
    void splitsBatchesSoEachRequestBodyStaysUnder5Mb() throws IOException {
        FakeApi api = api();
        Vexo vexo = start(client(api));
        String blob = "x".repeat(2 * 1024 * 1024);

        for (int i = 0; i < 3; i++) {
            vexo.capture("u", "e" + i, Map.of("blob", blob));
        }
        vexo.flush();

        assertEquals(List.of(2, 1), api.requests.stream().map(r -> r.batch.size()).collect(java.util.stream.Collectors.toList()));
        api.requests.forEach(r -> assertTrue(r.rawBody.getBytes(StandardCharsets.UTF_8).length <= Vexo.MAX_BODY_BYTES));
        assertEquals(List.of(), errors);
    }

    @Test
    void dropsNewMessagesOnceTheQueueIsFull() throws IOException {
        FakeApi api = api();
        Vexo vexo = start(client(api).maxQueueSize(1));

        vexo.capture("u", "a");
        vexo.capture("u", "b");
        vexo.flush();

        assertEquals(List.of("a"), api.requests.get(0).field("event"));
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).getMessage().contains("queue is full"));
    }

    // 5. Retries: network errors, 429 (Retry-After) and 5xx; never other 4xx; resend failed, report dropped.

    @Test
    void retries503And429WithTheSameMessageId() throws IOException {
        FakeApi api = api(reply(503, "0"), reply(429, "0"));
        Vexo vexo = start(client(api));

        vexo.capture("u", "a");
        vexo.flush();

        assertEquals(3, api.requests.size());
        assertEquals(api.requests.get(0).batch.get(0).get("messageId"), api.requests.get(2).batch.get(0).get("messageId"));
        assertEquals(List.of(), errors);
    }

    @Test
    void retriesANetworkError() throws IOException {
        FakeApi api = api(new Reply(DISCONNECT, null, null));
        Vexo vexo = start(client(api));

        vexo.capture("u", "a");
        vexo.flush();

        assertEquals(2, api.requests.size());
        assertEquals(api.requests.get(0).batch.get(0).get("messageId"), api.requests.get(1).batch.get(0).get("messageId"));
        assertEquals(List.of(), errors);
    }

    @Test
    void waitsTheRetryAfterSeconds() throws IOException {
        FakeApi api = api(reply(429, "1"));
        Vexo vexo = start(client(api));

        vexo.capture("u", "a");
        vexo.flush();

        assertEquals(2, api.requests.size());
        long waited = TimeUnit.NANOSECONDS.toMillis(api.requests.get(1).receivedNanos - api.requests.get(0).receivedNanos);
        assertTrue(waited >= 950, "waited " + waited + " ms");
    }

    @Test
    void capsRetryAfterAt60SecondsAndIgnoresAnythingButSeconds() {
        assertEquals(OptionalLong.of(0), BatchSender.retryAfterNanos("0"));
        assertEquals(OptionalLong.of(1_500_000_000L), BatchSender.retryAfterNanos(" 1.5 "));
        assertEquals(OptionalLong.of(TimeUnit.SECONDS.toNanos(60)), BatchSender.retryAfterNanos("120"));
        assertEquals(OptionalLong.empty(), BatchSender.retryAfterNanos(null));
        assertEquals(OptionalLong.empty(), BatchSender.retryAfterNanos("-1"));
        assertEquals(OptionalLong.empty(), BatchSender.retryAfterNanos("Wed, 21 Oct 2026 07:28:00 GMT"));
    }

    @Test
    void backsOffExponentiallyWithJitterUpTo30Seconds() {
        Set<Long> seen = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            long first = BatchSender.backoffNanos(0);
            long third = BatchSender.backoffNanos(2);
            long late = BatchSender.backoffNanos(40);
            assertTrue(first >= 500_000_000L && first <= 1_000_000_000L, "attempt 0: " + first);
            assertTrue(third >= 2_000_000_000L && third <= 4_000_000_000L, "attempt 2: " + third);
            assertTrue(late >= TimeUnit.SECONDS.toNanos(15) && late <= TimeUnit.SECONDS.toNanos(30), "attempt 40: " + late);
            seen.add(first);
        }
        assertTrue(seen.size() > 1, "no jitter");
    }

    @Test
    void doesNotRetryA4xxAndReportsIt() throws IOException {
        FakeApi api = api(reply(401));
        Vexo vexo = start(client(api));

        vexo.capture("u", "a");
        vexo.flush();

        assertEquals(1, api.requests.size());
        assertEquals(1, errors.size());
        assertEquals(401, errors.get(0).getStatus());
        assertEquals("a", errors.get(0).getBatch().get(0).get("event"));
    }

    @Test
    void givesUpAfterMaxRetries() throws IOException {
        FakeApi api = api(times(5, reply(500, "0")));
        Vexo vexo = start(client(api).maxRetries(2));

        vexo.capture("u", "a");
        vexo.flush();

        assertEquals(3, api.requests.size());
        assertEquals(1, errors.size());
        assertEquals(500, errors.get(0).getStatus());
    }

    @Test
    void reportsTheMessagesTheServerDroppedWithoutResendingThem() throws IOException {
        FakeApi api = api(ok("{\"accepted\":1,\"dropped\":[{\"index\":1,\"reason\":\"event exceeds 255 characters\"}],\"failed\":[]}"));
        Vexo vexo = start(client(api));

        vexo.capture("u", "ok");
        vexo.capture("u", "too_long");
        vexo.flush();

        assertEquals(1, api.requests.size());
        assertEquals(1, errors.size());
        VexoException.Dropped dropped = errors.get(0).getDropped().get(0);
        assertEquals("too_long", dropped.getMessage().get("event"));
        assertEquals("event exceeds 255 characters", dropped.getReason());
    }

    @Test
    void resendsOnlyTheMessagesTheServerReportsAsFailed() throws IOException {
        FakeApi api = api(ok("{\"accepted\":1,\"dropped\":[],\"failed\":[{\"index\":1,\"reason\":\"Storing failed; retry this message\"}]}"));
        Vexo vexo = start(client(api));

        vexo.capture("a", "stored");
        vexo.capture("b", "failed_once");
        vexo.flush();

        assertEquals(2, api.requests.size());
        assertEquals(List.of("failed_once"), api.requests.get(1).field("event"));
        assertEquals(api.requests.get(0).batch.get(1).get("messageId"), api.requests.get(1).batch.get(0).get("messageId"));
        assertEquals(List.of(), errors);
    }

    @Test
    void reportsFailedMessagesThatStillFailAfterTheRetries() throws IOException {
        Reply failed = ok("{\"accepted\":0,\"dropped\":[],\"failed\":[{\"index\":0,\"reason\":\"Storing failed; retry this message\"}]}");
        FakeApi api = api(failed, failed);
        Vexo vexo = start(client(api).maxRetries(1));

        vexo.capture("u", "a");
        vexo.flush();

        assertEquals(2, api.requests.size());
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).getMessage().contains("could not store"));
        assertEquals("a", errors.get(0).getDropped().get(0).getMessage().get("event"));
    }

    @Test
    void retriesARequestThatTimesOut() throws IOException {
        FakeApi api = api(new Reply(HANG, null, null));
        Vexo vexo = start(client(api).timeout(Duration.ofMillis(200)));

        vexo.capture("u", "a");
        vexo.flush();

        assertEquals(2, api.requests.size());
        assertEquals(List.of(), errors);
    }

    @Test
    void countsA2xxBodyItCannotReadAsASuccess() throws IOException {
        FakeApi api = api(new Reply(200, null, "<html>ok</html>"));
        Vexo vexo = start(client(api));

        vexo.capture("u", "a");
        vexo.flush();

        assertEquals(1, api.requests.size());
        assertEquals(List.of(), errors);
    }

    // 6. Circuit breaker.

    @Test
    void givesLaterBatchesOneAttemptAfterABatchUsedUpItsRetriesUntilARequestSucceeds() throws IOException {
        Reply error = reply(500, "0");
        FakeApi api = api(error, error, error, error, ok("{\"accepted\":1,\"dropped\":[],\"failed\":[]}"), error, error, error);
        Vexo vexo = start(client(api).maxRetries(2));

        vexo.capture("u", "e0");
        vexo.flush();
        assertEquals(3, api.requests.size(), "the first batch gets its retries");
        vexo.capture("u", "e1");
        vexo.flush();
        assertEquals(4, api.requests.size(), "an open circuit gives one attempt");
        vexo.capture("u", "e2");
        vexo.flush();
        assertEquals(5, api.requests.size(), "the success closes the circuit");
        vexo.capture("u", "e3");
        vexo.flush();
        assertEquals(8, api.requests.size(), "a closed circuit gives the retries again");
        assertEquals(3, errors.size());
    }

    // 7. flush() and close() with a deadline; the shutdown hook.

    @Test
    void closeSendsWhatIsQueuedAndDropsLaterMessages() throws IOException {
        FakeApi api = api();
        Vexo vexo = start(client(api));

        vexo.capture("u", "a");
        vexo.identify("u", Map.of("plan", "pro"));
        vexo.close();
        vexo.capture("u", "late");
        vexo.close();
        vexo.flush();

        assertEquals(1, api.requests.size());
        assertEquals(2, api.requests.get(0).batch.size());
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).getMessage().contains("closed"));
    }

    @Test
    void closeGivesUpAtTheShutdownTimeoutWhenTheApiIsUnreachable() throws IOException {
        int port;
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        Vexo vexo = start(Vexo.builder("test-key").host("http://127.0.0.1:" + port).shutdownHook(false).onError(errors::add)
                .flushAt(1).timeout(Duration.ofMillis(500)).shutdownTimeout(Duration.ofSeconds(1)));
        for (int i = 0; i < 5; i++) {
            vexo.capture("u", "e" + i);
        }

        long started = System.nanoTime();
        vexo.close();

        assertTrue(elapsedMillis(started) < 2500, "close() took " + elapsedMillis(started) + " ms");
        assertEquals(5, errors.stream().mapToInt(e -> e.getBatch().size()).sum(), "every message reported: " + errors);
    }

    @Test
    void closeAbortsTheBackgroundRequestInFlightAndReportsItsBatchBeforeReturning() throws IOException {
        FakeApi api = api(new Reply(HANG, null, null));
        Vexo vexo = start(client(api).flushAt(1).timeout(Duration.ofSeconds(30)).shutdownTimeout(Duration.ofMillis(500))
                .onError(this::recordSlowly));
        vexo.capture("u", "in_flight");
        waitFor(() -> api.requests.size() == 1);

        long started = System.nanoTime();
        vexo.close();

        assertTrue(elapsedMillis(started) < 1500, "close() took " + elapsedMillis(started) + " ms");
        assertEquals(List.of("in_flight"), reportedEvents());
    }

    @Test
    void closeEndsTheBackgroundRetryWaitStartsNoRetryAndReportsTheBatchBeforeReturning() throws Exception {
        FakeApi api = api(reply(429, "30"));
        Vexo vexo = start(client(api).flushAt(1).shutdownTimeout(Duration.ofMillis(500)).onError(this::recordSlowly));
        vexo.capture("u", "waiting");
        waitFor(() -> api.requests.size() == 1);
        Thread.sleep(100);

        long started = System.nanoTime();
        vexo.close();

        assertTrue(elapsedMillis(started) < 1500, "close() took " + elapsedMillis(started) + " ms");
        assertEquals(List.of("waiting"), reportedEvents());
        Thread.sleep(200);
        assertEquals(1, api.requests.size(), "a retry started after the deadline");
    }

    @Test
    void closeGivesUpAtOnceOnARetryThatWouldComeAfterTheDeadline() throws IOException {
        FakeApi api = api(reply(503, "5"));
        Vexo vexo = start(client(api).shutdownTimeout(Duration.ofSeconds(2)));
        vexo.capture("u", "a");

        long started = System.nanoTime();
        vexo.close();

        assertTrue(elapsedMillis(started) < 1000, "close() waited " + elapsedMillis(started) + " ms for a retry it can't make");
        assertEquals(1, api.requests.size());
        assertEquals(503, errors.get(0).getStatus());
        assertEquals(1, errors.get(0).getBatch().size());
    }

    @Test
    void closeReturnsAtTheShutdownTimeoutWhileARequestHangs() throws IOException {
        FakeApi api = api(new Reply(HANG, null, null));
        Vexo vexo = start(client(api).flushAt(1).timeout(Duration.ofSeconds(30)).shutdownTimeout(Duration.ofSeconds(1)));
        vexo.capture("u", "in_flight");
        waitFor(() -> api.requests.size() == 1);
        vexo.capture("u", "queued_1");
        vexo.capture("u", "queued_2");

        long started = System.nanoTime();
        vexo.close();

        assertTrue(elapsedMillis(started) < 2000, "close() took " + elapsedMillis(started) + " ms");
        VexoException deadline = errors.stream().filter(e -> e.getMessage().contains("deadline")).findFirst().orElseThrow();
        assertEquals(2, deadline.getBatch().size());
    }

    @Test
    void registersAShutdownHookByDefaultAndCloseRemovesIt() throws IOException {
        FakeApi api = api();
        Vexo vexo = start(Vexo.builder("test-key").host(api.host));
        assertNotNull(vexo.shutdownHook);
        assertTrue(Runtime.getRuntime().removeShutdownHook(vexo.shutdownHook), "the hook wasn't registered");
        Runtime.getRuntime().addShutdownHook(vexo.shutdownHook);

        vexo.close();

        assertFalse(Runtime.getRuntime().removeShutdownHook(vexo.shutdownHook), "close() left the hook registered");
        assertNull(start(client(api)).shutdownHook);
    }

    @Test
    void theShutdownHookSendsTheQueueWhenTheJvmExits() throws Exception {
        FakeApi api = api();

        long millis = runChildJvm(api.host, 10_000);

        assertTrue(millis < 10_000, "the JVM took " + millis + " ms to exit");
        assertEquals(1, api.requests.size());
        assertEquals(List.of("sent_by_the_shutdown_hook"), api.requests.get(0).field("event"));
    }

    @Test
    void theJvmExitsAtTheShutdownTimeoutWhenTheApiHangs() throws Exception {
        FakeApi api = api(new Reply(HANG, null, null));

        // The request timeout is 30 s; only the 1 s shutdown timeout lets the JVM exit sooner.
        long millis = runChildJvm(api.host, 1000);

        assertTrue(millis < 10_000, "the JVM took " + millis + " ms to exit");
        assertEquals(1, api.requests.size());
    }

    // 9. Safe for concurrent callers.

    @Test
    void sendsEachMessageOnceWithConcurrentCallers() throws Exception {
        FakeApi api = api();
        Vexo vexo = start(client(api).flushAt(50));
        CountDownLatch go = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            String distinctId = "user_" + t;
            Thread thread = new Thread(() -> {
                try {
                    go.await();
                } catch (InterruptedException e) {
                    return;
                }
                for (int i = 0; i < 250; i++) {
                    vexo.capture(distinctId, "e" + i);
                }
            });
            thread.start();
            threads.add(thread);
        }
        go.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        vexo.close();

        Set<Object> messageIds = new HashSet<>();
        api.requests.forEach(r -> {
            assertTrue(r.batch.size() <= 50);
            messageIds.addAll(r.field("messageId"));
        });
        assertEquals(2000, api.requests.stream().mapToInt(r -> r.batch.size()).sum());
        assertEquals(2000, messageIds.size());
        assertEquals(List.of(), errors);
    }

    @Test
    void anErrorCallbackThatThrowsDoesNotStopTheBackgroundThread() throws IOException {
        Logger logger = Logger.getLogger("co.vexo.server");
        Level level = logger.getLevel();
        logger.setLevel(Level.OFF);
        cleanup.push(() -> logger.setLevel(level));
        FakeApi api = api(reply(400));
        Vexo vexo = start(Vexo.builder("test-key").host(api.host).shutdownHook(false)
                .flushInterval(Duration.ofMillis(50))
                .onError(e -> {
                    throw new IllegalStateException("a bug in the callback");
                }));

        vexo.capture("u", "rejected");
        waitFor(() -> api.requests.size() == 1);
        vexo.capture("u", "sent_later");

        waitFor(() -> api.requests.size() == 2);
    }

    // An error callback that takes a while, so a report that arrives after close() returns is missed.
    private void recordSlowly(VexoException error) {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        errors.add(error);
    }

    private List<Object> reportedEvents() {
        return errors.stream().flatMap(e -> e.getBatch().stream()).map(m -> m.get("event"))
                .collect(java.util.stream.Collectors.toList());
    }

    private FakeApi api(Reply... replies) throws IOException {
        FakeApi api = new FakeApi(replies);
        cleanup.push(api);
        return api;
    }

    // A client for tests: no shutdown hook, no interval flush unless a test sets one, errors collected.
    private Vexo.Builder client(FakeApi api) {
        return Vexo.builder("test-key").host(api.host).flushInterval(Duration.ofMinutes(1)).shutdownHook(false)
                .onError(errors::add);
    }

    private Vexo start(Vexo.Builder builder) {
        Vexo vexo = builder.build();
        cleanup.push(vexo);
        return vexo;
    }

    // Runs ExitHookMain in a new JVM and returns how long it ran, in milliseconds.
    private static long runChildJvm(String host, long shutdownMillis) throws Exception {
        String java = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
        Process process = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                ExitHookMain.class.getName(), host, String.valueOf(shutdownMillis))
                .redirectErrorStream(true)
                .start();
        long started = System.nanoTime();
        boolean exited = process.waitFor(30, TimeUnit.SECONDS);
        long millis = elapsedMillis(started);
        if (!exited) {
            process.destroyForcibly();
        }
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(exited, "the JVM didn't exit: " + output);
        assertEquals(0, process.exitValue(), output);
        return millis;
    }

    private static void waitFor(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertTrue(condition.getAsBoolean(), "timed out");
    }

    private static long elapsedMillis(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }
}
