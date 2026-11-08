package co.vexo.server;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sends events and identifies to the Vexo server-side capture API. {@link #capture} and {@link #identify} only queue
 * the message; a daemon thread sends the queue in batches when {@code flushAt} messages are queued and every
 * {@code flushInterval}. Create one client for your process and share it: it's safe for concurrent use.
 *
 * <pre>{@code
 * Vexo vexo = Vexo.builder(System.getenv("VEXO_API_KEY")).build();
 * vexo.identify("user@example.com", Map.of("plan", "pro"));
 * vexo.capture("user@example.com", "purchase_completed", Map.of("amount", 49.9, "currency", "USD"));
 * }</pre>
 *
 * <p>No method throws because of a message or the network: failures go to the error callback.
 */
public final class Vexo implements AutoCloseable {
    // The API rejects a batch of more than 500 messages or a body over 5 MB.
    static final int MAX_BATCH_SIZE = 500;
    static final int MAX_BODY_BYTES = 5 * 1024 * 1024;
    static final int BATCH_ENVELOPE_BYTES = "{\"batch\":[]}".length();
    static final String USER_AGENT = "vexo-java/" + version();
    private static final String DEFAULT_HOST = "https://api.vexo.co";
    // How long close() waits past the deadline for the interrupted background thread to report its batch.
    private static final long ABORT_GRACE_MILLIS = 500;
    private static final Logger LOG = Logger.getLogger("co.vexo.server");

    private final int flushAt;
    private final int maxQueueSize;
    private final long shutdownTimeoutNanos;
    private final Consumer<? super VexoException> onError;
    private final BatchSender sender;
    private final ArrayDeque<Message> queue = new ArrayDeque<>();
    // Held while a thread sends, so batches go out one at a time and in order.
    private final ReentrantLock sendLock = new ReentrantLock();
    private final AtomicBoolean flushRequested = new AtomicBoolean();
    private final ScheduledExecutorService worker;
    // Package-private so tests can check the registration; null when the builder turned the hook off.
    final Thread shutdownHook;
    private volatile boolean closed;
    private volatile long deadlineNanos;

    private Vexo(Builder builder) {
        flushAt = builder.flushAt;
        maxQueueSize = builder.maxQueueSize;
        shutdownTimeoutNanos = builder.shutdownTimeout.toNanos();
        onError = builder.onError;
        sender = new BatchSender(URI.create(builder.host.replaceAll("/+$", "") + "/v1/batch"), builder.apiKey,
                USER_AGENT, builder.timeout, builder.maxRetries, this::report, this::remainingNanos);
        worker = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "vexo-flush");
            // A daemon thread never keeps the JVM alive; the shutdown hook sends what is queued.
            thread.setDaemon(true);
            return thread;
        });
        long interval = builder.flushInterval.toNanos();
        // A zero interval turns the timer off: only flushAt, flush(), and close() send.
        if (interval > 0) {
            worker.scheduleWithFixedDelay(this::flushInBackground, interval, interval, TimeUnit.NANOSECONDS);
        }
        shutdownHook = builder.shutdownHook ? new Thread(this::close, "vexo-shutdown") : null;
        if (shutdownHook != null) {
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
    }

    /**
     * Starts a client for the app with this API key. Copy the key from the app's settings in the Vexo dashboard.
     *
     * @throws IllegalArgumentException when the API key is null or blank
     */
    public static Builder builder(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalArgumentException("vexo: an API key is required");
        }
        return new Builder(apiKey);
    }

    /** Queues an event, timestamped now. */
    public void capture(String distinctId, String event) {
        capture(distinctId, event, null, null, null);
    }

    /** Queues an event with properties, timestamped now. */
    public void capture(String distinctId, String event, Map<String, ?> properties) {
        capture(distinctId, event, properties, null, null);
    }

    /** Queues an event with properties that happened at {@code timestamp}. */
    public void capture(String distinctId, String event, Map<String, ?> properties, Instant timestamp) {
        capture(distinctId, event, properties, timestamp, null);
    }

    /**
     * Queues an event.
     *
     * @param distinctId the person's ID: the one you pass to {@code identifyDevice()} in the app SDK
     * @param event      the event name
     * @param properties the event properties, or null
     * @param timestamp  when the event happened, sent as epoch milliseconds; null for now
     * @param messageId  a unique ID that makes a resent message count once; null for a random UUID
     */
    public void capture(String distinctId, String event, Map<String, ?> properties, Instant timestamp,
                        String messageId) {
        try {
            requireText(distinctId, "distinctId");
            requireText(event, "event");
            Map<String, Object> message = new LinkedHashMap<>();
            message.put("type", "capture");
            message.put("distinctId", distinctId);
            message.put("event", event);
            message.put("properties", properties == null ? Map.of() : properties);
            message.put("timestamp", timestamp == null ? System.currentTimeMillis() : timestamp.toEpochMilli());
            enqueue(message, messageId);
        } catch (RuntimeException e) {
            report(new VexoException("vexo: message dropped: " + e.getMessage(), e));
        }
    }

    /** Queues an identify with no traits. */
    public void identify(String distinctId) {
        identify(distinctId, null);
    }

    /**
     * Queues an identify: merges {@code traits} into the person with this distinct ID. A trait set to null is
     * removed.
     */
    public void identify(String distinctId, Map<String, ?> traits) {
        try {
            requireText(distinctId, "distinctId");
            Map<String, Object> message = new LinkedHashMap<>();
            message.put("type", "identify");
            message.put("distinctId", distinctId);
            message.put("traits", traits == null ? Map.of() : traits);
            enqueue(message, null);
        } catch (RuntimeException e) {
            report(new VexoException("vexo: message dropped: " + e.getMessage(), e));
        }
    }

    /**
     * Sends everything queued, blocking until the queue is empty. Send failures go to the error callback. Before
     * {@link #close()}, a batch that the API keeps failing takes its full retries.
     */
    public void flush() {
        try {
            // Interruptible, so close() can stop a background flush that waits here at the deadline.
            sendLock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        try {
            drain();
        } finally {
            sendLock.unlock();
        }
    }

    /**
     * Stops the background thread and sends everything queued, for up to {@code shutdownTimeout}. At the deadline it
     * interrupts a request or retry wait still in progress and starts no new attempt; the messages it didn't send,
     * and what is still queued, are reported to the error callback before it returns. Later messages are dropped and
     * reported. Calling it again does nothing. The shutdown hook calls it when the JVM exits.
     */
    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            deadlineNanos = System.nanoTime() + shutdownTimeoutNanos;
            closed = true;
        }
        if (shutdownHook != null && Thread.currentThread() != shutdownHook) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException e) {
                // The JVM is already shutting down; the hook calls close() again, which returns at once.
            }
        }
        worker.shutdown();
        boolean locked = false;
        try {
            // The background thread may be in a request; wait for it only until the deadline.
            locked = sendLock.tryLock(remainingNanos(), TimeUnit.NANOSECONDS);
            if (locked) {
                drain();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (locked) {
                sendLock.unlock();
            }
            // At the deadline, interrupts the background thread's request or retry wait, and gives it a moment to
            // report the batch it held, so that report comes before close() returns.
            worker.shutdownNow();
            try {
                worker.awaitTermination(ABORT_GRACE_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            dropQueue();
        }
    }

    private void enqueue(Map<String, Object> message, String messageId) {
        message.put("messageId", messageId == null || messageId.isBlank() ? UUID.randomUUID().toString() : messageId);
        byte[] json;
        try {
            json = Json.write(message).getBytes(StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("it can't be sent as JSON: " + e.getMessage(), e);
        }
        if (json.length + BATCH_ENVELOPE_BYTES > MAX_BODY_BYTES) {
            throw new IllegalArgumentException("it is larger than 5 MB");
        }
        boolean ready;
        synchronized (queue) {
            // Checked under the queue's lock: close() sets closed before its last dropQueue(), so a message that
            // races close() is either reported by that dropQueue() or refused here, never lost silently.
            if (closed) {
                throw new IllegalStateException("the client is closed");
            }
            if (queue.size() >= maxQueueSize) {
                throw new IllegalStateException("the queue is full");
            }
            queue.add(new Message(message, json));
            ready = queue.size() >= flushAt;
        }
        if (ready && flushRequested.compareAndSet(false, true)) {
            try {
                worker.execute(this::flushInBackground);
            } catch (RejectedExecutionException e) {
                // close() started after the check above; it sends the queue.
            }
        }
    }

    private void flushInBackground() {
        flushRequested.set(false);
        flush();
    }

    // Sends batches until the queue is empty, or drops the rest at the shutdown deadline. Call with sendLock held.
    private void drain() {
        try {
            while (true) {
                if (remainingNanos() <= 0) {
                    dropQueue();
                    return;
                }
                List<Message> batch = nextBatch();
                if (batch.isEmpty()) {
                    return;
                }
                sender.send(batch);
            }
        } catch (InterruptedException e) {
            // What is left stays queued; close() reports it.
            Thread.currentThread().interrupt();
        }
    }

    // Up to flushAt messages whose {"batch":[...]} body fits in 5 MB, counting a comma between messages.
    private List<Message> nextBatch() {
        List<Message> batch = new ArrayList<>();
        long bytes = BATCH_ENVELOPE_BYTES;
        synchronized (queue) {
            while (batch.size() < flushAt && !queue.isEmpty()
                    && bytes + queue.peek().json.length + batch.size() <= MAX_BODY_BYTES) {
                bytes += queue.peek().json.length;
                batch.add(queue.poll());
            }
        }
        return batch;
    }

    private void dropQueue() {
        List<Message> dropped;
        synchronized (queue) {
            dropped = new ArrayList<>(queue);
            queue.clear();
        }
        if (!dropped.isEmpty()) {
            report(new VexoException("vexo: shutdown deadline reached; " + dropped.size() + " message(s) dropped",
                    null, 0, dropped));
        }
    }

    // Time left before the shutdown deadline; Long.MAX_VALUE until close() starts.
    private long remainingNanos() {
        return closed ? Math.max(0, deadlineNanos - System.nanoTime()) : Long.MAX_VALUE;
    }

    // A callback that throws must not stop the background thread: a scheduled task that throws never runs again.
    private void report(VexoException error) {
        try {
            onError.accept(error);
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "vexo: the onError callback threw", e);
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
    }

    private static String version() {
        try (InputStream in = Vexo.class.getResourceAsStream("version.properties")) {
            Properties properties = new Properties();
            properties.load(Objects.requireNonNull(in));
            return properties.getProperty("version", "unknown");
        } catch (IOException | RuntimeException e) {
            return "unknown";
        }
    }

    /** Options for a {@link Vexo} client. Every option has a default; only the API key is required. */
    public static final class Builder {
        private final String apiKey;
        private String host = DEFAULT_HOST;
        private int flushAt = 100;
        private Duration flushInterval = Duration.ofSeconds(10);
        private int maxRetries = 3;
        private int maxQueueSize = 10_000;
        private Duration timeout = Duration.ofSeconds(10);
        private Duration shutdownTimeout = Duration.ofSeconds(10);
        private Consumer<? super VexoException> onError = error -> LOG.warning(error.getMessage());
        private boolean shutdownHook = true;

        private Builder(String apiKey) {
            this.apiKey = apiKey;
        }

        /** The API base URL. Default: {@code https://api.vexo.co}. */
        public Builder host(String host) {
            this.host = Objects.requireNonNull(host, "host");
            return this;
        }

        /** Sends a batch when this many messages are queued, from 1 to 500. Default: 100. */
        public Builder flushAt(int flushAt) {
            this.flushAt = Math.max(1, Math.min(flushAt, MAX_BATCH_SIZE));
            return this;
        }

        /**
         * Sends whatever is queued this often. {@link Duration#ZERO} turns the timer off, so only {@code flushAt},
         * {@link Vexo#flush()}, and {@link Vexo#close()} send. Default: 10 seconds.
         */
        public Builder flushInterval(Duration flushInterval) {
            this.flushInterval = Objects.requireNonNull(flushInterval, "flushInterval");
            return this;
        }

        /** Retries for network errors, 429, and 5xx responses. Default: 3. */
        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        /** Messages kept in memory; new messages are dropped and reported once it's full. Default: 10,000. */
        public Builder maxQueueSize(int maxQueueSize) {
            this.maxQueueSize = maxQueueSize;
            return this;
        }

        /** The timeout of each request. Default: 10 seconds. */
        public Builder timeout(Duration timeout) {
            this.timeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        /** How long {@link Vexo#close()} keeps sending. Default: 10 seconds. */
        public Builder shutdownTimeout(Duration shutdownTimeout) {
            this.shutdownTimeout = Objects.requireNonNull(shutdownTimeout, "shutdownTimeout");
            return this;
        }

        /**
         * Called with send failures, the messages the API dropped, and the messages the client dropped. Runs on the
         * thread that calls {@code capture()} or {@code identify()}, or on the background thread. Default: log a
         * warning to the {@code co.vexo.server} java.util.logging logger.
         */
        public Builder onError(Consumer<? super VexoException> onError) {
            this.onError = Objects.requireNonNull(onError, "onError");
            return this;
        }

        /**
         * Whether to register a JVM shutdown hook that calls {@link Vexo#close()}. Turn it off when your framework
         * closes the client, for example a Spring bean's destroy method. Default: true.
         */
        public Builder shutdownHook(boolean shutdownHook) {
            this.shutdownHook = shutdownHook;
            return this;
        }

        /**
         * Creates the client and starts its background thread.
         *
         * @throws IllegalArgumentException when an option is out of range: a host that isn't a URI, a timeout or
         *                                  queue size that isn't positive, or a negative flush interval
         */
        public Vexo build() {
            if (maxQueueSize <= 0 || flushInterval.isNegative()) {
                throw new IllegalArgumentException("vexo: maxQueueSize must be positive and flushInterval not negative");
            }
            return new Vexo(this);
        }
    }
}
