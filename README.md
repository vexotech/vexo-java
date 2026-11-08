# vexo-java

Send events and identifies to [Vexo](https://vexo.co) from your Java or Kotlin backend: purchases, subscription changes, background jobs, anything your app's SDK can't see. Server events land in the same app as your mobile and web events, bill as events, and join the person who has the same distinct ID.

- Zero runtime dependencies (Java 11 or later; `java.net.http.HttpClient` and `java.util.concurrent` from the JDK).
- Batches messages on a daemon thread, sends on size and on an interval, and sends what's queued when the JVM exits, within a deadline.
- Retries network errors, timeouts, `429`, and `5xx` responses (and the messages the API reports as `failed`) with exponential backoff, and honors `Retry-After`.
- Keeps each request under the API's 5 MB body limit, and is safe to share across threads.

## Install

Maven:

```xml
<dependency>
    <groupId>co.vexo</groupId>
    <artifactId>vexo-java</artifactId>
    <version>0.1.0</version>
</dependency>
```

Gradle (Kotlin DSL):

```kotlin
implementation("co.vexo:vexo-java:0.1.0")
```

## Usage

```java
import co.vexo.server.Vexo;
import java.util.Map;

Vexo vexo = Vexo.builder(System.getenv("VEXO_API_KEY")).build();

// Attach traits to the person. Use the same ID you pass to identifyDevice() in the app SDK.
vexo.identify("user@example.com", Map.of("plan", "pro", "company", "Acme"));

// Record an event.
vexo.capture("user@example.com", "purchase_completed", Map.of("amount", 49.9, "currency", "USD"));

// Optional: send everything still queued now. The shutdown hook also does it when the JVM exits.
vexo.close();
```

Create one client when your application starts and share it; it's safe for concurrent use. `capture()` and `identify()` only queue the message, so they never block a request thread. By default, the client registers a JVM shutdown hook that calls `close()`. If the process is killed without a normal shutdown (for example, `SIGKILL` or `Runtime.halt()`), queued messages are lost. `close()` gives up after `shutdownTimeout` and reports what was still queued to the error callback.

Copy your app's API key from the app's [settings](https://docs.vexo.co/settings/app-settings/app-general-information) in the Vexo dashboard. It's the same key your app SDK uses, so it isn't a secret; see [Security](https://docs.vexo.co/server-side/capture#security).

### Kotlin

The client is plain Java, so Kotlin needs no extra module:

```kotlin
import co.vexo.server.Vexo
import java.time.Duration

val vexo = Vexo.builder(System.getenv("VEXO_API_KEY"))
    .flushInterval(Duration.ofSeconds(5))
    .onError { error -> logger.warn(error.message) }
    .build()

vexo.identify("user@example.com", mapOf("plan" to "pro"))
vexo.capture("user@example.com", "purchase_completed", mapOf("amount" to 49.9, "currency" to "USD"))

// For a short-lived block, use() closes the client at the end.
Vexo.builder(apiKey).build().use { client -> client.capture("job-runner", "nightly_export_done") }
```

### Spring Boot

Declare the client as a bean, and let Spring close it when the application context shuts down:

```java
@Configuration
class VexoConfiguration {
    @Bean(destroyMethod = "close")
    Vexo vexo(@Value("${vexo.api-key}") String apiKey) {
        // Spring closes the bean, after the beans that use it; the JVM hook isn't needed.
        return Vexo.builder(apiKey).shutdownHook(false).build();
    }
}
```

Spring infers `close()` on an `AutoCloseable` bean even without `destroyMethod`; naming it makes the intent clear. Turn off the client's own shutdown hook so that a bean that captures an event while it shuts down still reaches an open client.

### Serverless

On AWS Lambda and similar platforms, the runtime freezes the JVM between invocations and may stop it without running shutdown hooks. Call `flush()` before your handler returns so that each invocation sends its own messages:

```java
public class Handler implements RequestHandler<Map<String, Object>, String> {
    private static final Vexo VEXO = Vexo.builder(System.getenv("VEXO_API_KEY")).build();

    @Override
    public String handleRequest(Map<String, Object> input, Context context) {
        VEXO.capture("user@example.com", "report_generated");
        VEXO.flush();
        return "ok";
    }
}
```

## API

### `Vexo.builder(String apiKey)`

Returns a `Vexo.Builder`. Throws `IllegalArgumentException` when the API key is null or blank. Set any of these options, then call `build()`:

| Option | Default | Description |
| --- | --- | --- |
| `host(String)` | `https://api.vexo.co` | API base URL. |
| `flushAt(int)` | `100` | Send a batch when this many messages are queued (1 to 500). |
| `flushInterval(Duration)` | 10 seconds | Send whatever is queued this often. `Duration.ZERO` turns the timer off, so messages go out only on `flushAt`, `flush()`, and `close()`. |
| `maxRetries(int)` | `3` | Retries for network errors, timeouts, `429`, and `5xx` responses. |
| `maxQueueSize(int)` | `10000` | Messages kept in memory; new messages are dropped once it's full. |
| `timeout(Duration)` | 10 seconds | The timeout of each request. |
| `shutdownTimeout(Duration)` | 10 seconds | How long `close()` keeps sending. |
| `onError(Consumer<? super VexoException>)` | Log a warning to the `co.vexo.server` `java.util.logging` logger | Called with send failures and with the messages that were dropped. |
| `shutdownHook(boolean)` | `true` | Register a JVM shutdown hook that calls `close()`. |

`build()` starts the client's daemon thread. It throws `IllegalArgumentException` for a host that isn't a URI, a timeout or queue size that isn't positive, or a negative flush interval.

### `capture`

```java
void capture(String distinctId, String event)
void capture(String distinctId, String event, Map<String, ?> properties)
void capture(String distinctId, String event, Map<String, ?> properties, Instant timestamp)
void capture(String distinctId, String event, Map<String, ?> properties, Instant timestamp, String messageId)
```

Queues an event. `timestamp` defaults to the time you call `capture()` and is sent as epoch milliseconds; convert other date-time types with `toInstant()`. `messageId` defaults to a random UUID; pass your own, such as an order ID, to make a message that you send twice count once.

### `identify`

```java
void identify(String distinctId)
void identify(String distinctId, Map<String, ?> traits)
```

Queues an identify. Traits merge into the person's existing traits; a trait set to `null` is removed.

### `flush()`

Sends everything queued, blocking until the queue is empty.

### `close()`

Stops the background thread and sends everything queued, for up to `shutdownTimeout`. At the deadline, it interrupts a request or a retry wait that's still in progress and starts no new attempt; a retry that would come after the deadline is given up at once. Before `close()` returns, it reports the messages it didn't send, including the batch it interrupted. Later `capture()` and `identify()` calls are dropped and reported. Calling `close()` again does nothing.

## Errors and dropped messages

The client never throws from `capture()`, `identify()`, `flush()`, or `close()`. Failures go to the `onError` callback as a `VexoException`, on the thread that called `capture()` or `identify()`, or on the background thread:

- A message without a `distinctId` or an `event`, or that comes after `close()`, is dropped when you queue it.
- Values that JSON can't hold directly are sent as strings: `BigDecimal`, `UUID`, `java.time` types, enums, and any other object, by its `toString()`. A `java.util.Date` is sent as an ISO 8601 instant. A message that still can't be serialized (`NaN`, an infinity, or a circular reference) or that is over 5 MB is dropped when you queue it; the rest of the queue is unaffected.
- A batch that still fails after the retries, or that the API rejects with a `4xx`, arrives with `getBatch()` (the messages it held) and `getStatus()` (the HTTP status, or 0 when there was no response). After that, the next batches get a single attempt until a request succeeds, so an unreachable API doesn't hold up your process.
- A message the API dropped (for example, properties over 64 KB) arrives in `getDropped()`, with the message and the API's reason. Messages the API reports as `failed` are retried, and reported the same way if they still fail.
- Messages still queued at the shutdown deadline arrive as one error with `getBatch()`.

Every message carries a `messageId`, so a retried batch is stored once.

## Development

You need JDK 11 or later and Maven.

```bash
mvn verify
```

The tests start a local mock API with the JDK's `com.sun.net.httpserver.HttpServer`; they don't use the network.

### Conformance

To run the scenarios of the shared `vexo-sdk-conformance` harness, build the driver and pass it to `run.py`:

```bash
mvn -q compile
javac --release 11 -d target/conformance -cp target/classes conformance/ConformanceDriver.java
python3 ../vexo-sdk-conformance/run.py \
  --driver "java -cp $PWD/target/classes:$PWD/target/conformance co.vexo.server.ConformanceDriver"
```

## License

MIT
