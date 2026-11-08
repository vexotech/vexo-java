package co.vexo.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** A local Vexo API that records each request and answers with the next scripted reply, then with a 200. */
final class FakeApi implements AutoCloseable {
    static final int DISCONNECT = -1;
    static final int HANG = -2;

    final List<Request> requests = new CopyOnWriteArrayList<>();
    final String host;
    private final Deque<Reply> replies = new ArrayDeque<>();
    private final CountDownLatch release = new CountDownLatch(1);
    private final HttpServer server;
    // A thread per request, so a hanging reply doesn't hold up the next request.
    private final ExecutorService handlers = Executors.newCachedThreadPool();

    FakeApi(Reply... replies) throws IOException {
        this.replies.addAll(List.of(replies));
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.setExecutor(handlers);
        server.start();
        host = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    static Reply reply(int status) {
        return new Reply(status, null, "{\"success\":false,\"code\":" + status + ",\"message\":\"scripted\"}");
    }

    static Reply reply(int status, String retryAfter) {
        return new Reply(status, retryAfter, "{\"success\":false}");
    }

    static Reply ok(String data) {
        return new Reply(200, null, "{\"success\":true,\"code\":200,\"message\":null,\"data\":" + data + "}");
    }

    static Reply[] times(int count, Reply reply) {
        Reply[] replies = new Reply[count];
        java.util.Arrays.fill(replies, reply);
        return replies;
    }

    private void handle(HttpExchange exchange) throws IOException {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        requests.add(new Request(exchange, body));
        Reply reply;
        synchronized (replies) {
            reply = replies.isEmpty() ? null : replies.poll();
        }
        if (reply != null && reply.status == HANG) {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            exchange.close();
            return;
        }
        if (reply != null && reply.status == DISCONNECT) {
            // Closing the exchange without a response drops the connection: the client sees a network error.
            exchange.close();
            return;
        }
        if (reply == null) {
            int accepted = ((List<?>) ((Map<?, ?>) Json.parse(body)).get("batch")).size();
            reply = ok("{\"accepted\":" + accepted + ",\"dropped\":[],\"failed\":[]}");
        }
        byte[] response = reply.body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        if (reply.retryAfter != null) {
            exchange.getResponseHeaders().set("Retry-After", reply.retryAfter);
        }
        exchange.sendResponseHeaders(reply.status, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }

    @Override
    public void close() {
        release.countDown();
        server.stop(0);
        handlers.shutdownNow();
    }

    static final class Reply {
        final int status;
        final String retryAfter;
        final String body;

        Reply(int status, String retryAfter, String body) {
            this.status = status;
            this.retryAfter = retryAfter;
            this.body = body;
        }
    }

    static final class Request {
        final long receivedNanos = System.nanoTime();
        final String method;
        final String path;
        final Map<String, List<String>> headers;
        final String rawBody;
        final List<Map<String, Object>> batch;

        @SuppressWarnings("unchecked")
        Request(HttpExchange exchange, String rawBody) {
            this.method = exchange.getRequestMethod();
            this.path = exchange.getRequestURI().getPath();
            this.headers = exchange.getRequestHeaders();
            this.rawBody = rawBody;
            this.batch = (List<Map<String, Object>>) ((Map<String, Object>) Json.parse(rawBody)).get("batch");
        }

        String header(String name) {
            return headers.entrySet().stream().filter(e -> e.getKey().equalsIgnoreCase(name))
                    .map(e -> e.getValue().get(0)).findFirst().orElse(null);
        }

        List<Object> field(String name) {
            return batch.stream().map(m -> m.get(name)).collect(java.util.stream.Collectors.toList());
        }
    }
}
