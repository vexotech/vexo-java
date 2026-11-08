package co.vexo.server;

import java.time.Duration;

/**
 * Run in a child JVM by VexoTest: queues one event and returns from main without calling close(), so only the
 * shutdown hook can send it, and only a daemon worker lets the JVM exit. Arguments: host, shutdown timeout in ms.
 */
public final class ExitHookMain {
    public static void main(String[] args) {
        Vexo vexo = Vexo.builder("test-key")
                .host(args[0])
                .shutdownTimeout(Duration.ofMillis(Long.parseLong(args[1])))
                .timeout(Duration.ofSeconds(30))
                .flushInterval(Duration.ofMinutes(1))
                .build();
        vexo.capture("u", "sent_by_the_shutdown_hook");
    }
}
