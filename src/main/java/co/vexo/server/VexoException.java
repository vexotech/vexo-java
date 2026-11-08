package co.vexo.server;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Why messages weren't stored. The client never throws it: it passes it to the error callback that you set with
 * {@link Vexo.Builder#onError}.
 */
public final class VexoException extends RuntimeException {
    private final int status;
    private final List<Map<String, Object>> batch;
    private final List<Dropped> dropped;

    VexoException(String message, Throwable cause) {
        this(message, cause, 0, List.of(), List.of());
    }

    VexoException(String message, Throwable cause, int status, List<Message> batch) {
        this(message, cause, status, batch, List.of());
    }

    VexoException(String message, List<Dropped> dropped) {
        this(message, null, 0, List.of(), dropped);
    }

    private VexoException(String message, Throwable cause, int status, List<Message> batch, List<Dropped> dropped) {
        super(message, cause);
        this.status = status;
        this.batch = batch.stream().map(m -> m.fields).collect(Collectors.toUnmodifiableList());
        this.dropped = List.copyOf(dropped);
    }

    /** The HTTP status of a rejected batch, or 0 when the error has no HTTP status. */
    public int getStatus() {
        return status;
    }

    /**
     * The messages that weren't sent: a batch that failed after its retries or was rejected, or what was still
     * queued at the shutdown deadline. Empty for other errors.
     */
    public List<Map<String, Object>> getBatch() {
        return batch;
    }

    /**
     * The messages the API dropped as invalid, or reported as failed and that still failed after the retries, each
     * with the API's reason. Empty for other errors.
     */
    public List<Dropped> getDropped() {
        return dropped;
    }

    /** A message the API didn't store, and why. */
    public static final class Dropped {
        private final Map<String, Object> message;
        private final String reason;

        Dropped(Map<String, Object> message, String reason) {
            this.message = message;
            this.reason = reason;
        }

        /** The message as it was queued, including its {@code messageId}. */
        public Map<String, Object> getMessage() {
            return message;
        }

        /** The API's reason, for example {@code event is required}. */
        public String getReason() {
            return reason;
        }

        @Override
        public String toString() {
            return reason + ": " + message;
        }
    }
}
