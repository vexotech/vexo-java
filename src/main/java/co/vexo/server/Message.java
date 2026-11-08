package co.vexo.server;

import java.util.Collections;
import java.util.Map;

/** A queued message: its fields, for error reports, and its JSON, serialized once at enqueue and sent as is. */
final class Message {
    final Map<String, Object> fields;
    final byte[] json;

    Message(Map<String, Object> fields, byte[] json) {
        this.fields = Collections.unmodifiableMap(fields);
        this.json = json;
    }
}
