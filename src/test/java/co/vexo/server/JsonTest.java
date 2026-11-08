package co.vexo.server;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonTest {
    @Test
    void writesScalars() {
        assertEquals("null", Json.write(null));
        assertEquals("true", Json.write(true));
        assertEquals("42", Json.write(42));
        assertEquals("-7", Json.write(-7L));
        assertEquals("49.9", Json.write(49.9));
        assertEquals("1.5", Json.write(1.5f));
        assertEquals("1.0E20", Json.write(1e20));
        assertEquals("123456789012345678901234567890", Json.write(new BigInteger("123456789012345678901234567890")));
        assertEquals("3", Json.write(new AtomicLong(3)));
    }

    @Test
    void escapesStrings() {
        assertEquals("\"a\\\"b\\\\c\\u000a\\u0001\\u001f é 😀 /\"", Json.write("a\"b\\c\n\u0001\u001f é 😀 /"));
        assertEquals("\"x\"", Json.write('x'));
    }

    @Test
    void replacesLoneSurrogatesWithReplacementCharacters() {
        assertEquals("\"ok\uFFFD \uFFFDx \uD83D\uDE00 \uFFFD\"", Json.write("ok\uD800 \uDC00x \uD83D\uDE00 \uD83D"));
        assertEquals("{\"k\uFFFD\":1}", Json.write(Map.of("k\uDFFF", 1)));
    }

    @Test
    void writesBigDecimalUuidJavaTimeEnumsAndDatesAsStrings() {
        assertEquals("\"9.90\"", Json.write(new BigDecimal("9.90")));
        assertEquals("\"0.00000012345678901234567\"", Json.write(new BigDecimal("1.2345678901234567E-7")));
        assertEquals("\"1000\"", Json.write(new BigDecimal("1E+3")));
        assertEquals("\"5b0c8f0e-6d1e-4f62-9d55-3f1f0f6a2b71\"", Json.write(UUID.fromString("5b0c8f0e-6d1e-4f62-9d55-3f1f0f6a2b71")));
        assertEquals("\"2026-11-07T12:00:00.000Z\"", Json.write(Instant.parse("2026-11-07T12:00:00Z")));
        assertEquals("\"2026-11-07T12:00:00.123Z\"", Json.write(Instant.parse("2026-11-07T12:00:00.123456789Z")));
        assertEquals("\"2026-11-07T10:00:00.000Z\"", Json.write(OffsetDateTime.parse("2026-11-07T12:00:00+02:00")));
        assertEquals("\"2026-11-07T11:00:00.000Z\"", Json.write(ZonedDateTime.parse("2026-11-07T12:00:00+01:00[Europe/Paris]")));
        assertEquals("\"2026-11-07T12:00:00.000Z\"", Json.write(LocalDateTime.of(2026, 11, 7, 12, 0)));
        assertEquals("\"2026-11-07\"", Json.write(LocalDate.of(2026, 11, 7)));
        assertEquals("\"PT1H\"", Json.write(Duration.ofHours(1)));
        assertEquals("\"SECONDS\"", Json.write(TimeUnit.SECONDS));
        assertEquals("\"1970-01-01T00:00:00.001Z\"", Json.write(new Date(1)));
    }

    @Test
    void writesMapsCollectionsAndArrays() {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put("list", List.of(1, "two"));
        map.put(7, new int[] {1, 2});
        map.put("objects", new Object[] {null, false});
        map.put("empty", Map.of());
        map.put("nested", Map.of("deeper", List.of(Map.of())));

        assertEquals("{\"list\":[1,\"two\"],\"7\":[1,2],\"objects\":[null,false],\"empty\":{},\"nested\":{\"deeper\":[{}]}}",
                Json.write(map));
    }

    @Test
    void rejectsValuesJsonCannotHold() {
        assertThrows(IllegalArgumentException.class, () -> Json.write(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> Json.write(Float.POSITIVE_INFINITY));
        assertThrows(IllegalArgumentException.class, () -> Json.write(List.of(Double.NEGATIVE_INFINITY)));
        List<Object> circular = new ArrayList<>();
        circular.add(circular);
        assertThrows(IllegalArgumentException.class, () -> Json.write(circular));
    }

    @Test
    void readsTheApiResponse() {
        Object parsed = Json.parse(" {\"success\":true,\"code\":200,\"message\":null,\"data\":{\"accepted\":1,"
                + "\"dropped\":[{\"index\":1,\"reason\":\"caf\\u00e9 \\\"quoted\\\" \\\\ \\/ \\n\"}],\"failed\":[]}} ");

        Map<String, Object> dropped = Map.of("index", 1.0, "reason", "café \"quoted\" \\ / \n");
        Map<String, Object> data = Map.of("accepted", 1.0, "dropped", List.of(dropped), "failed", List.of());
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("success", true);
        envelope.put("code", 200.0);
        envelope.put("message", null);
        envelope.put("data", data);
        assertEquals(envelope, parsed);
    }

    @Test
    void readsWhatItWrites() {
        Map<String, Object> message = new LinkedHashMap<>();
        message.put("text", "line\nbreak \u0000 \"q\" é");
        message.put("numbers", List.of(-1.5, 2e-3, 0.0));
        message.put("none", null);
        assertEquals(message, Json.parse(Json.write(message)));
    }

    @Test
    void rejectsInvalidJson() {
        for (String text : List.of("", "{", "[1,]", "{\"a\" 1}", "{a:1}", "nul", "{} x", "\"open", "\"\\u12\"", "-")) {
            assertThrows(IllegalArgumentException.class, () -> Json.parse(text), text);
        }
    }
}
