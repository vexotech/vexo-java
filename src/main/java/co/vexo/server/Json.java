package co.vexo.server;

import java.lang.reflect.Array;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The JSON this SDK needs, and no more: writing a message, and reading the API's response. Not a general JSON library.
 */
final class Json {
    // Deep enough for any real event; a circular reference hits it instead of overflowing the stack.
    static final int MAX_DEPTH = 100;

    private Json() {
    }

    /**
     * Writes maps, collections, arrays, strings, numbers, booleans and null. Like the reference SDKs, any other value
     * (BigDecimal, UUID, java.time types, enums) is written as its string; a java.util.Date as an ISO 8601 instant.
     *
     * @throws IllegalArgumentException for a value JSON can't hold: NaN, an infinity, or a circular reference
     */
    static String write(Object value) {
        StringBuilder out = new StringBuilder();
        write(value, out, 0);
        return out.toString();
    }

    private static void write(Object value, StringBuilder out, int depth) {
        if (depth > MAX_DEPTH) {
            throw new IllegalArgumentException("nested deeper than " + MAX_DEPTH + " levels (a circular reference?)");
        }
        if (value == null || value instanceof Boolean || isIntegral(value)) {
            out.append(value);
        } else if (value instanceof Double || value instanceof Float) {
            if (!Double.isFinite(((Number) value).doubleValue())) {
                throw new IllegalArgumentException(value + " isn't a JSON number");
            }
            out.append(value);
        } else if (value instanceof Map) {
            out.append('{');
            String separator = "";
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                out.append(separator);
                string(String.valueOf(entry.getKey()), out);
                out.append(':');
                write(entry.getValue(), out, depth + 1);
                separator = ",";
            }
            out.append('}');
        } else if (value instanceof Collection) {
            array(((Collection<?>) value).toArray(), out, depth);
        } else if (value.getClass().isArray()) {
            Object[] items = new Object[Array.getLength(value)];
            for (int i = 0; i < items.length; i++) {
                items[i] = Array.get(value, i);
            }
            array(items, out, depth);
        } else if (value instanceof Date) {
            // getTime(), not toInstant(): java.sql.Date doesn't support toInstant().
            string(Instant.ofEpochMilli(((Date) value).getTime()).toString(), out);
        } else {
            string(value.toString(), out);
        }
    }

    // The number types whose toString() is always a valid JSON number. BigDecimal is written as a string instead.
    private static boolean isIntegral(Object value) {
        return value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte
                || value instanceof BigInteger || value instanceof AtomicInteger || value instanceof AtomicLong;
    }

    private static void array(Object[] items, StringBuilder out, int depth) {
        out.append('[');
        for (int i = 0; i < items.length; i++) {
            if (i > 0) {
                out.append(',');
            }
            write(items[i], out, depth + 1);
        }
        out.append(']');
    }

    private static void string(String value, StringBuilder out) {
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"' || c == '\\') {
                out.append('\\').append(c);
            } else if (c < 0x20) {
                out.append(String.format("\\u%04x", (int) c));
            } else {
                out.append(c);
            }
        }
        out.append('"');
    }

    /**
     * Reads a JSON document into maps, lists, strings, doubles, booleans and null.
     *
     * @throws IllegalArgumentException when the text isn't JSON
     */
    static Object parse(String json) {
        Reader reader = new Reader(json);
        Object value = reader.value();
        reader.skipWhitespace();
        if (reader.position < json.length()) {
            throw reader.error();
        }
        return value;
    }

    private static final class Reader {
        private final String text;
        private int position;

        Reader(String text) {
            this.text = text;
        }

        Object value() {
            skipWhitespace();
            switch (peek()) {
                case '{':
                    return object();
                case '[':
                    return array();
                case '"':
                    return string();
                case 't':
                    return literal("true", Boolean.TRUE);
                case 'f':
                    return literal("false", Boolean.FALSE);
                case 'n':
                    return literal("null", null);
                default:
                    return number();
            }
        }

        private Map<String, Object> object() {
            Map<String, Object> object = new LinkedHashMap<>();
            expect('{');
            skipWhitespace();
            if (peek() == '}') {
                position++;
                return object;
            }
            do {
                skipWhitespace();
                String key = string();
                skipWhitespace();
                expect(':');
                object.put(key, value());
                skipWhitespace();
            } while (consume(','));
            expect('}');
            return object;
        }

        private List<Object> array() {
            List<Object> array = new ArrayList<>();
            expect('[');
            skipWhitespace();
            if (peek() == ']') {
                position++;
                return array;
            }
            do {
                array.add(value());
                skipWhitespace();
            } while (consume(','));
            expect(']');
            return array;
        }

        private String string() {
            expect('"');
            StringBuilder out = new StringBuilder();
            for (char c = next(); c != '"'; c = next()) {
                if (c != '\\') {
                    out.append(c);
                    continue;
                }
                char escaped = next();
                switch (escaped) {
                    case 'u':
                        if (position + 4 > text.length()) {
                            throw error();
                        }
                        out.append((char) Integer.parseInt(text.substring(position, position + 4), 16));
                        position += 4;
                        break;
                    case 'b':
                        out.append('\b');
                        break;
                    case 'f':
                        out.append('\f');
                        break;
                    case 'n':
                        out.append('\n');
                        break;
                    case 'r':
                        out.append('\r');
                        break;
                    case 't':
                        out.append('\t');
                        break;
                    default:
                        out.append(escaped);
                }
            }
            return out.toString();
        }

        private Object literal(String word, Object value) {
            if (!text.startsWith(word, position)) {
                throw error();
            }
            position += word.length();
            return value;
        }

        private Double number() {
            int start = position;
            while (position < text.length() && "+-0123456789.eE".indexOf(text.charAt(position)) >= 0) {
                position++;
            }
            try {
                return Double.valueOf(text.substring(start, position));
            } catch (NumberFormatException e) {
                throw error();
            }
        }

        void skipWhitespace() {
            while (position < text.length() && Character.isWhitespace(text.charAt(position))) {
                position++;
            }
        }

        private boolean consume(char c) {
            if (position < text.length() && text.charAt(position) == c) {
                position++;
                return true;
            }
            return false;
        }

        private void expect(char c) {
            if (!consume(c)) {
                throw error();
            }
        }

        private char peek() {
            if (position >= text.length()) {
                throw error();
            }
            return text.charAt(position);
        }

        private char next() {
            char c = peek();
            position++;
            return c;
        }

        IllegalArgumentException error() {
            return new IllegalArgumentException("invalid JSON at position " + position);
        }
    }
}
