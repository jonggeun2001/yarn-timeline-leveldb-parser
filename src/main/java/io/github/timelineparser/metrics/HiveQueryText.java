package io.github.timelineparser.metrics;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;

/** Preserves the SQL recorded by Hive without parsing or rewriting the statement. */
final class HiveQueryText {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    private HiveQueryText() { }

    static String read(Map<?, ?> plan) throws IOException {
        String raw = readDagInfo(plan.get("dagInfo"));
        if (raw != null) return raw;
        Object value = plan.get("dagContext");
        if (value == null) return null;
        if (!(value instanceof Map)) throw invalid();
        Map<?, ?> context = (Map<?, ?>) value;
        if (!isHive(context.get("context")) && !"HIVE_QUERY_ID".equals(context.get("callerType"))) return null;
        return query(context.get("description"));
    }

    private static String readDagInfo(Object value) throws IOException {
        if (value == null) return null;
        if (!(value instanceof String)) throw invalid();
        try (JsonParser parser = JSON.getFactory().createParser((String) value)) {
            JsonNode info = JSON.readTree(parser);
            if (info == null || !info.isObject() || parser.nextToken() != null) throw invalid();
            JsonNode context = info.get("context");
            if (context == null || !context.isTextual() || !isHive(context.asText())) return null;
            JsonNode description = info.get("description");
            if (description == null || description.isNull()) return null;
            if (!description.isTextual()) throw invalid();
            return query(description.asText());
        } catch (IOException exception) {
            // JSON parser errors may contain SQL. Report only the metadata location upstream.
            throw invalid();
        }
    }

    private static String query(Object value) throws IOException {
        if (value == null) return null;
        if (!(value instanceof String) || ((String) value).trim().isEmpty()) throw invalid();
        return (String) value;
    }

    private static boolean isHive(Object value) {
        return value instanceof String && "Hive".equalsIgnoreCase((String) value);
    }

    private static IOException invalid() { return new IOException("Malformed Hive query metadata"); }
}
