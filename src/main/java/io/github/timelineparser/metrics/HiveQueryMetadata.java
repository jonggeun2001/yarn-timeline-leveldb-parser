package io.github.timelineparser.metrics;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

import static io.github.timelineparser.metrics.HiveSqlClassifier.Kind;
import static io.github.timelineparser.metrics.HiveSqlClassifier.Kind.UNSUPPORTED;

/** Reads only the Hive statement kind from Tez ATS DAG metadata. */
final class HiveQueryMetadata {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    private HiveQueryMetadata() { }

    static Kind read(Map<?, ?> dagPlan) {
        if (dagPlan == null) return null;
        Kind raw = readDagInfo(dagPlan.get("dagInfo"));
        if (raw == UNSUPPORTED) return UNSUPPORTED;
        Kind context = readDagContext(dagPlan.get("dagContext"));
        if (raw == null) return context;
        // Hive's caller context can be redacted; invalid context text must not replace valid raw SQL.
        if (context != null && context != UNSUPPORTED && context != raw) return UNSUPPORTED;
        return raw;
    }

    private static Kind readDagInfo(Object value) {
        if (value == null) return null;
        if (!(value instanceof String)) return UNSUPPORTED;
        try (JsonParser parser = JSON.getFactory().createParser((String) value)) {
            JsonNode info = JSON.readTree(parser);
            if (info == null || !info.isObject() || parser.nextToken() != null) return UNSUPPORTED;
            JsonNode context = info.get("context");
            if (context == null || !context.isTextual() || !isHive(context.asText())) return null;
            JsonNode description = info.get("description");
            if (description == null || description.isNull()) return null;
            return description.isTextual() ? classify(description.asText()) : UNSUPPORTED;
        } catch (IOException ignored) {
            // Parser exceptions can include query text. Never log or retain them.
            return UNSUPPORTED;
        }
    }

    private static Kind readDagContext(Object value) {
        if (!(value instanceof Map)) return null;
        Map<?, ?> context = (Map<?, ?>) value;
        if (!isHive(context.get("context")) && !"HIVE_QUERY_ID".equals(context.get("callerType"))) return null;
        // Tez's ATS conversion renames CallerContext.blob to description.
        Object description = context.get("description");
        if (description == null) return null;
        return description instanceof String ? classify((String) description) : UNSUPPORTED;
    }

    private static boolean isHive(Object context) {
        return context instanceof String && "Hive".equalsIgnoreCase((String) context);
    }

    private static Kind classify(String sql) {
        return sql.trim().isEmpty() ? null : HiveSqlClassifier.classify(sql);
    }
}
