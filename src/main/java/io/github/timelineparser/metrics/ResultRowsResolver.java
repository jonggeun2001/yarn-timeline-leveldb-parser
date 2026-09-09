package io.github.timelineparser.metrics;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/** Reads an unambiguous Hive FileSink counter, with optional per-DAG overrides. */
public final class ResultRowsResolver {
    // Hive FileSinkOperator uses the numeric destination ID, optionally followed by the table name.
    private static final Pattern FILE_SINK_COUNTER = Pattern.compile("RECORDS_OUT_[0-9]+(?:_.+)?");
    private final Map<String, Mapping> mappings;

    public ResultRowsResolver() { this.mappings = Collections.emptyMap(); }
    private ResultRowsResolver(Map<String, Mapping> mappings) {
        this.mappings = Collections.unmodifiableMap(new HashMap<>(mappings));
    }

    public static ResultRowsResolver fromFile(Path path) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
        JsonNode root;
        try (JsonParser parser = mapper.getFactory().createParser(path.toFile())) {
            root = mapper.readTree(parser);
            if (parser.nextToken() != null) throw new IOException("Trailing content in row mapping: " + path);
        }
        if (root == null || !root.isObject()) throw new IOException("Row mapping must be a JSON object: " + path);
        Map<String, Mapping> mappings = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> entries = root.fields();
        while (entries.hasNext()) {
            Map.Entry<String, JsonNode> entry = entries.next();
            String dagId = entry.getKey();
            DagCollector.applicationIdFromDagId(dagId);
            JsonNode value = entry.getValue();
            if (!value.isObject() || value.size() != 3)
                throw new IOException("Expected kind, counterGroup and counterName for " + dagId);
            String kind = requiredText(value, "kind", dagId);
            String group = requiredText(value, "counterGroup", dagId);
            String counter = requiredText(value, "counterName", dagId);
            if (!("SELECT_RESULT".equals(kind) || "CTAS_WRITE".equals(kind)))
                throw new IOException("Unsupported resultRows kind for " + dagId);
            if (!counter.startsWith("RECORDS_OUT_") || counter.length() == "RECORDS_OUT_".length())
                throw new IOException("Expected an exact FileSink RECORDS_OUT counter for " + dagId);
            mappings.put(dagId, new Mapping(kind, group, counter));
        }
        return new ResultRowsResolver(mappings);
    }

    private static String requiredText(JsonNode value, String key, String dagId) throws IOException {
        JsonNode text = value.get(key);
        if (text == null || !text.isTextual() || text.asText().trim().isEmpty())
            throw new IOException("Missing text " + key + " for " + dagId);
        return text.asText();
    }

    boolean needsCounter(String dagId, String group, String name) {
        Mapping mapping = mappings.get(dagId);
        return mapping == null ? FILE_SINK_COUNTER.matcher(name).matches()
                : mapping.group.equals(group) && mapping.counter.equals(name);
    }

    void resolve(String dagId, String status, Map<String, Long> counters, Map<String, Object> result) {
        if (!"SUCCEEDED".equals(status)) return;
        Mapping mapping = mappings.get(dagId);
        if (mapping != null) {
            setResult(counters.get(MetricsExtractor.counterKey(mapping.group, mapping.counter)),
                    mapping.kind, mapping.counter, result);
            return;
        }

        String selectedName = null;
        Long selectedCount = null;
        for (Map.Entry<String, Long> counter : counters.entrySet()) {
            String name = counter.getKey().substring(counter.getKey().indexOf('\u0000') + 1);
            if (!FILE_SINK_COUNTER.matcher(name).matches()) continue;
            // Distinct groups also represent distinct candidates. Never sum or choose the largest.
            if (selectedName != null) return;
            selectedName = name;
            selectedCount = counter.getValue();
        }
        setResult(selectedCount, "FILE_SINK_OUTPUT", selectedName, result);
    }

    private static void setResult(Long count, String kind, String counter, Map<String, Object> result) {
        if (count == null || count < 0) return;
        result.put("resultRows", count);
        result.put("resultRowsKind", kind);
        result.put("resultRowsSource", counter);
    }

    private static final class Mapping {
        private final String kind;
        private final String group;
        private final String counter;
        private Mapping(String kind, String group, String counter) {
            this.kind = kind;
            this.group = group;
            this.counter = counter;
        }
    }
}
