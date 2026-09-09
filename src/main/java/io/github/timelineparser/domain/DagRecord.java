package io.github.timelineparser.domain;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable scalar values ready for the Parquet schema. */
public final class DagRecord {
    private final Map<String, Object> values;

    public DagRecord(Map<String, Object> values) {
        this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    public Object get(String name) { return values.get(name); }
    public Map<String, Object> getValues() { return values; }
}
