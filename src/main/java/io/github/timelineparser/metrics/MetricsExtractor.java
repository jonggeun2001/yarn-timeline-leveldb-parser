package io.github.timelineparser.metrics;

import io.github.timelineparser.domain.DagRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

/** Tez 0.9.1 names, independent of display names and of task/vertex aggregates. */
final class MetricsExtractor {
    static final String TASK_GROUP = "org.apache.tez.common.counters.TaskCounter";
    static final String FS_GROUP = "org.apache.tez.common.counters.FileSystemCounter";
    private static final Map<String, String> COUNTERS = new LinkedHashMap<>();
    static {
        COUNTERS.put("cpuMilliseconds", counterKey(TASK_GROUP, "CPU_MILLISECONDS"));
        COUNTERS.put("gcMilliseconds", counterKey(TASK_GROUP, "GC_TIME_MILLIS"));
        COUNTERS.put("hdfsBytesRead", counterKey(FS_GROUP, "HDFS_BYTES_READ"));
        COUNTERS.put("hdfsBytesWritten", counterKey(FS_GROUP, "HDFS_BYTES_WRITTEN"));
        COUNTERS.put("shuffleBytes", counterKey(TASK_GROUP, "SHUFFLE_BYTES"));
        COUNTERS.put("additionalSpillBytesWritten", counterKey(TASK_GROUP, "ADDITIONAL_SPILLS_BYTES_WRITTEN"));
    }

    private MetricsExtractor() { }
    static String counterKey(String group, String name) { return group + "\u0000" + name; }
    static boolean needsCounter(String group, String name) { return COUNTERS.containsValue(counterKey(group, name)); }

    static DagRecord extract(String dagId, String applicationId, Map<String, Object> fields,
                             Map<String, Long> counters, ResultRowsResolver resolver) throws IOException {
        return extract(dagId, applicationId, fields, counters, null, resolver, true);
    }

    static DagRecord extract(String dagId, String applicationId, Map<String, Object> fields,
                             Map<String, Long> counters, HiveSqlClassifier.Kind queryKind, ResultRowsResolver resolver,
                             boolean automaticRowsTrusted) throws IOException {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("dagId", dagId);
        result.put("applicationId", applicationId);
        result.put("user", fields.get("user"));
        result.put("hiveQueryId", "HIVE_QUERY_ID".equals(fields.get("callerType")) ? fields.get("callerId") : null);
        Long start = nonnegative(number(fields.get("startTime"), dagId + "/startTime"));
        Long end = nonnegative(number(fields.get("endTime"), dagId + "/endTime"));
        if (start != null && end != null && end < start) end = null;
        result.put("startTime", start);
        result.put("endTime", end);
        result.put("resultRows", null);
        result.put("cpuMilliseconds", nonnegative(counters.get(COUNTERS.get("cpuMilliseconds"))));
        result.put("status", fields.get("status"));
        result.put("queueName", fields.get("queueName"));
        result.put("durationMilliseconds", start != null && end != null ? end - start : null);
        for (Map.Entry<String, String> counter : COUNTERS.entrySet())
            result.put(counter.getKey(), nonnegative(counters.get(counter.getValue())));
        // Tez 0.9.1 writes ProgressBuilder.totalTaskCount as numCompletedTasks.
        result.put("totalTasks", nonnegative(number(fields.get("numCompletedTasks"), dagId + "/numCompletedTasks")));
        result.put("failedTaskAttempts", nonnegative(number(fields.get("numFailedTaskAttempts"), dagId + "/numFailedTaskAttempts")));
        result.put("resultRowsKind", null);
        result.put("resultRowsSource", null);
        resolver.resolve(dagId, (String) fields.get("status"), counters, queryKind, result, automaticRowsTrusted);
        return new DagRecord(result);
    }

    static Long number(Object value, String location) throws IOException {
        if (value == null) return null;
        if (!(value instanceof Number)) throw new IOException("Expected integer at " + location);
        try {
            return new BigDecimal(value.toString()).longValueExact();
        } catch (ArithmeticException | NumberFormatException e) {
            throw new IOException("Invalid INT64 at " + location, e);
        }
    }
    private static Long nonnegative(Long value) { return value == null || value < 0 ? null : value; }
}
