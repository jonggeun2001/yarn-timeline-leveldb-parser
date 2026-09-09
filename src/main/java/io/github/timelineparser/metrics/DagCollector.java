package io.github.timelineparser.metrics;

import io.github.timelineparser.domain.DagRecord;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Retains only scalar DAG metadata and requested counters across database scans. */
public final class DagCollector {
    private static final Pattern DAG_ID = Pattern.compile("dag_([0-9]+)_([0-9]+)_([0-9]+)");
    private static final Pattern APP_ID = Pattern.compile("application_[0-9]+_[0-9]+");
    private static final Set<String> NUMERIC_FIELDS = new HashSet<>(Arrays.asList(
            "startTime", "endTime", "numCompletedTasks", "numFailedTaskAttempts"));
    private static final Set<String> TEXT_FIELDS = new HashSet<>(Arrays.asList(
            "applicationId", "user", "queueName", "callerId", "callerType", "status"));
    private static final Set<String> FILTER_FIELDS = new HashSet<>(Arrays.asList(
            "applicationId", "user", "queueName", "callerId", "status"));
    private static final Set<String> TERMINAL_APP_STATES = new HashSet<>(Arrays.asList("FINISHED", "FAILED", "KILLED"));
    private static final Set<String> TERMINAL_DAG_STATES = new HashSet<>(Arrays.asList("SUCCEEDED", "FAILED", "KILLED", "ERROR"));

    private final ResultRowsResolver resolver;
    private final Consumer<String> warningLog;
    private final Map<String, State> dags = new TreeMap<>();
    private final Set<String> observedCompletedApps = new HashSet<>();

    public DagCollector(ResultRowsResolver resolver) { this(resolver, System.err::println); }

    public DagCollector(ResultRowsResolver resolver, Consumer<String> warningLog) {
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.warningLog = Objects.requireNonNull(warningLog, "warningLog");
    }

    /** Unique observed DAGs, including those excluded by application completion filtering. */
    public int getDagCount() { return dags.size(); }

    public void accept(TimelineEntity entity) throws IOException {
        if (entity == null) throw new IOException("Null timeline entity");
        String type = entity.getEntityType();
        if ("YARN_APPLICATION".equals(type)) {
            collectApplication(entity);
            return;
        }
        if (!("TEZ_DAG_ID".equals(type) || "TEZ_DAG_EXTRA_INFO".equals(type))) return;
        String dagId = entity.getEntityId();
        String appId = applicationIdFromDagId(dagId);
        State state = dags.computeIfAbsent(dagId, id -> new State(appId));
        if ("TEZ_DAG_ID".equals(type)) state.hasBaseEntity = true;
        Map<String, Object> info = entity.getOtherInfo();
        for (String name : TEXT_FIELDS) {
            Object value = info.get(name);
            if (value != null) {
                if (!(value instanceof String)) throw new IOException("Expected string at " + dagId + "/" + name);
                merge(state.fields, name, value, dagId);
                if ("applicationId".equals(name)) validateApplication(state, (String) value, dagId);
            }
        }
        for (String name : NUMERIC_FIELDS) {
            Long value = MetricsExtractor.number(info.get(name), dagId + "/" + name);
            if ("startTime".equals(name) || "endTime".equals(name))
                mergeTimestamp(state.fields, name, value, dagId);
            else
                merge(state.fields, name, value, dagId);
        }
        for (String name : FILTER_FIELDS) {
            Set<Object> filter = entity.getPrimaryFilters().get(name);
            if (filter == null) continue;
            for (Object value : filter) {
                if (!(value instanceof String)) throw new IOException("Expected string filter at " + dagId + "/" + name);
                if ("applicationId".equals(name)) validateApplication(state, (String) value, dagId);
                state.filters.computeIfAbsent(name, key -> new TreeSet<>()).add((String) value);
            }
        }
        Set<String> relatedApps = entity.getRelatedEntities().get("TEZ_APPLICATION");
        if (relatedApps != null) for (String related : relatedApps) {
            if (related == null || !related.startsWith("tez_application_"))
                throw new IOException("Invalid TEZ_APPLICATION relationship for " + dagId);
            validateApplication(state, related.substring(4), dagId);
        }
        for (TimelineEvent event : entity.getEvents()) {
            if ("DAG_STARTED".equals(event.getEventType()))
                mergeTimestamp(state.eventTimes, "startTime", event.getTimestamp(), dagId);
            if ("DAG_FINISHED".equals(event.getEventType()))
                mergeTimestamp(state.eventTimes, "endTime", event.getTimestamp(), dagId);
        }
        Object planObject = info.get("dagPlan");
        if (planObject != null) {
            Map<?, ?> plan = objectMap(planObject, dagId + "/dagPlan");
            HiveSqlClassifier.Kind queryKind = HiveQueryMetadata.read(plan);
            if (queryKind != null) {
                state.queryKind = state.queryKind == null || state.queryKind == queryKind
                        ? queryKind : HiveSqlClassifier.Kind.UNSUPPORTED;
            }
            Object contextObject = plan.get("dagContext");
            if (contextObject != null) {
                Map<?, ?> context = objectMap(contextObject, dagId + "/dagContext");
                for (String key : Arrays.asList("callerId", "callerType")) {
                    Object value = context.get(key);
                    if (value != null && !(value instanceof String)) throw new IOException("Invalid caller context for " + dagId);
                    merge(state.fields, key, value, dagId);
                }
            }
        }
        collectCounters(dagId, info.get("counters"), state);
    }

    public List<DagRecord> finish(Set<String> completedApplicationIds) throws IOException {
        Set<String> completed = completedApplicationIds == null ? observedCompletedApps : completedApplicationIds;
        for (String app : completed) if (app == null || !APP_ID.matcher(app).matches())
            throw new IOException("Invalid completed application ID: " + app);
        boolean hasHiveCandidates = dags.values().stream().anyMatch(state -> isHiveCandidate(state.fields));
        boolean hasCompletionEvidence = dags.values().stream()
                .anyMatch(state -> isHiveCandidate(state.fields) && completed.contains(state.applicationId));
        if (completedApplicationIds == null && hasHiveCandidates && !hasCompletionEvidence)
            throw new IOException("DAGs were found but no matching application completion evidence; provide completed application IDs");
        List<DagRecord> records = new ArrayList<>();
        for (Map.Entry<String, State> entry : dags.entrySet()) {
            State state = entry.getValue();
            if (!completed.contains(state.applicationId)) continue;
            Map<String, Object> fields = new HashMap<>(state.fields);
            for (Map.Entry<String, Set<String>> filter : state.filters.entrySet()) {
                if (fields.containsKey(filter.getKey())) {
                    // Status filters can contain historical states; identity filters cannot.
                    if ("status".equals(filter.getKey())) {
                        for (String status : filter.getValue())
                            if (TERMINAL_DAG_STATES.contains(status) && !status.equals(fields.get("status")))
                                throw new IOException("Conflicting terminal status for " + entry.getKey());
                    } else if (!filter.getValue().isEmpty()
                            && !(filter.getValue().size() == 1 && filter.getValue().contains(fields.get(filter.getKey()))))
                        throw new IOException("Conflicting primary filter " + filter.getKey() + " for " + entry.getKey());
                    continue;
                }
                Set<String> candidates = new TreeSet<>(filter.getValue());
                if ("status".equals(filter.getKey()) && candidates.size() > 1) {
                    Set<String> terminal = new TreeSet<>(candidates);
                    terminal.retainAll(TERMINAL_DAG_STATES);
                    if (!terminal.isEmpty()) candidates = terminal;
                }
                if (candidates.size() > 1) throw new IOException("Conflicting primary filter " + filter.getKey() + " for " + entry.getKey());
                if (!candidates.isEmpty()) fields.put(filter.getKey(), candidates.iterator().next());
            }
            for (Map.Entry<String, Long> time : state.eventTimes.entrySet())
                mergeTimestamp(fields, time.getKey(), time.getValue(), entry.getKey());
            if (!isHiveCandidate(fields)) continue;
            if (!state.hasBaseEntity) throw new IOException("Missing TEZ_DAG_ID entity for " + entry.getKey());
            records.add(MetricsExtractor.extract(entry.getKey(), state.applicationId, fields,
                    state.counters, state.queryKind, resolver));
        }
        records.sort(Comparator.comparing((DagRecord record) -> (String) record.get("applicationId"))
                .thenComparing(record -> (String) record.get("dagId")));
        return records;
    }

    /** Missing caller metadata may still belong to Hive; an explicit other framework does not. */
    private static boolean isHiveCandidate(Map<String, Object> fields) {
        Object callerType = fields.get("callerType");
        return callerType == null || "HIVE_QUERY_ID".equals(callerType);
    }

    private void collectApplication(TimelineEntity entity) throws IOException {
        String appId = entity.getEntityId();
        if (appId == null || !APP_ID.matcher(appId).matches()) throw new IOException("Invalid YARN application ID: " + appId);
        for (TimelineEvent event : entity.getEvents()) {
            if ("YARN_APPLICATION_FINISHED".equals(event.getEventType())
                    || ("YARN_APPLICATION_STATE_UPDATED".equals(event.getEventType())
                    && event.getEventInfo() != null
                    && TERMINAL_APP_STATES.contains(String.valueOf(event.getEventInfo().get("YARN_APPLICATION_STATE")))))
                observedCompletedApps.add(appId);
        }
    }

    private void collectCounters(String dagId, Object value, State state) throws IOException {
        if (value == null) return;
        Object groups = objectMap(value, dagId + "/counters").get("counterGroups");
        if (groups == null) return;
        if (!(groups instanceof Collection)) throw new IOException("Invalid counterGroups for " + dagId);
        for (Object groupObject : (Collection<?>) groups) {
            Map<?, ?> group = objectMap(groupObject, dagId + "/counterGroup");
            Object groupName = group.get("counterGroupName");
            if (!(groupName instanceof String)) throw new IOException("Missing counterGroupName for " + dagId);
            Object items = group.get("counters");
            if (items == null) continue;
            if (!(items instanceof Collection)) throw new IOException("Invalid counters list for " + dagId);
            for (Object item : (Collection<?>) items) {
                Map<?, ?> counter = objectMap(item, dagId + "/counter");
                Object counterName = counter.get("counterName");
                if (!(counterName instanceof String)) throw new IOException("Missing counterName for " + dagId);
                String groupText = (String) groupName;
                String name = (String) counterName;
                if (!MetricsExtractor.needsCounter(groupText, name) && !resolver.needsCounter(dagId, groupText, name)) continue;
                String key = MetricsExtractor.counterKey(groupText, name);
                Long count = MetricsExtractor.number(counter.get("counterValue"), dagId + "/" + groupText + "/" + name);
                if (count == null) throw new IOException("Missing counterValue at " + dagId + "/" + groupText + "/" + name);
                merge(state.counters, key, count, dagId);
            }
        }
    }

    static String applicationIdFromDagId(String dagId) throws IOException {
        Matcher match = DAG_ID.matcher(dagId == null ? "" : dagId);
        if (!match.matches()) throw new IOException("Invalid Tez DAG ID: " + dagId);
        try {
            long timestamp = Long.parseLong(match.group(1));
            int app = new BigInteger(match.group(2)).intValueExact();
            new BigInteger(match.group(3)).intValueExact();
            return "application_" + timestamp + "_" + String.format(Locale.ROOT, "%04d", app);
        } catch (ArithmeticException | NumberFormatException e) {
            throw new IOException("Out-of-range Tez DAG ID: " + dagId, e);
        }
    }
    private static void validateApplication(State state, String value, String dagId) throws IOException {
        if (!state.applicationId.equals(value)) throw new IOException("Conflicting application identity for " + dagId);
    }
    private static Map<?, ?> objectMap(Object value, String location) throws IOException {
        if (!(value instanceof Map)) throw new IOException("Expected object at " + location);
        return (Map<?, ?>) value;
    }
    /** Lifecycle times use the latest timestamp, regardless of database or event iteration order. */
    private void mergeTimestamp(Map<String, ? super Long> target, String key, Long value, String dagId) {
        if (value == null) return;
        Long previous = (Long) target.get(key);
        long selected = value;
        if (previous != null && !previous.equals(value)) {
            selected = Math.max(previous, value);
            warningLog.accept("WARN Conflicting timestamp at " + dagId + "/" + key
                    + ": previous=" + previous + " incoming=" + value + " selected=" + selected);
        }
        target.put(key, selected);
    }

    private static <T> void merge(Map<String, T> target, String key, T value, String dagId) throws IOException {
        if (value == null) return;
        T previous = target.get(key);
        if (previous != null && !previous.equals(value)) throw new IOException("Conflicting value at " + dagId + "/" + key.replace('\u0000', '/'));
        target.put(key, value);
    }
    private static final class State {
        private final String applicationId;
        private HiveSqlClassifier.Kind queryKind;
        private final Map<String, Object> fields = new HashMap<>();
        private final Map<String, Set<String>> filters = new HashMap<>();
        private final Map<String, Long> eventTimes = new HashMap<>();
        private final Map<String, Long> counters = new HashMap<>();
        private boolean hasBaseEntity;
        private State(String applicationId) { this.applicationId = applicationId; }
    }
}
