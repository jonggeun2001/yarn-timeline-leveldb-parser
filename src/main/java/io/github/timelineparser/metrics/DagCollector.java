package io.github.timelineparser.metrics;

import io.github.timelineparser.diagnostics.Diagnostics;
import io.github.timelineparser.domain.DagRecord;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Retains only scalar DAG metadata and requested counters across database scans. */
public final class DagCollector {
    private static final Logger LOG = LoggerFactory.getLogger(DagCollector.class);
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
    private boolean discardedEntities;

    public DagCollector(ResultRowsResolver resolver) { this(resolver, null); }

    public DagCollector(ResultRowsResolver resolver, Consumer<String> warningLog) {
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.warningLog = warningLog == null ? LOG::warn : warningLog;
    }

    /** Unique observed DAGs, including those excluded by application completion filtering. */
    public int getDagCount() { return dags.size(); }

    /** Distinguishes damaged input discarded during collection from a valid empty result. */
    public boolean hasDiscardedEntities() { return discardedEntities; }

    public void accept(TimelineEntity entity) throws IOException {
        if (entity == null) {
            discard("unknown", "entity", "null timeline entity");
            return;
        }
        String type = entity.getEntityType();
        if ("YARN_APPLICATION".equals(type)) {
            collectApplication(entity);
            return;
        }
        if (!("TEZ_DAG_ID".equals(type) || "TEZ_DAG_EXTRA_INFO".equals(type))) return;
        String dagId = entity.getEntityId();
        String appId;
        try {
            appId = applicationIdFromDagId(dagId);
        } catch (IOException e) {
            discard(dagId, "dagId", Diagnostics.describe(e));
            return;
        }
        State state = dags.computeIfAbsent(dagId, id -> new State(appId));
        if (state.quarantined) return;
        try {
            collectDag(entity, type, dagId, state);
        } catch (RuntimeException failure) {
            // Scanner recovery cannot roll back state already merged from this entity.
            quarantine(state, dagId, "entity", Diagnostics.describe(failure));
            LOG.warn("Cannot read DAG entity dag=" + Diagnostics.singleLine(dagId) + " action=skip_dag", failure);
        }
    }

    private void collectDag(TimelineEntity entity, String type, String dagId, State state) {
        if ("TEZ_DAG_ID".equals(type)) state.hasBaseEntity = true;
        Map<?, ?> info = emptyIfNull(entity.getOtherInfo());
        for (String name : TEXT_FIELDS) {
            Object value = info.get(name);
            if (value == null) continue;
            if ("applicationId".equals(name)) {
                if (!validateApplication(state, value, dagId)) return;
            } else {
                collectText(state, name, value, dagId);
            }
        }
        for (String name : NUMERIC_FIELDS) {
            Object value = info.get(name);
            if (value == null || state.invalidFields.contains(name)) continue;
            try {
                Long number = nonnegativeNumber(value, dagId + "/" + name);
                if ("startTime".equals(name) || "endTime".equals(name))
                    mergeTimestamp(state.fields, name, number, dagId);
                else
                    merge(state.fields, state.invalidFields, name, number, dagId);
            } catch (IOException e) {
                invalidateField(state, name, dagId, Diagnostics.describe(e));
            }
        }
        Map<?, ?> filters = emptyIfNull(entity.getPrimaryFilters());
        for (String name : FILTER_FIELDS) {
            Object filter = filters.get(name);
            if (filter == null) continue;
            if (!(filter instanceof Collection)) {
                if ("applicationId".equals(name)) {
                    quarantine(state, dagId, name, "expected filter values");
                    return;
                }
                invalidateField(state, name, dagId, "expected filter values");
                continue;
            }
            for (Object value : (Collection<?>) filter) {
                if ("applicationId".equals(name)) {
                    if (!validateApplication(state, value, dagId)) return;
                } else if (!isText(value)) {
                    invalidateField(state, name, dagId, "expected nonempty string filter");
                } else if (!state.invalidFields.contains(name)) {
                    state.filters.computeIfAbsent(name, key -> new TreeSet<>()).add((String) value);
                }
            }
        }
        Object relatedApps = emptyIfNull(entity.getRelatedEntities()).get("TEZ_APPLICATION");
        if (relatedApps != null) {
            if (!(relatedApps instanceof Collection)) {
                quarantine(state, dagId, "TEZ_APPLICATION", "expected relationship values");
                return;
            }
            for (Object related : (Collection<?>) relatedApps) {
                if (!(related instanceof String) || !((String) related).startsWith("tez_application_")) {
                    quarantine(state, dagId, "TEZ_APPLICATION", "invalid application relationship");
                    return;
                }
                if (!validateApplication(state, ((String) related).substring(4), dagId)) return;
            }
        }
        for (Object eventObject : emptyIfNull(entity.getEvents())) {
            if (!(eventObject instanceof TimelineEvent)) {
                warn(dagId, "events", "invalid event", "skip_event");
                continue;
            }
            TimelineEvent event = (TimelineEvent) eventObject;
            String name = "DAG_STARTED".equals(event.getEventType()) ? "startTime"
                    : "DAG_FINISHED".equals(event.getEventType()) ? "endTime" : null;
            if (name == null || state.invalidFields.contains(name)) continue;
            if (event.getTimestamp() < 0) invalidateField(state, name, dagId, "negative event timestamp");
            else mergeTimestamp(state.eventTimes, name, event.getTimestamp(), dagId);
        }
        collectQuery(dagId, info.get("dagPlan"), state);
        collectCallerContext(dagId, info.get("dagPlan"), state);
        try {
            collectCounters(dagId, info.get("counters"), state);
        } catch (RuntimeException failure) {
            // A partially read counter collection cannot establish a unique FileSink.
            invalidateAutomaticRows(state, dagId, "counters", Diagnostics.describe(failure));
            LOG.warn("Cannot read DAG counters dag=" + Diagnostics.singleLine(dagId) + " action=null", failure);
        }
    }

    public List<DagRecord> finish(Set<String> completedApplicationIds) throws IOException {
        Set<String> completed = completedApplicationIds == null ? observedCompletedApps : completedApplicationIds;
        for (String app : completed) if (app == null || !APP_ID.matcher(app).matches())
            throw new IOException("Invalid completed application ID: " + app);
        boolean hasHiveCandidates = dags.values().stream().anyMatch(state -> state.hasBaseEntity && isHiveCandidate(state));
        boolean hasCompletionEvidence = dags.values().stream()
                .anyMatch(state -> state.hasBaseEntity && isHiveCandidate(state) && completed.contains(state.applicationId));
        if (completedApplicationIds == null && hasHiveCandidates && !hasCompletionEvidence)
            throw new IOException("DAGs were found but no matching application completion evidence; provide completed application IDs");
        List<DagRecord> records = new ArrayList<>();
        for (Map.Entry<String, State> entry : dags.entrySet()) {
            State state = entry.getValue();
            if (!isHiveCandidate(state)) continue;
            if (!state.hasBaseEntity) {
                if (completedApplicationIds == null || completed.contains(state.applicationId))
                    discard(entry.getKey(), "TEZ_DAG_ID", "missing base entity");
                continue;
            }
            if (!completed.contains(state.applicationId)) continue;
            Map<String, Object> fields = resolveFields(entry.getKey(), state);
            Long start = (Long) fields.get("startTime");
            Long end = (Long) fields.get("endTime");
            if (start != null && end != null && end < start)
                warn(entry.getKey(), "endTime", "endTime precedes startTime", "null");
            records.add(MetricsExtractor.extract(entry.getKey(), state.applicationId, fields, state.counters,
                    resolver, state.automaticRowsTrusted));
        }
        records.sort(Comparator.comparing((DagRecord record) -> (String) record.get("applicationId"))
                .thenComparing(record -> (String) record.get("dagId")));
        return records;
    }

    private Map<String, Object> resolveFields(String dagId, State state) {
        Map<String, Object> fields = new HashMap<>(state.fields);
        for (Map.Entry<String, Set<String>> filter : state.filters.entrySet()) {
            String name = filter.getKey();
            if (state.invalidFields.contains(name)) continue;
            if (fields.containsKey(name)) {
                // Status filters can contain historical nonterminal states.
                boolean conflict = "status".equals(name)
                        ? filter.getValue().stream().anyMatch(status -> TERMINAL_DAG_STATES.contains(status)
                            && !status.equals(fields.get(name)))
                        : !filter.getValue().isEmpty()
                            && !(filter.getValue().size() == 1 && filter.getValue().contains(fields.get(name)));
                if (conflict) invalidateField(state, name, dagId, "conflicting primary filter");
                continue;
            }
            Set<String> candidates = new TreeSet<>(filter.getValue());
            if ("status".equals(name) && candidates.size() > 1) {
                Set<String> terminal = new TreeSet<>(candidates);
                terminal.retainAll(TERMINAL_DAG_STATES);
                if (!terminal.isEmpty()) candidates = terminal;
            }
            if (candidates.size() > 1) invalidateField(state, name, dagId, "conflicting primary filter");
            else if (!candidates.isEmpty()) fields.put(name, candidates.iterator().next());
        }
        for (Map.Entry<String, Long> time : state.eventTimes.entrySet())
            if (!state.invalidFields.contains(time.getKey())) mergeTimestamp(fields, time.getKey(), time.getValue(), dagId);
        for (String name : state.invalidFields) fields.remove(name);
        return fields;
    }

    /** Missing caller metadata may still belong to Hive; any explicit other framework does not. */
    private static boolean isHiveCandidate(State state) {
        return !state.quarantined && !state.explicitNonHive;
    }

    private void collectApplication(TimelineEntity entity) {
        String appId = entity.getEntityId();
        if (appId == null || !APP_ID.matcher(appId).matches()) {
            discard(appId, "applicationId", "invalid YARN application ID");
            return;
        }
        for (Object eventObject : emptyIfNull(entity.getEvents())) {
            if (!(eventObject instanceof TimelineEvent)) {
                warn(appId, "events", "invalid event", "skip_event");
                continue;
            }
            TimelineEvent event = (TimelineEvent) eventObject;
            if ("YARN_APPLICATION_FINISHED".equals(event.getEventType())
                    || ("YARN_APPLICATION_STATE_UPDATED".equals(event.getEventType())
                    && TERMINAL_APP_STATES.contains(emptyIfNull(event.getEventInfo()).get("YARN_APPLICATION_STATE"))))
                observedCompletedApps.add(appId);
        }
    }

    private void collectText(State state, String name, Object value, String dagId) {
        if (value == null) return;
        if (!isText(value)) {
            invalidateField(state, name, dagId, "expected nonempty string");
            return;
        }
        if ("callerType".equals(name) && !"HIVE_QUERY_ID".equals(value)) state.explicitNonHive = true;
        merge(state.fields, state.invalidFields, name, value, dagId);
    }

    private void collectQuery(String dagId, Object planObject, State state) {
        if (planObject == null || state.invalidFields.contains("query")) return;
        if (!(planObject instanceof Map)) {
            invalidateField(state, "query", dagId, "expected dagPlan object");
            return;
        }
        String query;
        try {
            query = HiveQueryText.read((Map<?, ?>) planObject);
        } catch (IOException | RuntimeException exception) {
            invalidateField(state, "query", dagId, "malformed Hive query metadata");
            return;
        }
        merge(state.fields, state.invalidFields, "query", query, dagId);
    }

    private void collectCallerContext(String dagId, Object planObject, State state) {
        if (planObject == null) return;
        if (!(planObject instanceof Map)) {
            invalidateCallerContext(state, dagId, "expected dagPlan object");
            return;
        }
        Object context = ((Map<?, ?>) planObject).get("dagContext");
        if (context == null) return;
        if (!(context instanceof Map)) {
            invalidateCallerContext(state, dagId, "expected dagContext object");
            return;
        }
        for (String name : Arrays.asList("callerId", "callerType"))
            collectText(state, name, ((Map<?, ?>) context).get(name), dagId);
    }

    private void invalidateCallerContext(State state, String dagId, String reason) {
        invalidateField(state, "callerId", dagId, reason);
        invalidateField(state, "callerType", dagId, reason);
    }

    private void collectCounters(String dagId, Object value, State state) {
        if (value == null) return;
        if (!(value instanceof Map)) {
            invalidateAutomaticRows(state, dagId, "counters", "expected object");
            return;
        }
        Object groups = ((Map<?, ?>) value).get("counterGroups");
        if (groups == null) return;
        if (!(groups instanceof Collection)) {
            invalidateAutomaticRows(state, dagId, "counterGroups", "expected collection");
            return;
        }
        for (Object groupObject : (Collection<?>) groups) {
            if (!(groupObject instanceof Map)) {
                invalidateAutomaticRows(state, dagId, "counterGroup", "expected object");
                continue;
            }
            Map<?, ?> group = (Map<?, ?>) groupObject;
            Object groupName = group.get("counterGroupName");
            if (!isCounterName(groupName)) {
                invalidateAutomaticRows(state, dagId, "counterGroupName", "expected nonempty string");
                continue;
            }
            Object items = group.get("counters");
            if (items == null) continue;
            if (!(items instanceof Collection)) {
                invalidateAutomaticRows(state, dagId, groupName + "/counters", "expected collection");
                continue;
            }
            for (Object item : (Collection<?>) items) {
                if (!(item instanceof Map)) {
                    invalidateAutomaticRows(state, dagId, groupName + "/counter", "expected object");
                    continue;
                }
                Map<?, ?> counter = (Map<?, ?>) item;
                Object counterName = counter.get("counterName");
                if (!isCounterName(counterName)) {
                    invalidateAutomaticRows(state, dagId, groupName + "/counterName", "expected nonempty string");
                    continue;
                }
                String groupText = (String) groupName;
                String name = (String) counterName;
                if (!MetricsExtractor.needsCounter(groupText, name) && !resolver.needsCounter(dagId, groupText, name)) continue;
                String key = MetricsExtractor.counterKey(groupText, name);
                if (state.invalidCounters.contains(key)) continue;
                try {
                    Long count = nonnegativeNumber(counter.get("counterValue"), dagId + "/" + groupText + "/" + name);
                    if (count == null) throw new IOException("missing counterValue");
                    merge(state.counters, state.invalidCounters, key, count, dagId);
                } catch (IOException e) {
                    invalidate(state.counters, state.invalidCounters, key, dagId, Diagnostics.describe(e));
                }
            }
        }
    }

    private void invalidateAutomaticRows(State state, String dagId, String field, String reason) {
        state.automaticRowsTrusted = false;
        warn(dagId, field, reason + "; resultRows candidates unknown", "null");
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

    private boolean validateApplication(State state, Object value, String dagId) {
        if (state.applicationId.equals(value)) return true;
        quarantine(state, dagId, "applicationId", "invalid or conflicting application identity");
        return false;
    }

    private void quarantine(State state, String dagId, String field, String reason) {
        state.quarantined = true;
        state.fields.clear();
        state.filters.clear();
        state.eventTimes.clear();
        state.counters.clear();
        discardedEntities = true;
        warn(dagId, field, reason, "skip_dag");
    }

    private void discard(String id, String field, String reason) {
        discardedEntities = true;
        warn(id, field, reason, "skip_entity");
    }

    private void warn(String id, String field, String reason, String action) {
        logWarning("WARN dag=" + id + " field=" + field.replace('\u0000', '/')
                + " reason=" + reason + " action=" + action);
    }

    private void logWarning(String message) {
        message = Diagnostics.singleLine(message);
        try {
            warningLog.accept(message);
        } catch (RuntimeException failure) {
            LOG.warn(message + "; warning sink failed", failure);
        }
    }

    private static boolean isText(Object value) {
        return value instanceof String && !((String) value).trim().isEmpty();
    }

    private static boolean isCounterName(Object value) {
        return isText(value) && ((String) value).indexOf('\u0000') < 0;
    }

    private static Map<?, ?> emptyIfNull(Map<?, ?> value) {
        return value == null ? Collections.emptyMap() : value;
    }

    private static Collection<?> emptyIfNull(Collection<?> value) {
        return value == null ? Collections.emptyList() : value;
    }

    private static Long nonnegativeNumber(Object value, String location) throws IOException {
        Long result = MetricsExtractor.number(value, location);
        if (result != null && result < 0) throw new IOException("Negative integer at " + location);
        return result;
    }

    private void invalidateField(State state, String name, String dagId, String reason) {
        invalidate(state.fields, state.invalidFields, name, dagId, reason);
        state.eventTimes.remove(name);
    }

    /** Lifecycle times use the latest timestamp, regardless of database or event iteration order. */
    private void mergeTimestamp(Map<String, ? super Long> target, String key, Long value, String dagId) {
        if (value == null) return;
        Long previous = (Long) target.get(key);
        long selected = value;
        if (previous != null && !previous.equals(value)) {
            selected = Math.max(previous, value);
            logWarning("WARN Conflicting timestamp at " + dagId + "/" + key
                    + ": previous=" + previous + " incoming=" + value + " selected=" + selected);
        }
        target.put(key, selected);
    }

    private <T> void merge(Map<String, T> target, Set<String> invalid, String key, T value, String dagId) {
        if (value == null || invalid.contains(key)) return;
        T previous = target.get(key);
        if (previous != null && !previous.equals(value)) {
            invalidate(target, invalid, key, dagId, "conflicting values");
            return;
        }
        target.put(key, value);
    }

    private <T> void invalidate(Map<String, T> target, Set<String> invalid, String key, String dagId, String reason) {
        // Keep known invalid counter keys: dropping one could make a second FileSink appear unique.
        target.put(key, null);
        if (invalid.add(key)) warn(dagId, key, reason, "null");
    }

    private static final class State {
        private final String applicationId;
        private final Map<String, Object> fields = new HashMap<>();
        private final Map<String, Set<String>> filters = new HashMap<>();
        private final Map<String, Long> eventTimes = new HashMap<>();
        private final Map<String, Long> counters = new HashMap<>();
        private final Set<String> invalidFields = new HashSet<>();
        private final Set<String> invalidCounters = new HashSet<>();
        private boolean hasBaseEntity;
        private boolean quarantined;
        private boolean explicitNonHive;
        private boolean automaticRowsTrusted = true;
        private State(String applicationId) { this.applicationId = applicationId; }
    }
}
