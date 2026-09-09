package io.github.timelineparser.metrics;

import io.github.timelineparser.domain.DagRecord;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class DagCollectorTest {
    static final String APP = "application_1700000000000_0001";
    static final String DAG = "dag_1700000000000_0001_1";
    static final String TASK = "org.apache.tez.common.counters.TaskCounter";
    static final String FS = "org.apache.tez.common.counters.FileSystemCounter";

    @Test void joinsSplitEntitiesUsingExecutionTimesAndExactCounterGroups() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        TimelineEntity dag = dag(DAG);
        dag.setStartTime(10L);
        dag.addOtherInfo("startTime", 1000L);
        dag.addOtherInfo("endTime", 3500L);
        dag.addOtherInfo("numCompletedTasks", 7);
        dag.addOtherInfo("numFailedTaskAttempts", 2);
        dag.addOtherInfo("user", "alice");
        dag.addOtherInfo("queueName", "analytics");
        dag.addOtherInfo("callerId", "query-123");
        dag.addOtherInfo("callerType", "HIVE_QUERY_ID");
        TimelineEntity extra = entity("TEZ_DAG_EXTRA_INFO", DAG);
        extra.addOtherInfo("counters", counters(
                group(TASK, "CPU_MILLISECONDS", 120L, "GC_TIME_MILLIS", 0L,
                        "SHUFFLE_BYTES", 90L, "ADDITIONAL_SPILLS_BYTES_WRITTEN", 40L),
                group(FS, "HDFS_BYTES_READ", 456L, "HDFS_BYTES_WRITTEN", 12L),
                group("unrelated", "CPU_MILLISECONDS", 999L)));
        collector.accept(extra);
        collector.accept(dag);
        collector.accept(finished(APP));
        DagRecord row = onlyRow(collector.finish(null));
        assertEquals(DAG, row.get("dagId"));
        assertEquals(APP, row.get("applicationId"));
        assertEquals("alice", row.get("user"));
        assertEquals("query-123", row.get("hiveQueryId"));
        assertEquals("analytics", row.get("queueName"));
        assertEquals(1000L, row.get("startTime"));
        assertEquals(3500L, row.get("endTime"));
        assertEquals(2500L, row.get("durationMilliseconds"));
        assertEquals(120L, row.get("cpuMilliseconds"));
        assertEquals(0L, row.get("gcMilliseconds"));
        assertEquals(456L, row.get("hdfsBytesRead"));
        assertEquals(12L, row.get("hdfsBytesWritten"));
        assertEquals(90L, row.get("shuffleBytes"));
        assertEquals(40L, row.get("additionalSpillBytesWritten"));
        assertEquals(7L, row.get("totalTasks"));
        assertEquals(2L, row.get("failedTaskAttempts"));
        assertEquals(20, row.getValues().size());
        assertNull(row.get("resultRows"));
        assertNull(row.get("resultRowsKind"));
        assertNull(row.get("resultRowsSource"));
        assertThrows(UnsupportedOperationException.class, () -> row.getValues().put("user", "changed"));
    }

    @Test void noApplicationCompletionEvidenceFailsInsteadOfSilentlyReturningEmpty() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        collector.accept(dag(DAG));
        assertThrows(IOException.class, () -> collector.finish(null));
    }

    @Test void explicitCompletedIdsOverrideObservedCompletionAndAllowEmptyScope() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        collector.accept(dag(DAG));
        collector.accept(finished(APP));
        assertTrue(collector.finish(Collections.emptySet()).isEmpty());
        assertEquals(1, collector.finish(Collections.singleton(APP)).size());
    }

    @Test void stateUpdatedEventAcceptsTerminalStateButStaleOtherInfoDoesNot() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        collector.accept(dag(DAG));
        TimelineEntity app = entity("YARN_APPLICATION", APP);
        app.addOtherInfo("YARN_APPLICATION_STATE", "FINISHED");
        collector.accept(app);
        assertThrows(IOException.class, () -> collector.finish(null));
        TimelineEvent terminal = event("YARN_APPLICATION_STATE_UPDATED", 5000L);
        terminal.addEventInfo("YARN_APPLICATION_STATE", "KILLED");
        app.addEvent(terminal);
        collector.accept(app);
        assertEquals(1, collector.finish(null).size());
    }

    @Test void stateUpdateWithoutEventInfoDoesNotHideSeparateCompletionEvidence() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        collector.accept(dag(DAG));
        TimelineEntity app = finished(APP);
        TimelineEvent stateUpdate = event("YARN_APPLICATION_STATE_UPDATED", 4000L);
        stateUpdate.setEventInfo(null);
        app.addEvent(stateUpdate);
        assertDoesNotThrow(() -> collector.accept(app));
        assertEquals(DAG, onlyRow(collector.finish(null)).get("dagId"));
    }

    @Test void repeatedEntitiesDoNotDoubleCountAndInputOrderDoesNotChangeRows() throws Exception {
        TimelineEntity first = dag(DAG);
        first.addOtherInfo("counters", counters(group(TASK, "CPU_MILLISECONDS", 5L)));
        TimelineEntity second = dag("dag_1700000000000_0001_2");
        List<TimelineEntity> entities = Arrays.asList(second, first, first, finished(APP));
        DagCollector forward = new DagCollector(new ResultRowsResolver());
        DagCollector reverse = new DagCollector(new ResultRowsResolver());
        for (TimelineEntity e : entities) forward.accept(e);
        List<TimelineEntity> reversed = new ArrayList<>(entities);
        Collections.reverse(reversed);
        for (TimelineEntity e : reversed) reverse.accept(e);
        List<DagRecord> rows = forward.finish(null);
        assertEquals(2, rows.size());
        assertEquals(DAG, rows.get(0).get("dagId"));
        assertEquals(5L, rows.get(0).get("cpuMilliseconds"));
        assertEquals(rows.get(0).getValues(), reverse.finish(null).get(0).getValues());
    }

    @Test void conflictingSnapshotsAndApplicationIdentityAreRejected() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        TimelineEntity first = dag(DAG);
        first.addOtherInfo("endTime", 4000L);
        collector.accept(first);
        TimelineEntity changed = dag(DAG);
        changed.addOtherInfo("endTime", 4500L);
        assertThrows(IOException.class, () -> collector.accept(changed));
        TimelineEntity wrongApp = dag("dag_1700000000000_0001_2");
        wrongApp.addOtherInfo("applicationId", "application_1700000000000_0002");
        assertThrows(IOException.class, () -> new DagCollector(new ResultRowsResolver()).accept(wrongApp));
    }

    @Test void conflictingCountersFailAndIrrelevantCountersAreNotRetained() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        TimelineEntity first = dag(DAG);
        first.addOtherInfo("counters", counters(group(TASK, "CPU_MILLISECONDS", 10L)));
        collector.accept(first);
        TimelineEntity extra = entity("TEZ_DAG_EXTRA_INFO", DAG);
        extra.addOtherInfo("counters", counters(group(TASK, "CPU_MILLISECONDS", 11L)));
        assertThrows(IOException.class, () -> collector.accept(extra));
    }

    @Test void missingCountersStayNullAndInvalidDurationIsNotExported() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        TimelineEntity dag = dag(DAG);
        dag.addOtherInfo("startTime", 200L);
        dag.addOtherInfo("endTime", 100L);
        collector.accept(dag);
        DagRecord row = onlyRow(collector.finish(Collections.singleton(APP)));
        assertNull(row.get("durationMilliseconds"));
        assertNull(row.get("endTime"));
        assertNull(row.get("cpuMilliseconds"));
        assertNull(row.get("totalTasks"));
    }

    @Test void eventAndCallerContextFallbacksUseOnlyHiveIdentity() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        TimelineEntity dag = dag(DAG);
        dag.addPrimaryFilter("user", "bob");
        dag.addPrimaryFilter("queueName", "batch");
        dag.addEvent(event("DAG_STARTED", 100L));
        dag.addEvent(event("DAG_FINISHED", 200L));
        collector.accept(dag);
        TimelineEntity extra = entity("TEZ_DAG_EXTRA_INFO", DAG);
        extra.addOtherInfo("dagPlan", map("dagContext", map("callerId", "query-x", "callerType", "HIVE_QUERY_ID")));
        collector.accept(extra);
        DagRecord row = onlyRow(collector.finish(Collections.singleton(APP)));
        assertEquals("query-x", row.get("hiveQueryId"));
        assertEquals("bob", row.get("user"));
        assertEquals("batch", row.get("queueName"));
        assertEquals(100L, row.get("durationMilliseconds"));
    }

    @Test void acceptsEmptyDatasetAndRejectsMalformedDagIds() throws Exception {
        assertTrue(new DagCollector(new ResultRowsResolver()).finish(null).isEmpty());
        assertThrows(IOException.class, () -> new DagCollector(new ResultRowsResolver())
                .accept(entity("TEZ_DAG_ID", "not-a-dag")));
    }

    @Test void conflictingUserFilterDoesNotSilentlyOverrideIdentity() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        TimelineEntity dag = dag(DAG);
        dag.addOtherInfo("user", "alice");
        dag.addPrimaryFilter("user", "bob");
        collector.accept(dag);
        assertThrows(IOException.class, () -> collector.finish(Collections.singleton(APP)));
    }

    @Test void selectedCounterWithoutValueIsAnErrorRatherThanMissingMetric() throws Exception {
        TimelineEntity dag = dag(DAG);
        dag.addOtherInfo("counters", counters(map("counterGroupName", TASK, "counters",
                Collections.singletonList(map("counterName", "CPU_MILLISECONDS")))));
        assertThrows(IOException.class, () -> new DagCollector(new ResultRowsResolver()).accept(dag));
    }

    @Test void skipsUnfinishedApplicationsAndRejectsOrphanSelectedExtraInfo() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        collector.accept(dag(DAG));
        collector.accept(dag("dag_1700000000000_0002_1"));
        collector.accept(finished(APP));
        assertEquals(2, collector.getDagCount(), "Collected count includes DAGs excluded for unknown application completion");
        assertEquals(DAG, onlyRow(collector.finish(null)).get("dagId"));
        DagCollector orphan = new DagCollector(new ResultRowsResolver());
        orphan.accept(entity("TEZ_DAG_EXTRA_INFO", DAG));
        assertThrows(IOException.class, () -> orphan.finish(Collections.singleton(APP)));
    }

    @Test void acceptsNumericRepresentationsAndRetainsOnlyRequestedMetrics() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        TimelineEntity first = dag(DAG);
        first.addOtherInfo("numCompletedTasks", 7);
        first.addOtherInfo("counters", counters(group(TASK, "CPU_MILLISECONDS", 12)));
        collector.accept(first);
        TimelineEntity duplicate = dag(DAG);
        duplicate.addOtherInfo("numCompletedTasks", 7L);
        duplicate.addOtherInfo("counters", counters(group(TASK, "CPU_MILLISECONDS", 12L),
                group("ignored", "INTERNAL_RECORDS", "not-needed")));
        collector.accept(duplicate);
        assertEquals(12L, onlyRow(collector.finish(Collections.singleton(APP))).get("cpuMilliseconds"));
    }

    @Test void missingCallerTypeDoesNotBecomeHiveQueryIdAndRecordCopiesInputMap() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        TimelineEntity dag = dag(DAG);
        dag.addOtherInfo("callerId", "other-query");
        collector.accept(dag);
        assertNull(onlyRow(collector.finish(Collections.singleton(APP))).get("hiveQueryId"));
        Map<String, Object> source = map("dagId", DAG);
        DagRecord snapshot = new DagRecord(source);
        source.put("dagId", "changed");
        assertEquals(DAG, snapshot.get("dagId"));
    }

    @Test void includesHiveAndUnknownCallersButExcludesExplicitOtherFrameworks() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        TimelineEntity hive = dag(DAG);
        hive.addOtherInfo("callerId", "hive-query");
        hive.addOtherInfo("callerType", "HIVE_QUERY_ID");
        collector.accept(hive);
        TimelineEntity pig = dag("dag_1700000000000_0001_2");
        pig.addOtherInfo("callerType", "PIG_SCRIPT_ID");
        collector.accept(pig);
        TimelineEntity unknown = dag("dag_1700000000000_0001_3");
        unknown.addOtherInfo("callerId", "untyped-query");
        collector.accept(unknown);
        collector.accept(finished(APP));
        List<DagRecord> rows = collector.finish(null);
        assertEquals(2, rows.size());
        assertEquals(DAG, rows.get(0).get("dagId"));
        assertEquals("hive-query", rows.get(0).get("hiveQueryId"));
        assertEquals("dag_1700000000000_0001_3", rows.get(1).get("dagId"));
        assertNull(rows.get(1).get("hiveQueryId"));
        assertEquals(3, collector.getDagCount());
    }

    @Test void nonHiveOnlyInputDoesNotRequireCompletionEvidence() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        TimelineEntity pig = dag(DAG);
        pig.addOtherInfo("callerType", "PIG_SCRIPT_ID");
        collector.accept(pig);
        assertTrue(collector.finish(null).isEmpty());
    }

    @Test void explicitlyNonHiveExtraInfoDoesNotRequireAnInScopeBaseEntity() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        TimelineEntity pig = entity("TEZ_DAG_EXTRA_INFO", DAG);
        pig.addOtherInfo("dagPlan", map("dagContext", map("callerType", "PIG_SCRIPT_ID")));
        collector.accept(pig);
        assertTrue(collector.finish(Collections.singleton(APP)).isEmpty());
    }

    @Test void conflictingLifecycleTimestampSourcesAreRejected() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        TimelineEntity base = dag(DAG);
        base.addOtherInfo("endTime", 300L);
        collector.accept(base);
        TimelineEntity extra = entity("TEZ_DAG_EXTRA_INFO", DAG);
        extra.addEvent(event("DAG_FINISHED", 400L));
        collector.accept(extra);
        assertThrows(IOException.class, () -> collector.finish(Collections.singleton(APP)));
    }

    @Test void conflictingTerminalStatusCannotBecomeSuccessfulResults() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        TimelineEntity dag = dag(DAG);
        dag.addPrimaryFilter("status", "FAILED");
        collector.accept(dag);
        assertThrows(IOException.class, () -> collector.finish(Collections.singleton(APP)));
    }

    @Test void historicalNonterminalStatusFilterIsCompatibleWithFinalStatus() throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        TimelineEntity dag = dag(DAG);
        dag.addPrimaryFilter("status", "RUNNING");
        dag.addPrimaryFilter("status", "SUCCEEDED");
        dag.addOtherInfo("endTime", 200L);
        dag.addEvent(event("DAG_FINISHED", 200L));
        collector.accept(dag);
        assertEquals("SUCCEEDED", onlyRow(collector.finish(Collections.singleton(APP))).get("status"));
    }

    static DagRecord onlyRow(List<DagRecord> rows) {
        assertEquals(1, rows.size(), "Expected one selected DAG");
        return rows.get(0);
    }

    static TimelineEntity dag(String id) {
        TimelineEntity e = entity("TEZ_DAG_ID", id);
        e.addOtherInfo("status", "SUCCEEDED");
        return e;
    }
    static TimelineEntity entity(String type, String id) {
        TimelineEntity e = new TimelineEntity();
        e.setEntityType(type);
        e.setEntityId(id);
        return e;
    }
    static TimelineEntity finished(String app) {
        TimelineEntity e = entity("YARN_APPLICATION", app);
        e.addEvent(event("YARN_APPLICATION_FINISHED", 5000L));
        return e;
    }
    static TimelineEvent event(String type, long timestamp) {
        TimelineEvent e = new TimelineEvent();
        e.setEventType(type);
        e.setTimestamp(timestamp);
        return e;
    }
    static Map<String, Object> map(Object... values) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) result.put((String) values[i], values[i + 1]);
        return result;
    }
    @SafeVarargs static Map<String, Object> counters(Map<String, Object>... groups) {
        return map("counterGroups", Arrays.asList(groups));
    }
    static Map<String, Object> group(String name, Object... entries) {
        List<Map<String, Object>> counters = new ArrayList<>();
        for (int i = 0; i < entries.length; i += 2)
            counters.add(map("counterName", entries[i], "counterValue", entries[i + 1]));
        return map("counterGroupName", name, "counters", counters);
    }
}
