package io.github.timelineparser.metrics;

import io.github.timelineparser.domain.DagRecord;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Collections;
import java.util.Map;

import static io.github.timelineparser.metrics.DagCollectorTest.*;
import static org.junit.jupiter.api.Assertions.*;

class ResultRowsResolverTest {
    @TempDir Path temporary;

    @Test void automaticallyReadsFileSinkCounterWithoutMapping() throws Exception {
        DagRecord row = automatic("SUCCEEDED", group("HIVE", "RECORDS_OUT_0", 42L,
                "RECORDS_OUT_INTERMEDIATE", 100L, "RECORDS_OUT_INTERMEDIATE_Map_1", 100L,
                "RECORDS_OUT_OPERATOR_FS_1", 42L), group(TASK, "OUTPUT_RECORDS", 10000L));
        assertEquals(42L, row.get("resultRows"));
        assertEquals("FILE_SINK_OUTPUT", row.get("resultRowsKind"));
        assertEquals("RECORDS_OUT_0", row.get("resultRowsSource"));
    }

    @Test void supportsTableSuffixAndConfiguredCounterGroupWithoutGuessingQueryKind() throws Exception {
        DagRecord row = automatic("SUCCEEDED", group("CUSTOM_HIVE", "RECORDS_OUT_12_database.table", 5000000000L));
        assertEquals(5000000000L, row.get("resultRows"));
        assertEquals("FILE_SINK_OUTPUT", row.get("resultRowsKind"));
        assertEquals("RECORDS_OUT_12_database.table", row.get("resultRowsSource"));
        assertEquals(0L, automatic("SUCCEEDED", group("HIVE", "RECORDS_OUT_1", 0L)).get("resultRows"));
    }

    @Test void missingOrNonFileSinkCountersDoNotCreateResultRows() throws Exception {
        assertNoRows(automatic("SUCCEEDED"));
        assertNoRows(automatic("SUCCEEDED", group("HIVE", "RECORDS_OUT_INTERMEDIATE", 42L,
                "RECORDS_OUT_OPERATOR_FS_1", 42L, "RECORDS_OUT_", 42L,
                "RECORDS_OUT_1_", 42L, "RECORDS_OUT_1suffix", 42L), group(TASK, "OUTPUT_RECORDS", 42L)));
    }

    @Test void multipleFileSinkCountersAreNotSummedOrSelectedArbitrarily() throws Exception {
        assertNoRows(automatic("SUCCEEDED", group("HIVE", "RECORDS_OUT_0", 42L, "RECORDS_OUT_1_table", 999L)));
        assertNoRows(automatic("SUCCEEDED", group("HIVE", "RECORDS_OUT_0", 42L),
                group("CUSTOM_HIVE", "RECORDS_OUT_0", 42L)));
        assertNoRows(automatic("SUCCEEDED", group("HIVE", "RECORDS_OUT_0", 42L, "RECORDS_OUT_1", -1L)));
    }

    @Test void automaticResultsRequireSuccessAndNonnegativeCount() throws Exception {
        for (String status : new String[] { "FAILED", "KILLED", "ERROR", "RUNNING" })
            assertNoRows(automatic(status, group("HIVE", "RECORDS_OUT_0", 42L)));
        assertNoRows(automatic("SUCCEEDED", group("HIVE", "RECORDS_OUT_0", -1L)));
    }

    @Test void joinsExtraInfoCountersInEitherOrderWithoutCountingDuplicatesTwice() throws Exception {
        for (boolean extraFirst : new boolean[] { true, false }) {
            DagCollector collector = new DagCollector(new ResultRowsResolver());
            TimelineEntity base = dag(DAG);
            TimelineEntity extra = entity("TEZ_DAG_EXTRA_INFO", DAG);
            extra.addOtherInfo("counters", counters(group("HIVE", "RECORDS_OUT_0", 42L)));
            collector.accept(extraFirst ? extra : base);
            collector.accept(extraFirst ? base : extra);
            collector.accept(extra);
            DagRecord row = onlyRow(collector.finish(Collections.singleton(APP)));
            assertEquals(42L, row.get("resultRows"));
            assertEquals("FILE_SINK_OUTPUT", row.get("resultRowsKind"));
        }
    }

    @Test void mappingFileStillAllowsAutomaticExtractionForUnmappedDags() throws Exception {
        DagCollector collector = new DagCollector(mapping("SELECT_RESULT"));
        TimelineEntity unmapped = dag("dag_1700000000000_0001_2");
        unmapped.addOtherInfo("counters", counters(group("HIVE", "RECORDS_OUT_0", 42L)));
        collector.accept(unmapped);
        DagRecord row = onlyRow(collector.finish(Collections.singleton(APP)));
        assertEquals(42L, row.get("resultRows"));
        assertEquals("FILE_SINK_OUTPUT", row.get("resultRowsKind"));
    }

    @Test void validatedSelectMappingUsesOnlyItsExactCounter() throws Exception {
        ResultRowsResolver resolver = mapping("SELECT_RESULT");
        DagRecord row = collect(resolver, "SUCCEEDED", 42L);
        assertEquals(42L, row.get("resultRows"));
        assertEquals("SELECT_RESULT", row.get("resultRowsKind"));
        assertEquals("RECORDS_OUT_0", row.get("resultRowsSource"));
    }

    @Test void ctasMappingSupportsExplicitZeroAndRejectsFailedOrNegativeResults() throws Exception {
        ResultRowsResolver resolver = mapping("CTAS_WRITE");
        assertEquals(0L, collect(resolver, "SUCCEEDED", 0L).get("resultRows"));
        assertEquals("CTAS_WRITE", collect(resolver, "SUCCEEDED", 5L).get("resultRowsKind"));
        assertNull(collect(resolver, "FAILED", 12L).get("resultRows"));
        assertNull(collect(resolver, "SUCCEEDED", -1L).get("resultRows"));
        assertNull(collect(new ResultRowsResolver(), "SUCCEEDED", 12L).get("resultRows"));
    }

    @Test void malformedMappingAndDuplicateDagIdsAreRejected() throws Exception {
        Path file = temporary.resolve("invalid.json");
        Files.write(file, "{\"dag_1700000000000_0001_1\":{\"kind\":\"GUESS\"}}".getBytes(StandardCharsets.UTF_8));
        assertThrows(IOException.class, () -> ResultRowsResolver.fromFile(file));
        Files.write(file, ("{\"" + DAG + "\":{},\"" + DAG + "\":{}}").getBytes(StandardCharsets.UTF_8));
        assertThrows(IOException.class, () -> ResultRowsResolver.fromFile(file));
    }

    @Test void mappingNeverCreatesAZeroWhenItsCounterIsMissing() throws Exception {
        DagCollector collector = new DagCollector(mapping("SELECT_RESULT"));
        TimelineEntity dag = dag(DAG);
        dag.addOtherInfo("counters", counters(group("HIVE", "RECORDS_OUT_1", 100L)));
        collector.accept(dag);
        DagRecord row = onlyRow(collector.finish(Collections.singleton(APP)));
        assertNull(row.get("resultRows"));
        assertNull(row.get("resultRowsKind"));
        assertNull(row.get("resultRowsSource"));
    }

    private ResultRowsResolver mapping(String kind) throws Exception {
        Path file = temporary.resolve(kind + ".json");
        Files.write(file, ("{\"" + DAG + "\":{\"kind\":\"" + kind
                + "\",\"counterGroup\":\"HIVE\",\"counterName\":\"RECORDS_OUT_0\"}}")
                .getBytes(StandardCharsets.UTF_8));
        return ResultRowsResolver.fromFile(file);
    }
    @SafeVarargs private final DagRecord automatic(String status, Map<String, Object>... groups) throws Exception {
        DagCollector collector = new DagCollector(new ResultRowsResolver());
        TimelineEntity dag = dag(DAG);
        dag.addOtherInfo("status", status);
        dag.addOtherInfo("counters", counters(groups));
        collector.accept(dag);
        return onlyRow(collector.finish(Collections.singleton(APP)));
    }

    private void assertNoRows(DagRecord row) {
        assertNull(row.get("resultRows"));
        assertNull(row.get("resultRowsKind"));
        assertNull(row.get("resultRowsSource"));
    }

    private DagRecord collect(ResultRowsResolver resolver, String status, long value) throws Exception {
        DagCollector collector = new DagCollector(resolver);
        TimelineEntity dag = dag(DAG);
        dag.addOtherInfo("status", status);
        dag.addOtherInfo("counters", counters(group("HIVE", "RECORDS_OUT_0", value, "RECORDS_OUT_1", 999L),
                group(TASK, "OUTPUT_RECORDS", 10000L)));
        collector.accept(dag);
        return onlyRow(collector.finish(Collections.singleton(APP)));
    }
}
