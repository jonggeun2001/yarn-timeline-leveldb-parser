package io.github.timelineparser.metrics;

import io.github.timelineparser.domain.DagRecord;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Collections;

import static io.github.timelineparser.metrics.DagCollectorTest.*;
import static org.junit.jupiter.api.Assertions.*;

class ResultRowsResolverTest {
    @TempDir Path temporary;

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
