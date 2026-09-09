package io.github.timelineparser.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.timelineparser.domain.DagRecord;
import io.github.timelineparser.fixture.RollingStoreFixture;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class QueryColumnTest {
    private static final String APP = "application_1700000000000_0001";
    private static final String DAG = "dag_1700000000000_0001_1";
    private static final String SQL = "  -- 원문 주석\nSELECT '비밀 값',\tname FROM sample\nWHERE id = 7;  ";

    @Test void keepsDagInfoSqlExactlyIncludingUnicodeWhitespaceAndComments() throws Exception {
        DagCollector collector = collector();
        collector.accept(dag(map("dagInfo", dagInfo(SQL))));
        assertEquals(SQL, row(collector).get("query"));
    }

    @Test void usesRecordedHiveContextWhenDagInfoHasNoQuery() throws Exception {
        for (Map<String, Object> context : Arrays.asList(map("context", "HIVE", "description", SQL),
                map("callerType", "HIVE_QUERY_ID", "description", SQL))) {
            DagCollector collector = collector();
            collector.accept(dag(map("dagInfo", dagInfo(null), "dagContext", context)));
            assertEquals(SQL, row(collector).get("query"));
        }
    }

    @Test void rawDagInfoTakesPrecedenceOverMaskedCallerContext() throws Exception {
        DagCollector collector = collector();
        collector.accept(dag(map("dagInfo", dagInfo(SQL), "dagContext",
                map("context", "HIVE", "description", "SELECT '***'"))));
        assertEquals(SQL, row(collector).get("query"));
    }

    @Test void rawDagInfoTakesPrecedenceAcrossSnapshotsInEitherOrder() throws Exception {
        TimelineEntity raw = dag(map("dagInfo", dagInfo(SQL)));
        TimelineEntity masked = dag(map("dagContext", map("context", "HIVE", "description", "SELECT '***'")));
        for (List<TimelineEntity> snapshots : Arrays.asList(Arrays.asList(masked, raw), Arrays.asList(raw, masked))) {
            DagCollector collector = collector();
            for (TimelineEntity snapshot : snapshots) collector.accept(snapshot);
            assertEquals(SQL, row(collector).get("query"));
        }
    }

    @Test void conflictingFallbackCannotOverrideRawQuery() throws Exception {
        DagCollector collector = collector();
        collector.accept(dag(map("dagContext", map("context", "HIVE", "description", "SELECT '***'"))));
        collector.accept(dag(map("dagContext", map("context", "HIVE", "description", "SELECT 'masked'"))));
        assertNull(row(collector).get("query"));
        collector.accept(dag(map("dagInfo", dagInfo(SQL))));
        assertEquals(SQL, row(collector).get("query"));
    }

    @Test void invalidRawQueryCannotUseOtherwiseValidFallback() throws Exception {
        DagCollector collector = collector();
        collector.accept(dag(map("dagContext", map("context", "HIVE", "description", SQL))));
        collector.accept(dag(map("dagInfo", "invalid JSON")));
        collector.accept(dag(map("dagContext", map("context", "HIVE", "description", SQL))));
        assertNull(row(collector).get("query"));
    }

    @Test void malformedFallbackStaysNullWithoutDiscardingTheDag() throws Exception {
        DagCollector collector = collector();
        collector.accept(dag(map("dagContext", map("context", "HIVE", "description", 42))));
        collector.accept(dag(map("dagContext", map("context", "HIVE", "description", SQL))));
        assertNull(row(collector).get("query"));
        assertEquals(1700000000100L, row(collector).get("startTime"));
    }

    @Test void missingAndNonHiveDescriptionsRemainNull() throws Exception {
        for (Map<String, Object> plan : Arrays.asList(map(), map("dagInfo", "{\"context\":\"Pig\",\"description\":\"not SQL\"}"),
                map("dagContext", map("description", "unattributed text")))) {
            DagCollector collector = collector();
            collector.accept(dag(plan));
            assertNull(row(collector).get("query"));
        }
    }

    @Test void malformedQueryIsNullAndCannotBeRevivedOrLeakSqlIntoWarnings() throws Exception {
        List<Object> invalid = Arrays.asList(42, "{invalid:" + SQL, "{\"context\":\"Hive\",\"description\":42}",
                "{\"context\":\"Hive\",\"description\":\"SELECT 1\",\"description\":\"SELECT 2\"}",
                dagInfo(" \n\t"));
        for (Object badInfo : invalid) {
            List<String> warnings = new ArrayList<>();
            DagCollector collector = new DagCollector(new ResultRowsResolver(), warnings::add);
            collector.accept(dag(map("dagInfo", dagInfo(SQL))));
            collector.accept(dag(map("dagInfo", badInfo)));
            collector.accept(dag(map("dagInfo", dagInfo(SQL))));
            assertNull(row(collector).get("query"));
            assertEquals(1700000000100L, row(collector).get("startTime"));
            assertTrue(warnings.stream().anyMatch(line -> line.contains("query")), warnings::toString);
            assertFalse(warnings.stream().anyMatch(line -> line.contains("비밀 값")), warnings::toString);
        }
    }

    @Test void conflictingSqlRemainsNullAfterLaterMatchingSnapshot() throws Exception {
        List<String> warnings = new ArrayList<>();
        DagCollector collector = new DagCollector(new ResultRowsResolver(), warnings::add);
        collector.accept(dag(map("dagInfo", dagInfo(SQL))));
        collector.accept(dag(map("dagInfo", dagInfo("SELECT 2"))));
        collector.accept(dag(map("dagInfo", dagInfo(SQL))));
        assertNull(row(collector).get("query"));
        assertTrue(warnings.stream().anyMatch(line -> line.contains("query") && line.contains("conflict")), warnings::toString);
        assertFalse(warnings.stream().anyMatch(line -> line.contains("비밀 값")), warnings::toString);
    }

    @Test void missingSnapshotDoesNotEraseRecordedSql() throws Exception {
        DagCollector collector = collector();
        collector.accept(dag(map("dagInfo", dagInfo(SQL))));
        collector.accept(dag(map()));
        assertEquals(SQL, row(collector).get("query"));
    }

    @Test void retainsStatementsRegardlessOfResultRowClassificationSupport() throws Exception {
        DagCollector collector = collector();
        String sql = "UPDATE sample SET value = 10 WHERE id = 7";
        collector.accept(dag(map("dagInfo", dagInfo(sql))));
        assertEquals(sql, row(collector).get("query"));
    }

    @Test void queryIsNotTruncatedToDiagnosticLineLimit() throws Exception {
        String sql = "SELECT '" + String.join("", Collections.nCopies(6000, "한")) + "'";
        DagCollector collector = collector();
        collector.accept(dag(map("dagInfo", dagInfo(sql))));
        assertEquals(sql, row(collector).get("query"));
    }

    private DagCollector collector() { return new DagCollector(new ResultRowsResolver()); }

    private DagRecord row(DagCollector collector) throws Exception {
        List<DagRecord> rows = collector.finish(Collections.singleton(APP));
        assertEquals(1, rows.size());
        return rows.get(0);
    }

    private TimelineEntity dag(Map<String, Object> plan) {
        TimelineEntity entity = RollingStoreFixture.entity("TEZ_DAG_ID", DAG, 1700000000000L);
        entity.addOtherInfo("startTime", 1700000000100L);
        entity.addOtherInfo("dagPlan", plan);
        return entity;
    }

    private String dagInfo(String sql) throws Exception {
        return new ObjectMapper().writeValueAsString(map("context", "Hive", "description", sql));
    }

    private static Map<String, Object> map(Object... pairs) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) result.put((String) pairs[i], pairs[i + 1]);
        return result;
    }
}
