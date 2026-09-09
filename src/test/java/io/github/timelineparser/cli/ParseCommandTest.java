package io.github.timelineparser.cli;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.timelineparser.fixture.RollingStoreFixture;
import io.github.timelineparser.fixture.LogCapture;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.yarn.api.records.timeline.*;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

@org.junit.jupiter.api.parallel.ResourceLock(org.junit.jupiter.api.parallel.Resources.SYSTEM_ERR)
class ParseCommandTest {
    @TempDir Path temp;
    private static final String APP = "application_1700000000000_0001";
    private static final String DAG = "dag_1700000000000_0001_1";

    @Test void transformsRealHadoopDatabaseAndReplacesRatherThanAppending() throws Exception {
        Path input = fixture(true);
        Path output = temp.resolve("output");
        StringWriter log = new StringWriter();
        assertEquals(0, run(log, "--input", input.toString(), "--output", output.toString()));
        assertFalse(log.toString().contains("WARN Conflicting timestamp"));
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new LocalInputFile(output.resolve("result.parquet")))
                .withConf(new PlainParquetConfiguration()).build()) {
            GenericRecord row = reader.read();
            assertNotNull(row);
            assertEquals(137L, row.get("resultRows"));
            assertEquals("FILE_SINK_OUTPUT", row.get("resultRowsKind").toString());
            assertEquals("RECORDS_OUT_0", row.get("resultRowsSource").toString());
            assertNull(reader.read());
        }
        List<String> first = rows(output);
        assertEquals(1, first.size());
        assertTrue(first.get(0).contains(DAG));
        assertTrue(first.get(0).contains("120"));
        assertEquals(0, run("--input", input.toString(), "--output", output.toString()));
        assertEquals(first, rows(output));
        Path empty = RollingStoreFixture.write(temp.resolve("empty"));
        // No entity database exists when no entity was written; a real empty entity DB is valid.
        Path emptyDb = Files.createDirectories(empty.resolve("entity-ldb.2023-11-14"));
        try (org.iq80.leveldb.DB ignored = org.fusesource.leveldbjni.JniDBFactory.factory.open(emptyDb.toFile(), new org.iq80.leveldb.Options().createIfMissing(true))) { }
        assertEquals(0, run("--input", empty.toString(), "--output", output.toString()));
        assertTrue(rows(output).isEmpty());
    }

    @Test void unknownCompletionRequiresEvidenceAndFailurePreservesExistingOutput() throws Exception {
        Path input = fixture(false);
        Path output = Files.createDirectory(temp.resolve("output"));
        byte[] previous = "previous successful result".getBytes(StandardCharsets.UTF_8);
        Files.write(output.resolve("result.parquet"), previous);
        assertEquals(1, run("--input", input.toString(), "--output", output.toString()));
        assertArrayEquals(previous, Files.readAllBytes(output.resolve("result.parquet")));
        Path ids = temp.resolve("completed.txt");
        Files.write(ids, Arrays.asList(APP), StandardCharsets.UTF_8);
        assertEquals(0, run("--input", input.toString(), "--output", output.toString(), "--completed-applications", ids.toString()));
        assertEquals(1, rows(output).size());
    }

    @Test void overlappingPathsAreRejectedBeforeCreatingOutputInsideInput() throws Exception {
        Path input = Files.createDirectory(temp.resolve("input"));
        Path nestedOutput = input.resolve("new-output");
        assertEquals(2, run("--input", input.toString(), "--output", nestedOutput.toString()));
        assertFalse(Files.exists(nestedOutput));
    }

    @Test void helpAndMissingRequiredArgumentsHaveConventionalExitCodes() {
        assertEquals(0, run("--help"));
        assertEquals(2, run("--input", temp.toString()));
    }

    @Test void reportsMavenProjectVersion() {
        StringWriter output = new StringWriter();
        CommandLine cli = new CommandLine(new ParseCommand()).setOut(new java.io.PrintWriter(output));
        assertEquals(0, cli.execute("--version"));
        assertEquals(System.getProperty("parser.version"), output.toString().trim());
    }

    @Test void completedIdsRejectOverflowBeforeTouchingOutput() throws Exception {
        Path input = Files.createDirectory(temp.resolve("input"));
        Path output = Files.createDirectory(temp.resolve("output"));
        byte[] previous = "previous".getBytes(StandardCharsets.UTF_8);
        Files.write(output.resolve("result.parquet"), previous);
        Path ids = temp.resolve("completed.txt");
        for (String id : Arrays.asList("application_9223372036854775808_0001", "application_1700000000000_2147483648")) {
            Files.write(ids, Arrays.asList(id), StandardCharsets.UTF_8);
            assertEquals(2, run("--input", input.toString(), "--output", output.toString(), "--completed-applications", ids.toString()));
            assertArrayEquals(previous, Files.readAllBytes(output.resolve("result.parquet")));
        }
    }

    @Test void completedIdsUseHadoopCanonicalPadding() throws Exception {
        Path input = fixture(false);
        Path ids = temp.resolve("completed.txt");
        Files.write(ids, Arrays.asList("application_1700000000000_1"), StandardCharsets.UTF_8);
        Path output = temp.resolve("output");
        assertEquals(0, run("--input", input.toString(), "--output", output.toString(), "--completed-applications", ids.toString()));
        assertEquals(1, rows(output).size());
    }

    @Test void usesExplicitSelectAndCtasOverridesInTheFullPipeline() throws Exception {
        Path input = fixture(true);
        Path output = temp.resolve("output");
        Path mapping = temp.resolve("mapping.json");
        for (String kind : Arrays.asList("SELECT_RESULT", "CTAS_WRITE")) {
            Files.write(mapping, ("{\"" + DAG + "\":{\"kind\":\"" + kind
                    + "\",\"counterGroup\":\"HIVE\",\"counterName\":\"RECORDS_OUT_0\"}}")
                    .getBytes(StandardCharsets.UTF_8));
            assertEquals(0, run("--input", input.toString(), "--output", output.toString(), "--result-rows-mapping", mapping.toString()));
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new LocalInputFile(output.resolve("result.parquet")))
                    .withConf(new PlainParquetConfiguration()).build()) {
                GenericRecord row = reader.read();
                assertEquals(137L, row.get("resultRows"));
                assertEquals(kind, row.get("resultRowsKind").toString());
                assertEquals("RECORDS_OUT_0", row.get("resultRowsSource").toString());
                assertEquals(120L, row.get("cpuMilliseconds"));
                assertEquals("hive-query-1", row.get("hiveQueryId").toString());
                assertEquals(1700000000100L, row.get("startTime"));
                assertEquals(1000L, row.get("durationMilliseconds"));
                assertNull(reader.read());
            }
        }
    }

    @Test void selectsCountersFromSqlMetadataWithBothDestinationIdsPresent() throws Exception {
        List<SqlCase> cases = Arrays.asList(
                new SqlCase("SELECT id FROM source_table", 42L, "RECORDS_OUT_0"),
                new SqlCase("CREATE TABLE target_table AS SELECT id FROM source_table", 999L, "RECORDS_OUT_1"),
                new SqlCase("INSERT INTO TABLE target_table SELECT id FROM source_table", 999L, "RECORDS_OUT_1"),
                new SqlCase("INSERT OVERWRITE TABLE target_table SELECT id FROM source_table", 999L, "RECORDS_OUT_1"),
                new SqlCase("INSERT INTO TABLE target_table PARTITION (ds='2026-01-01') SELECT id FROM source_table", 999L, "RECORDS_OUT_1"),
                new SqlCase("INSERT INTO TABLE target_table PARTITION (ds) SELECT id, ds FROM source_table", 999L, "RECORDS_OUT_1"),
                new SqlCase("INSERT OVERWRITE TABLE target_table PARTITION (ds='2026-01-01') SELECT id FROM source_table", 999L, "RECORDS_OUT_1"),
                new SqlCase("INSERT OVERWRITE TABLE target_table PARTITION (ds) SELECT id, ds FROM source_table", 999L, "RECORDS_OUT_1"),
                new SqlCase("INSERT OVERWRITE DIRECTORY '/tmp/query-output' SELECT id FROM source_table", 999L, "RECORDS_OUT_1"),
                new SqlCase("FROM source_table INSERT INTO TABLE target_a SELECT id INSERT INTO TABLE target_b SELECT id", null, null));
        Map<String, SqlCase> expected = new LinkedHashMap<>();
        List<TimelineEntity> entities = new ArrayList<>();
        for (int index = 0; index < cases.size(); index++) {
            String dagId = "dag_1700000000000_0001_" + (index + 1);
            TimelineEntity dag = dagWithTimes(1700000000100L, 1700000001100L);
            dag.setEntityId(dagId);
            dag.addOtherInfo("callerId", "hive-query-" + dagId);
            entities.add(dag);
            entities.add(sqlExtraInfo(dagId, cases.get(index).sql));
            expected.put(dagId, cases.get(index));
        }
        entities.add(completedApplication());
        Path input = RollingStoreFixture.write(temp.resolve("sql-input"), entities.toArray(new TimelineEntity[0]));
        Path output = temp.resolve("sql-output");

        assertEquals(0, run("--input", input.toString(), "--output", output.toString()));

        Set<String> seen = new HashSet<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new LocalInputFile(output.resolve("result.parquet")))
                .withConf(new PlainParquetConfiguration()).build()) {
            GenericRecord row;
            while ((row = reader.read()) != null) {
                String dagId = row.get("dagId").toString();
                assertTrue(seen.add(dagId), "Duplicate DAG " + dagId);
                SqlCase query = expected.get(dagId);
                assertNotNull(query, "Unexpected DAG " + dagId);
                assertEquals(query.rows, row.get("resultRows"), query.sql);
                if (query.rows == null) {
                    assertNull(row.get("resultRowsKind"), query.sql);
                    assertNull(row.get("resultRowsSource"), query.sql);
                } else {
                    assertEquals("FILE_SINK_OUTPUT", row.get("resultRowsKind").toString(), query.sql);
                    assertEquals(query.counter, row.get("resultRowsSource").toString(), query.sql);
                }
                assertEquals(21, row.getSchema().getFields().size());
                assertEquals(query.sql, row.get("query").toString());
                assertEquals(APP, row.get("applicationId").toString());
                assertEquals("hive-query-" + dagId, row.get("hiveQueryId").toString());
                assertEquals(120L, row.get("cpuMilliseconds"));
                assertEquals(1700000000100L, row.get("startTime"));
                assertEquals(1700000001100L, row.get("endTime"));
                assertEquals(1000L, row.get("durationMilliseconds"));
            }
        }
        assertEquals(expected.keySet(), seen);
        List<String> first = rows(output);
        assertEquals(0, run("--input", input.toString(), "--output", output.toString()));
        assertEquals(first, rows(output), "SQL selection must remain identical on rerun");
    }

    @Test void keepsLatestEventAndOtherInfoTimestampsAndLogsConflicts() throws Exception {
        TimelineEntity dag = dagWithTimes(1700000000100L, 1700000000900L);
        dag.addEvent(event("DAG_STARTED", 1700000000200L));
        dag.addEvent(event("DAG_STARTED", 1700000000300L));
        dag.addEvent(event("DAG_FINISHED", 1700000001000L));
        dag.addEvent(event("DAG_FINISHED", 1700000001200L));
        Path input = RollingStoreFixture.write(temp.resolve("input"), dag, completedApplication());
        Path output = temp.resolve("output");
        StringWriter log = new StringWriter();

        assertEquals(0, run(log, "--input", input.toString(), "--output", output.toString()));

        assertTimestamps(output, 1700000000300L, 1700000001200L, 900L);
        assertWarning(log, "startTime", 1700000000300L, 1700000000200L, 1700000000300L);
        assertWarning(log, "endTime", 1700000001200L, 1700000001000L, 1700000001200L);
        assertWarning(log, "startTime", 1700000000100L, 1700000000300L, 1700000000300L);
        assertWarning(log, "endTime", 1700000000900L, 1700000001200L, 1700000001200L);
    }

    @Test void retainsLatestTimestampsWhenEarlierSnapshotIsReadLastAndLogsConflicts() throws Exception {
        Path input = Files.createDirectory(temp.resolve("snapshots"));
        // The catalog sorts paths: the snapshot with larger timestamps is scanned first.
        RollingStoreFixture.write(input.resolve("a-later"),
                dagWithTimes(1700000000400L, 1700000001800L), completedApplication());
        RollingStoreFixture.write(input.resolve("z-earlier"),
                dagWithTimes(1700000000200L, 1700000001400L), completedApplication());
        Path output = temp.resolve("output");
        StringWriter log = new StringWriter();

        assertEquals(0, run(log, "--input", input.toString(), "--output", output.toString()));

        assertTimestamps(output, 1700000000400L, 1700000001800L, 1400L);
        assertWarning(log, "startTime", 1700000000400L, 1700000000200L, 1700000000400L);
        assertWarning(log, "endTime", 1700000001800L, 1700000001400L, 1700000001800L);
    }

    @Test void equalTimestampsAndMissingValuesDoNotProduceConflictWarnings() throws Exception {
        Path input = Files.createDirectory(temp.resolve("snapshots"));
        TimelineEntity dag = dagWithTimes(1700000000100L, 1700000001100L);
        dag.addEvent(event("DAG_STARTED", 1700000000100L));
        dag.addEvent(event("DAG_FINISHED", 1700000001100L));
        RollingStoreFixture.write(input.resolve("a-complete"), dag, completedApplication());
        TimelineEntity sparse = RollingStoreFixture.entity("TEZ_DAG_ID", DAG, 1700000000000L);
        sparse.addEvent(event("DAG_STARTED", 1700000000100L));
        sparse.addEvent(event("DAG_FINISHED", 1700000001100L));
        TimelineEntity extra = RollingStoreFixture.entity("TEZ_DAG_EXTRA_INFO", DAG, 1700000000000L);
        extra.addOtherInfo("startTime", 1700000000100L);
        extra.addOtherInfo("endTime", 1700000001100L);
        RollingStoreFixture.write(input.resolve("z-sparse"), sparse, extra);
        Path output = temp.resolve("output");
        StringWriter log = new StringWriter();

        assertEquals(0, run(log, "--input", input.toString(), "--output", output.toString()));

        assertTimestamps(output, 1700000000100L, 1700000001100L, 1000L);
        assertFalse(log.toString().contains("WARN Conflicting timestamp"), log::toString);
    }

    private int run(String... args) {
        return run(new StringWriter(), args);
    }

    private int run(StringWriter output, String... args) {
        CommandLine cli = new CommandLine(new ParseCommand());
        StringWriter errors = new StringWriter();
        cli.setOut(new java.io.PrintWriter(output)); cli.setErr(new java.io.PrintWriter(errors));
        int code;
        try (LogCapture logs = new LogCapture()) {
            code = cli.execute(args);
            output.append(logs.text());
        }
        if (code == 0 || code == 2 || errors.toString().contains("completion")) return code;
        System.err.println(output);
        System.err.println(errors);
        return code;
    }

    private void assertTimestamps(Path output, long startTime, long endTime, long durationMilliseconds) throws Exception {
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new LocalInputFile(output.resolve("result.parquet")))
                .withConf(new PlainParquetConfiguration()).build()) {
            GenericRecord row = reader.read();
            assertNotNull(row);
            assertEquals(DAG, row.get("dagId").toString());
            assertEquals(APP, row.get("applicationId").toString());
            assertEquals(startTime, row.get("startTime"));
            assertEquals(endTime, row.get("endTime"));
            assertEquals(durationMilliseconds, row.get("durationMilliseconds"));
            assertNull(reader.read());
        }
    }

    private void assertWarning(StringWriter log, String field, long previous, long incoming, long selected) {
        String expected = "WARN Conflicting timestamp at " + DAG + "/" + field
                + ": previous=" + previous + " incoming=" + incoming + " selected=" + selected;
        assertTrue(log.toString().contains(expected), () -> "Missing warning: " + expected + "\nCLI output:\n" + log);
    }

    private TimelineEntity dagWithTimes(long startTime, long endTime) {
        TimelineEntity dag = RollingStoreFixture.entity("TEZ_DAG_ID", DAG, 1700000000000L);
        dag.addOtherInfo("applicationId", APP);
        dag.addOtherInfo("callerType", "HIVE_QUERY_ID");
        dag.addOtherInfo("startTime", startTime);
        dag.addOtherInfo("endTime", endTime);
        dag.addOtherInfo("status", "SUCCEEDED");
        return dag;
    }

    private TimelineEntity completedApplication() {
        TimelineEntity app = RollingStoreFixture.entity("YARN_APPLICATION", APP, 1700000000000L);
        app.addEvent(event("YARN_APPLICATION_FINISHED", 1700000002100L));
        return app;
    }

    private TimelineEvent event(String type, long timestamp) {
        TimelineEvent event = new TimelineEvent();
        event.setEventType(type);
        event.setTimestamp(timestamp);
        return event;
    }

    private List<String> rows(Path output) throws Exception {
        List<String> rows = new ArrayList<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new LocalInputFile(output.resolve("result.parquet")))
                .withConf(new PlainParquetConfiguration()).build()) {
            GenericRecord row;
            while ((row = reader.read()) != null) rows.add(row.toString());
        }
        return rows;
    }

    private TimelineEntity sqlExtraInfo(String dagId, String sql) throws Exception {
        TimelineEntity extra = RollingStoreFixture.entity("TEZ_DAG_EXTRA_INFO", dagId, 1700000000000L);
        Map<String, String> dagInfo = new LinkedHashMap<>();
        dagInfo.put("context", "Hive");
        dagInfo.put("description", sql);
        extra.addOtherInfo("dagPlan", Collections.singletonMap("dagInfo", new ObjectMapper().writeValueAsString(dagInfo)));
        Map<String, Object> hive = new LinkedHashMap<>();
        hive.put("counterGroupName", "HIVE");
        hive.put("counters", Arrays.asList(counter("RECORDS_OUT_0", 42L), counter("RECORDS_OUT_1", 999L)));
        Map<String, Object> task = new LinkedHashMap<>();
        task.put("counterGroupName", "org.apache.tez.common.counters.TaskCounter");
        task.put("counters", Collections.singletonList(counter("CPU_MILLISECONDS", 120L)));
        extra.addOtherInfo("counters", Collections.singletonMap("counterGroups", Arrays.asList(hive, task)));
        return extra;
    }

    private Map<String, Object> counter(String name, long value) {
        Map<String, Object> counter = new LinkedHashMap<>();
        counter.put("counterName", name);
        counter.put("counterValue", value);
        return counter;
    }

    private static final class SqlCase {
        private final String sql;
        private final Long rows;
        private final String counter;
        private SqlCase(String sql, Long rows, String counter) {
            this.sql = sql;
            this.rows = rows;
            this.counter = counter;
        }
    }

    private Path fixture(boolean completed) throws Exception {
        TimelineEntity dag = RollingStoreFixture.entity("TEZ_DAG_ID", DAG, 1700000000000L);
        dag.addOtherInfo("applicationId", APP); dag.addOtherInfo("user", "analyst");
        dag.addOtherInfo("callerId", "hive-query-1"); dag.addOtherInfo("callerType", "HIVE_QUERY_ID");
        dag.addOtherInfo("startTime", 1700000000100L); dag.addOtherInfo("endTime", 1700000001100L);
        dag.addOtherInfo("status", "SUCCEEDED"); dag.addOtherInfo("numCompletedTasks", 3);
        TimelineEntity extra = RollingStoreFixture.entity("TEZ_DAG_EXTRA_INFO", DAG, 1700000000000L);
        Map<String,Object> counter = new LinkedHashMap<>();
        counter.put("counterName", "CPU_MILLISECONDS"); counter.put("counterValue", 120L);
        Map<String,Object> group = new LinkedHashMap<>();
        group.put("counterGroupName", "org.apache.tez.common.counters.TaskCounter");
        group.put("counters", Arrays.asList(counter));
        Map<String,Object> sink = new LinkedHashMap<>();
        sink.put("counterName", "RECORDS_OUT_0"); sink.put("counterValue", 137L);
        Map<String,Object> hive = new LinkedHashMap<>();
        hive.put("counterGroupName", "HIVE"); hive.put("counters", Arrays.asList(sink));
        Map<String,Object> counters = new LinkedHashMap<>(); counters.put("counterGroups", Arrays.asList(group, hive));
        extra.addOtherInfo("counters", counters);
        TimelineEntity app = RollingStoreFixture.entity("YARN_APPLICATION", APP, 1700000000000L);
        if (completed) {
            TimelineEvent event = new TimelineEvent(); event.setEventType("YARN_APPLICATION_FINISHED");
            event.setTimestamp(1700000002100L); event.addEventInfo("YARN_APPLICATION_STATE", "FINISHED"); app.addEvent(event);
        }
        return RollingStoreFixture.write(temp.resolve("input"), dag, extra, app);
    }
}
