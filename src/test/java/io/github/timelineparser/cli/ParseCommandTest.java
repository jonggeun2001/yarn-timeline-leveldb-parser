package io.github.timelineparser.cli;

import static org.junit.jupiter.api.Assertions.*;
import io.github.timelineparser.fixture.RollingStoreFixture;
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

class ParseCommandTest {
    @TempDir Path temp;
    private static final String APP = "application_1700000000000_0001";
    private static final String DAG = "dag_1700000000000_0001_1";

    @Test void transformsRealHadoopDatabaseAndReplacesRatherThanAppending() throws Exception {
        Path input = fixture(true);
        Path output = temp.resolve("output");
        assertEquals(0, run("--input", input.toString(), "--output", output.toString()));
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

    @Test void usesVerifiedSelectAndCtasMappingsInTheFullPipeline() throws Exception {
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

    private int run(String... args) {
        CommandLine cli = new CommandLine(new ParseCommand());
        StringWriter output = new StringWriter();
        cli.setOut(new java.io.PrintWriter(output)); cli.setErr(new java.io.PrintWriter(output));
        int code = cli.execute(args);
        if (code == 0 || code == 2 || output.toString().contains("completion")) return code;
        System.err.println(output);
        return code;
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
