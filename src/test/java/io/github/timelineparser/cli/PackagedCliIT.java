package io.github.timelineparser.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.timelineparser.fixture.RollingStoreFixture;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises native library extraction and shaded dependencies in a separate Java process. */
class PackagedCliIT {
    @TempDir Path temp;

    @Test void packagedCliAndManifestReportTheMavenVersion() throws Exception {
        String expected = System.getProperty("parser.version");
        try (java.util.jar.JarFile jar = new java.util.jar.JarFile(System.getProperty("parser.jar"))) {
            assertEquals(expected, jar.getManifest().getMainAttributes().getValue("Implementation-Version"));
        }
        Path log = temp.resolve("version.txt");
        Process process = new ProcessBuilder(Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                "-jar", System.getProperty("parser.jar"), "--version")
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS));
            assertEquals(0, process.exitValue());
            assertEquals(expected, new String(Files.readAllBytes(log), StandardCharsets.UTF_8).trim());
        } finally { process.destroyForcibly(); }
    }

    @Test void convertsTenThousandDagsWithBoundedHeapAndSafeRerun() throws Exception {
        String appId = "application_1700000000000_0001";
        List<TimelineEntity> entities = new ArrayList<>();
        for (int index = 1; index <= 10000; index++) {
            TimelineEntity dag = RollingStoreFixture.entity("TEZ_DAG_ID", "dag_1700000000000_0001_" + index, 1700000000000L);
            dag.addOtherInfo("user", "분석가");
            dag.addOtherInfo("applicationId", appId);
            dag.addOtherInfo("callerId", "hive-query-" + index);
            dag.addOtherInfo("callerType", "HIVE_QUERY_ID");
            dag.addOtherInfo("status", "SUCCEEDED");
            dag.addOtherInfo("startTime", 1700000000100L);
            dag.addOtherInfo("endTime", 1700000001100L);
            entities.add(dag);
            // Exercise the SQL parser inside the shaded JAR, including its runtime dependencies.
            if (index == 1 || index == 2) {
                entities.add(sqlExtraInfo(dag.getEntityId(), index == 1
                        ? "SELECT id FROM source_table"
                        : "CREATE TABLE target_table AS SELECT id FROM source_table"));
            }
        }
        TimelineEntity app = RollingStoreFixture.entity("YARN_APPLICATION", appId, 1700000000000L);
        TimelineEvent end = new TimelineEvent();
        end.setEventType("YARN_APPLICATION_FINISHED"); end.setTimestamp(1700000002100L);
        app.addEvent(end); entities.add(app);
        Path input = RollingStoreFixture.write(temp.resolve("input"), entities.toArray(new TimelineEntity[0]));
        entities.clear();
        Map<String, String> before = hashes(input);
        Path output = temp.resolve("output");
        assertEquals(0, invoke(input, output));
        assertRows(output);
        assertEquals(before, hashes(input), "Every source database file must remain unchanged");
        assertEquals(0, invoke(input, output));
        assertRows(output);
        byte[] previous = Files.readAllBytes(output.resolve("result.parquet"));
        Path broken = Files.createDirectories(temp.resolve("broken/entity-ldb.2023-11-14"));
        Files.write(broken.resolve("CURRENT"), "MANIFEST-999999\n".getBytes(StandardCharsets.UTF_8));
        assertEquals(1, invoke(broken, output));
        assertArrayEquals(previous, Files.readAllBytes(output.resolve("result.parquet")));
        try (Stream<Path> paths = Files.list(output)) {
            assertEquals(new HashSet<>(Arrays.asList("result.parquet", ".timeline-parser.lock")),
                    paths.map(path -> path.getFileName().toString()).collect(java.util.stream.Collectors.toSet()));
        }
    }

    private int invoke(Path input, Path output) throws Exception {
        Path log = Files.createTempFile(temp, "process-", ".log");
        Process process = new ProcessBuilder(Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx512m", "-jar", System.getProperty("parser.jar"), "--input", input.toString(), "--output", output.toString())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue(process.waitFor(120, TimeUnit.SECONDS), "Packaged parser timed out");
            String messages = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
            System.out.println("Packaged Java 8 CLI: " + messages);
            return process.exitValue();
        } finally { process.destroyForcibly(); }
    }

    private void assertRows(Path output) throws Exception {
        Set<String> ids = new HashSet<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new LocalInputFile(output.resolve("result.parquet")))
                .withConf(new PlainParquetConfiguration()).build()) {
            GenericRecord row;
            while ((row = reader.read()) != null) {
                String dagId = row.get("dagId").toString();
                assertTrue(ids.add(dagId));
                assertEquals(20, row.getSchema().getFields().size());
                assertEquals("분석가", row.get("user").toString());
                assertEquals(1000L, row.get("durationMilliseconds"));
                if ("dag_1700000000000_0001_1".equals(dagId)) {
                    assertEquals(42L, row.get("resultRows"));
                    assertEquals("FILE_SINK_OUTPUT", row.get("resultRowsKind").toString());
                    assertEquals("RECORDS_OUT_0", row.get("resultRowsSource").toString());
                } else if ("dag_1700000000000_0001_2".equals(dagId)) {
                    assertEquals(999L, row.get("resultRows"));
                    assertEquals("FILE_SINK_OUTPUT", row.get("resultRowsKind").toString());
                    assertEquals("RECORDS_OUT_1", row.get("resultRowsSource").toString());
                } else {
                    assertNull(row.get("resultRows"));
                }
            }
        }
        assertEquals(10000, ids.size());
    }

    private TimelineEntity sqlExtraInfo(String dagId, String sql) throws Exception {
        TimelineEntity extra = RollingStoreFixture.entity("TEZ_DAG_EXTRA_INFO", dagId, 1700000000000L);
        Map<String, String> dagInfo = new LinkedHashMap<>();
        dagInfo.put("context", "Hive");
        dagInfo.put("description", sql);
        extra.addOtherInfo("dagPlan", Collections.singletonMap("dagInfo", new ObjectMapper().writeValueAsString(dagInfo)));
        Map<String, Object> hive = new LinkedHashMap<>();
        hive.put("counterGroupName", "HIVE");
        Map<String, Object> select = new LinkedHashMap<>();
        select.put("counterName", "RECORDS_OUT_0"); select.put("counterValue", 42L);
        Map<String, Object> table = new LinkedHashMap<>();
        table.put("counterName", "RECORDS_OUT_1"); table.put("counterValue", 999L);
        hive.put("counters", Arrays.asList(select, table));
        extra.addOtherInfo("counters", Collections.singletonMap("counterGroups", Collections.singletonList(hive)));
        return extra;
    }

    private Map<String, String> hashes(Path root) throws Exception {
        Map<String, String> hashes = new TreeMap<>();
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path file : (Iterable<Path>) paths.filter(Files::isRegularFile)::iterator) {
                hashes.put(root.relativize(file).toString(), Base64.getEncoder().encodeToString(
                        MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file))));
            }
        }
        return hashes;
    }
}
