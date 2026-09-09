package io.github.timelineparser.cli;

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
                assertTrue(ids.add(row.get("dagId").toString()));
                assertEquals("분석가", row.get("user").toString());
                assertEquals(1000L, row.get("durationMilliseconds"));
                assertNull(row.get("resultRows"));
            }
        }
        assertEquals(10000, ids.size());
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
