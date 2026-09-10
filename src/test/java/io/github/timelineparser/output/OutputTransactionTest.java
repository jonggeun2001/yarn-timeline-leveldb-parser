package io.github.timelineparser.output;

import io.github.timelineparser.domain.DagRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class OutputTransactionTest {
    @TempDir Path directory;

    @Test
    void closingWithoutCommitPreservesPreviousResultAndOnlyCleansOwnTemporaryFile() throws Exception {
        Path output = directory.resolve("output");
        Files.createDirectories(output);
        Path previous = output.resolve("result.parquet");
        byte[] oldBytes = "existing-result".getBytes(StandardCharsets.UTF_8);
        Files.write(previous, oldBytes);
        Path unrelated = output.resolve("notes.txt");
        Files.write(unrelated, new byte[]{9});
        Path abandoned = output.resolve(".result-other.tmp");
        Files.write(abandoned, new byte[]{7});
        Path temporary;
        try (OutputTransaction transaction = OutputTransaction.open(output)) {
            assertEquals(output.toRealPath(), transaction.outputDirectory());
            temporary = transaction.temporaryFile();
            assertFalse(Files.exists(temporary));
            assertEquals(output.toRealPath(), temporary.getParent());
            new ParquetOutput().write(temporary, Collections.emptyList());
            assertArrayEquals(oldBytes, Files.readAllBytes(previous));
        }
        assertFalse(Files.exists(temporary));
        assertArrayEquals(oldBytes, Files.readAllBytes(previous));
        assertArrayEquals(new byte[]{9}, Files.readAllBytes(unrelated));
        assertArrayEquals(new byte[]{7}, Files.readAllBytes(abandoned));
        assertTrue(Files.exists(output.resolve(".timeline-parser.lock")));
    }

    @Test
    void commitsACompleteReplacementAndCanReplaceItWithZeroRows() throws Exception {
        Path output = directory.resolve("output");
        try (OutputTransaction transaction = OutputTransaction.open(output)) {
            new ParquetOutput().write(transaction.temporaryFile(), Collections.singletonList(
                    new DagRecord(ParquetOutputTest.requiredValues("dag_1_0001_1"))));
            transaction.commit();
            assertFalse(Files.exists(transaction.temporaryFile()));
        }
        try (ParquetReader<GenericRecord> reader = ParquetOutputTest.reader(output.resolve("result.parquet"))) {
            assertEquals("dag_1_0001_1", reader.read().get("dagId").toString());
            assertNull(reader.read());
        }
        try (OutputTransaction transaction = OutputTransaction.open(output)) {
            new ParquetOutput().write(transaction.temporaryFile(), Collections.emptyList());
            transaction.commit();
        }
        try (ParquetReader<GenericRecord> reader = ParquetOutputTest.reader(output.resolve("result.parquet"))) {
            assertNull(reader.read());
        }
    }

    @Test
    void uncheckedCleanupFailureStillReleasesTheOutputLock() throws Exception {
        Path output = directory.resolve("output");
        OutputTransaction transaction = OutputTransaction.open(output);
        SecurityManager previous = System.getSecurityManager();
        try {
            System.setSecurityManager(new SecurityManager() {
                @Override public void checkPermission(java.security.Permission permission) { }
                @Override public void checkDelete(String file) {
                    if (transaction.temporaryFile().toString().equals(file))
                        throw new SecurityException("denied temporary cleanup");
                }
            });
            IOException failure = assertThrows(IOException.class, transaction::close);
            assertInstanceOf(SecurityException.class, failure.getCause());
        } finally {
            System.setSecurityManager(previous);
        }
        try (OutputTransaction ignored = OutputTransaction.open(output)) {
            assertTrue(Files.exists(output.resolve(".timeline-parser.lock")));
        }
    }

    @Test
    void rejectsTruncatedParquetBeforeReplacingPreviousResult() throws Exception {
        Path output = directory.resolve("output");
        Files.createDirectories(output);
        Path previous = output.resolve("result.parquet");
        byte[] oldBytes = {5, 4, 3};
        Files.write(previous, oldBytes);
        try (OutputTransaction transaction = OutputTransaction.open(output)) {
            Files.write(transaction.temporaryFile(), new byte[]{'P', 'A', 'R', '1', 0});
            assertThrows(IOException.class, transaction::commit);
            assertArrayEquals(oldBytes, Files.readAllBytes(previous));
        }
    }

    @Test
    void rejectsConcurrentLockAndAllowsReopeningAfterClose() throws Exception {
        Path output = directory.resolve("output");
        try (OutputTransaction first = OutputTransaction.open(output)) {
            assertThrows(OutputTransaction.LockUnavailableException.class, () -> OutputTransaction.open(output));
            Process child = new ProcessBuilder(javaExecutable(), "-cp", System.getProperty("java.class.path"),
                    LockProbe.class.getName(), output.toString()).redirectErrorStream(true).start();
            try {
                assertTrue(child.waitFor(30, TimeUnit.SECONDS), "lock probe timed out");
                assertEquals(23, child.exitValue(), "a separate JVM must be refused by the OS file lock");
            } finally {
                child.destroyForcibly();
            }
        }
        try (OutputTransaction reopened = OutputTransaction.open(output)) {
            assertFalse(Files.exists(reopened.temporaryFile()));
        }
    }

    @Test
    void rejectsCommitAfterClose() throws Exception {
        OutputTransaction transaction = OutputTransaction.open(directory.resolve("output"));
        transaction.close();
        assertThrows(IOException.class, transaction::commit);
    }

    @Test
    void rejectsAReadableParquetWithTheWrongSchema() throws Exception {
        Schema schema = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Other\","
                + "\"fields\":[{\"name\":\"value\",\"type\":\"string\"}]}");
        try (OutputTransaction transaction = OutputTransaction.open(directory.resolve("output"))) {
            Path previous = transaction.outputDirectory().resolve("result.parquet");
            Files.write(previous, new byte[]{8});
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                    new LocalOutputFile(transaction.temporaryFile())).withSchema(schema).build()) {
                GenericRecord row = new GenericData.Record(schema);
                row.put("value", "other dataset");
                writer.write(row);
            }
            assertThrows(IOException.class, transaction::commit);
            assertArrayEquals(new byte[]{8}, Files.readAllBytes(previous));
        }
    }

    @Test
    void rejectsExpectedCountThatDoesNotMatchTheClosedFooter() throws Exception {
        Schema schema;
        try (InputStream stream = getClass().getResourceAsStream("/dag-metrics.avsc")) {
            assertNotNull(stream);
            schema = new Schema.Parser().parse(stream);
        }
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("timeline.schema.version", "2");
        metadata.put("timeline.parser.version", System.getProperty("parser.version"));
        metadata.put("timeline.mapping.version", "tez-0.9.1-v1");
        metadata.put("timeline.row.count", "2");
        try (OutputTransaction transaction = OutputTransaction.open(directory.resolve("output"))) {
            Path previous = transaction.outputDirectory().resolve("result.parquet");
            Files.write(previous, new byte[]{8});
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                    new LocalOutputFile(transaction.temporaryFile())).withSchema(schema).withExtraMetaData(metadata).build()) {
                GenericRecord row = new GenericData.Record(schema);
                row.put("dagId", "dag_1_0001_1");
                row.put("applicationId", "application_1_0001");
                writer.write(row);
            }
            IOException failure = assertThrows(IOException.class, transaction::commit);
            assertTrue(failure.getMessage().contains("Invalid Parquet row count"), failure.getMessage());
            assertArrayEquals(new byte[]{8}, Files.readAllBytes(previous));
        }
    }

    @Test
    void failedAtomicReplacementDoesNotDeleteTheExistingTarget() throws Exception {
        Path temporary;
        Path result;
        try (OutputTransaction transaction = OutputTransaction.open(directory.resolve("output"))) {
            temporary = transaction.temporaryFile();
            result = transaction.outputDirectory().resolve("result.parquet");
            Files.createDirectory(result);
            Files.write(result.resolve("keep.txt"), new byte[]{6});
            new ParquetOutput().write(temporary, Collections.emptyList());
            assertThrows(IOException.class, transaction::commit);
            assertArrayEquals(new byte[]{6}, Files.readAllBytes(result.resolve("keep.txt")));
            assertTrue(Files.exists(temporary));
        }
        assertFalse(Files.exists(temporary));
        assertTrue(Files.isDirectory(result));
    }

    @Test
    void removesOnlyRecognizedOrphansAfterTakingTheOutputLock() throws Exception {
        Path output = Files.createDirectory(directory.resolve("output"));
        Path orphanResult = output.resolve(".result-11111111-1111-1111-1111-111111111111.tmp");
        Files.write(orphanResult, new byte[]{1});
        Path orphanWork = Files.createDirectory(output.resolve(".work-22222222-2222-2222-2222-222222222222"));
        Files.write(orphanWork.resolve(".timeline-parser-work"), "timeline-parser-work-v1\n".getBytes(StandardCharsets.UTF_8));
        Path data = Files.createDirectory(orphanWork.resolve("database"));
        Files.write(data.resolve("CURRENT"), new byte[]{2});
        Path outside = Files.createDirectory(directory.resolve("outside"));
        Files.write(outside.resolve("keep"), new byte[]{3});
        Files.createSymbolicLink(data.resolve("linked-outside"), outside);
        Path unmarked = Files.createDirectory(output.resolve(".work-33333333-3333-3333-3333-333333333333"));
        Files.write(unmarked.resolve("keep"), new byte[]{4});
        Path wrongMarker = Files.createDirectory(output.resolve(".work-44444444-4444-4444-4444-444444444444"));
        Files.write(wrongMarker.resolve(".timeline-parser-work"), "unrelated\n".getBytes(StandardCharsets.UTF_8));
        Path unrelated = output.resolve(".result-other.tmp");
        Files.write(unrelated, new byte[]{5});
        Path linkedResult = output.resolve(".result-55555555-5555-5555-5555-555555555555.tmp");
        Files.createSymbolicLink(linkedResult, outside.resolve("keep"));
        Path linkedWork = output.resolve(".work-66666666-6666-6666-6666-666666666666");
        Files.createSymbolicLink(linkedWork, outside);
        Path markerLinkWork = Files.createDirectory(output.resolve(".work-77777777-7777-7777-7777-777777777777"));
        Path markerTarget = directory.resolve("external-marker");
        Files.write(markerTarget, "timeline-parser-work-v1\n".getBytes(StandardCharsets.UTF_8));
        Files.createSymbolicLink(markerLinkWork.resolve(".timeline-parser-work"), markerTarget);

        try (OutputTransaction ignored = OutputTransaction.open(output)) {
            assertFalse(Files.exists(orphanResult), "recognized unfinished result must be removed");
            assertFalse(Files.exists(orphanWork), "marked abandoned work directory must be removed");
            assertArrayEquals(new byte[]{3}, Files.readAllBytes(outside.resolve("keep")));
            assertArrayEquals(new byte[]{4}, Files.readAllBytes(unmarked.resolve("keep")));
            assertTrue(Files.isDirectory(wrongMarker));
            assertTrue(Files.exists(unrelated));
            assertTrue(Files.isSymbolicLink(linkedResult));
            assertTrue(Files.isSymbolicLink(linkedWork));
            assertTrue(Files.isSymbolicLink(markerLinkWork.resolve(".timeline-parser-work")));
            assertTrue(Files.exists(output.resolve(".timeline-parser.lock")));
        }
    }

    @Test
    void doesNotRemoveOrphansWhenAnotherTransactionHoldsTheLock() throws Exception {
        Path output = directory.resolve("output");
        Path orphan;
        try (OutputTransaction ignored = OutputTransaction.open(output)) {
            orphan = output.resolve(".result-88888888-8888-8888-8888-888888888888.tmp");
            Files.write(orphan, new byte[]{7});
            assertThrows(OutputTransaction.LockUnavailableException.class, () -> OutputTransaction.open(output));
            assertArrayEquals(new byte[]{7}, Files.readAllBytes(orphan));
        }
        assertTrue(Files.exists(orphan));
        try (OutputTransaction ignored = OutputTransaction.open(output)) {
            assertFalse(Files.exists(orphan));
        }
    }

    private static String javaExecutable() {
        return java.nio.file.Paths.get(System.getProperty("java.home"), "bin", "java").toString();
    }

    public static class LockProbe {
        public static void main(String[] args) throws Exception {
            try (OutputTransaction ignored = OutputTransaction.open(java.nio.file.Paths.get(args[0]))) {
                System.exit(0);
            } catch (OutputTransaction.LockUnavailableException expected) {
                System.exit(23);
            }
        }
    }
}
