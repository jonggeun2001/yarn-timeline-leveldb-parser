package io.github.timelineparser.cli;

import io.github.timelineparser.fixture.RollingStoreFixture;
import io.github.timelineparser.fixture.LogCapture;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import picocli.CommandLine;
import static org.junit.jupiter.api.Assertions.*;

@ResourceLock(Resources.SYSTEM_ERR)
class ErrorRecoveryTest {
    @TempDir Path temp;
    private static final String APP = "application_1700000000000_0001";
    private static final String DAG = "dag_1700000000000_0001_1";

    @Test void directCallWithoutInjectedArgumentsReturnsControlledFailure() {
        assertEquals(2, assertDoesNotThrow(() -> new ParseCommand().call()));
    }

    @Test void brokenDatabaseDoesNotPreventScanningHealthyDatabaseOrReplacePreviousOutput() throws Exception {
        Path input = Files.createDirectory(temp.resolve("input"));
        Files.createDirectories(input.resolve("a-broken/entity-ldb.invalid"));
        RollingStoreFixture.write(input.resolve("z-healthy"), dag(), completed());
        Path output = Files.createDirectory(temp.resolve("output"));
        byte[] previous = "previous result".getBytes(StandardCharsets.UTF_8);
        Files.write(output.resolve("result.parquet"), previous);
        StringWriter log = new StringWriter();
        int code = run(log, input, output);
        assertEquals(1, code);
        assertTrue(log.toString().contains("a-broken"), log::toString);
        assertTrue(log.toString().contains("Scanning database") && log.toString().contains("z-healthy"), log::toString);
        assertTrue(log.toString().contains("preserv"), log::toString);
        assertArrayEquals(previous, Files.readAllBytes(output.resolve("result.parquet")));
    }

    @Test void allDiscardedDagEntitiesCannotPublishAnEmptyReplacement() throws Exception {
        TimelineEntity invalid = RollingStoreFixture.entity("TEZ_DAG_ID", "bad-dag-id", 1700000000000L);
        Path input = RollingStoreFixture.write(temp.resolve("input"), invalid, completed());
        Path output = Files.createDirectory(temp.resolve("output"));
        byte[] previous = "previous result".getBytes(StandardCharsets.UTF_8);
        Files.write(output.resolve("result.parquet"), previous);
        StringWriter log = new StringWriter();
        assertEquals(1, run(log, input, output));
        assertTrue(log.toString().contains("bad-dag-id"), log::toString);
        assertTrue(log.toString().contains("preserv"), log::toString);
        assertArrayEquals(previous, Files.readAllBytes(output.resolve("result.parquet")));
    }

    @Test void validationLogEscapesControlCharactersInInputPaths() {
        StringWriter errors = new StringWriter();
        Path missing = temp.resolve("missing\ninput");
        CommandLine command = new CommandLine(new ParseCommand()).setErr(new PrintWriter(errors));
        assertEquals(2, command.execute("--input", missing.toString(), "--output", temp.resolve("output").toString()));
        assertTrue(errors.toString().contains("missing\\ninput"), errors::toString);
        assertFalse(errors.toString().contains("missing\ninput"), errors::toString);
        assertTrue(errors.toString().contains("NoSuchFileException"), errors::toString);
    }

    @Test void malformedOptionalFieldsAndBadDagDoNotStopHealthyRows() throws Exception {
        TimelineEntity partial = RollingStoreFixture.entity("TEZ_DAG_ID", "dag_1700000000000_0001_2", 1700000000000L);
        partial.addOtherInfo("startTime", "invalid-time");
        partial.addOtherInfo("endTime", 1700000001100L);
        partial.addOtherInfo("callerId", 42);
        TimelineEntity invalid = RollingStoreFixture.entity("TEZ_DAG_ID", "bad-dag-id", 1700000000000L);
        Path input = RollingStoreFixture.write(temp.resolve("input"), dag(), partial, invalid, completed());
        Path output = temp.resolve("output");
        StringWriter log = new StringWriter();
        assertEquals(0, run(log, input, output), log::toString);
        Map<String, GenericRecord> rows = new HashMap<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                new LocalInputFile(output.resolve("result.parquet")))
                .withConf(new PlainParquetConfiguration()).build()) {
            GenericRecord row;
            while ((row = reader.read()) != null) rows.put(row.get("dagId").toString(), row);
        }
        assertEquals(2, rows.size());
        assertEquals(1700000000100L, rows.get(DAG).get("startTime"));
        assertEquals(1000L, rows.get(DAG).get("durationMilliseconds"));
        GenericRecord recovered = rows.get(partial.getEntityId());
        assertNotNull(recovered);
        assertNull(recovered.get("startTime"));
        assertNull(recovered.get("durationMilliseconds"));
        assertNull(recovered.get("hiveQueryId"));
        assertEquals(1700000001100L, recovered.get("endTime"));
        assertTrue(log.toString().contains("bad-dag-id"), log::toString);
        assertTrue(log.toString().contains(partial.getEntityId() + "/startTime"), log::toString);
    }

    private int run(StringWriter log, Path input, Path output) {
        try (LogCapture logs = new LogCapture()) {
            int code = new CommandLine(new ParseCommand()).setOut(new PrintWriter(log)).setErr(new PrintWriter(log))
                    .execute("--input", input.toString(), "--output", output.toString());
            log.append(logs.text());
            return code;
        }
    }

    private TimelineEntity dag() {
        TimelineEntity dag = RollingStoreFixture.entity("TEZ_DAG_ID", DAG, 1700000000000L);
        dag.addOtherInfo("startTime", 1700000000100L);
        dag.addOtherInfo("endTime", 1700000001100L);
        return dag;
    }

    private TimelineEntity completed() {
        TimelineEntity app = RollingStoreFixture.entity("YARN_APPLICATION", APP, 1700000000000L);
        TimelineEvent event = new TimelineEvent();
        event.setEventType("YARN_APPLICATION_FINISHED");
        event.setTimestamp(1700000002100L);
        app.addEvent(event);
        return app;
    }
}
