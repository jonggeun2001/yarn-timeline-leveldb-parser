package io.github.timelineparser.output;

import io.github.timelineparser.domain.DagRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ParquetOutputTest {
    @TempDir Path directory;

    @Test
    void roundTripsNullableMetricsUtcTimestampsLargeInt64AndOriginalQueryWithContractSchema() throws Exception {
        Map<String, Object> values = requiredValues("dag_1_0001_1");
        String query = "  -- 분석 쿼리 원문\r\nSELECT\t'서울 🐝' AS city /* 주석 */\nFROM events;  \n";
        values.put("query", query);
        values.put("user", "분석가");
        values.put("hiveQueryId", "query-1");
        values.put("startTime", 1788912000123L);
        values.put("endTime", 1788912001123L);
        values.put("cpuMilliseconds", 4294967296L);
        values.put("status", "SUCCEEDED");
        values.put("queueName", "analytics");
        values.put("durationMilliseconds", 1000L);
        values.put("gcMilliseconds", 0L);
        values.put("hdfsBytesRead", Long.MAX_VALUE);
        values.put("hdfsBytesWritten", 200L);
        values.put("shuffleBytes", 300L);
        values.put("additionalSpillBytesWritten", 400L);
        values.put("totalTasks", 5L);
        values.put("failedTaskAttempts", 0L);
        Path file = directory.resolve("metrics.parquet");

        new ParquetOutput().write(file, Arrays.asList(new DagRecord(values),
                new DagRecord(requiredValues("dag_1_0001_2"))));

        assertTrue(Files.isRegularFile(file), "write must create a Parquet file");
        try (ParquetReader<GenericRecord> reader = reader(file)) {
            GenericRecord row = reader.read();
            assertEquals("dag_1_0001_1", row.get("dagId").toString());
            assertEquals("application_1_0001", row.get("applicationId").toString());
            assertEquals("분석가", row.get("user").toString());
            assertEquals("query-1", row.get("hiveQueryId").toString());
            assertEquals(query, row.get("query").toString());
            assertEquals(1788912000123L, row.get("startTime"));
            assertEquals(1788912001123L, row.get("endTime"));
            assertEquals(4294967296L, row.get("cpuMilliseconds"));
            assertEquals(Long.MAX_VALUE, row.get("hdfsBytesRead"));
            assertEquals(0L, row.get("gcMilliseconds"));
            assertEquals(200L, row.get("hdfsBytesWritten"));
            assertEquals(300L, row.get("shuffleBytes"));
            assertEquals(400L, row.get("additionalSpillBytesWritten"));
            assertEquals(5L, row.get("totalTasks"));
            assertEquals(0L, row.get("failedTaskAttempts"));
            assertEquals(1000L, row.get("durationMilliseconds"));
            assertEquals("SUCCEEDED", row.get("status").toString());
            assertEquals("analytics", row.get("queueName").toString());
            assertNull(row.get("resultRows"));
            assertNull(row.get("resultRowsKind"));
            assertNull(row.get("resultRowsSource"));
            GenericRecord sparse = reader.read();
            assertEquals("dag_1_0001_2", sparse.get("dagId").toString());
            for (String field : Arrays.asList("user", "startTime", "endTime", "cpuMilliseconds", "gcMilliseconds", "query")) {
                assertNull(sparse.get(field), field);
            }
            assertNull(reader.read());
        }
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            ParquetMetadata footer = reader.getFooter();
            assertEquals(2L, reader.getRecordCount());
            assertEquals(Arrays.asList("dagId", "applicationId", "user", "hiveQueryId", "startTime", "endTime",
                    "resultRows", "cpuMilliseconds", "status", "queueName", "durationMilliseconds", "gcMilliseconds",
                    "hdfsBytesRead", "hdfsBytesWritten", "shuffleBytes", "additionalSpillBytesWritten", "totalTasks",
                    "failedTaskAttempts", "resultRowsKind", "resultRowsSource", "query"),
                    footer.getFileMetaData().getSchema().getFields().stream().map(Type::getName).collect(Collectors.toList()));
            assertTrue(footer.getFileMetaData().getSchema().getType("dagId").isRepetition(Type.Repetition.REQUIRED));
            assertTrue(footer.getFileMetaData().getSchema().getType("applicationId").isRepetition(Type.Repetition.REQUIRED));
            PrimitiveType timestamp = footer.getFileMetaData().getSchema().getType("startTime").asPrimitiveType();
            assertEquals(PrimitiveType.PrimitiveTypeName.INT64, timestamp.getPrimitiveTypeName());
            assertEquals(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS),
                    timestamp.getLogicalTypeAnnotation());
            assertEquals(PrimitiveType.PrimitiveTypeName.INT64,
                    footer.getFileMetaData().getSchema().getType("cpuMilliseconds").asPrimitiveType().getPrimitiveTypeName());
            PrimitiveType queryType = footer.getFileMetaData().getSchema().getType("query").asPrimitiveType();
            assertTrue(queryType.isRepetition(Type.Repetition.OPTIONAL));
            assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, queryType.getPrimitiveTypeName());
            assertEquals(LogicalTypeAnnotation.stringType(), queryType.getLogicalTypeAnnotation());
            assertEquals("2", footer.getFileMetaData().getKeyValueMetaData().get("timeline.schema.version"));
            assertEquals("2", footer.getFileMetaData().getKeyValueMetaData().get("timeline.row.count"));
            assertEquals(System.getProperty("parser.version"), footer.getFileMetaData().getKeyValueMetaData().get("timeline.parser.version"));
            assertEquals("tez-0.9.1-v1", footer.getFileMetaData().getKeyValueMetaData().get("timeline.mapping.version"));
            footer.getBlocks().forEach(block -> block.getColumns().forEach(column ->
                    assertEquals(CompressionCodecName.SNAPPY, column.getCodec())));
        }
        try (Stream<Path> files = Files.list(directory)) {
            assertEquals(Collections.singletonList("metrics.parquet"),
                    files.map(path -> path.getFileName().toString()).collect(Collectors.toList()));
        }
    }

    @Test
    void producesReadableZeroRowFileWithSchema() throws Exception {
        Path file = directory.resolve("empty.parquet");
        new ParquetOutput().write(file, Collections.emptyList());
        assertTrue(Files.isRegularFile(file), "zero rows must still produce a file");
        try (ParquetReader<GenericRecord> reader = reader(file)) {
            assertNull(reader.read());
        }
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            assertEquals(0L, reader.getRecordCount());
            assertEquals(21, reader.getFooter().getFileMetaData().getSchema().getFieldCount());
            PrimitiveType queryType = reader.getFooter().getFileMetaData().getSchema().getType("query").asPrimitiveType();
            assertTrue(queryType.isRepetition(Type.Repetition.OPTIONAL));
            assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, queryType.getPrimitiveTypeName());
            assertEquals(LogicalTypeAnnotation.stringType(), queryType.getLogicalTypeAnnotation());
            assertEquals("2", reader.getFooter().getFileMetaData().getKeyValueMetaData().get("timeline.schema.version"));
            assertEquals("0", reader.getFooter().getFileMetaData().getKeyValueMetaData().get("timeline.row.count"));
        }
    }

    @Test
    void refusesToOverwriteAnExistingFile() throws Exception {
        Path file = directory.resolve("existing.parquet");
        byte[] existing = {1, 2, 3};
        Files.write(file, existing);
        assertThrows(IOException.class, () -> new ParquetOutput().write(file, Collections.emptyList()));
        assertArrayEquals(existing, Files.readAllBytes(file));
    }

    static Map<String, Object> requiredValues(String dag) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("dagId", dag);
        values.put("applicationId", "application_1_0001");
        return values;
    }

    static ParquetReader<GenericRecord> reader(Path file) throws IOException {
        return AvroParquetReader.<GenericRecord>builder(new LocalInputFile(file)).build();
    }
}
