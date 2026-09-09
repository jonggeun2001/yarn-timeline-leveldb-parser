package io.github.timelineparser.output;

import io.github.timelineparser.domain.DagRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Writes the fixed DAG contract to a new local file and validates its closed footer. */
public final class ParquetOutput {
    private static final String SCHEMA_VERSION_KEY = "timeline.schema.version";
    private static final String PARSER_VERSION_KEY = "timeline.parser.version";
    private static final String MAPPING_VERSION_KEY = "timeline.mapping.version";
    private static final String ROW_COUNT_KEY = "timeline.row.count";
    private static final String SCHEMA_VERSION = "1";
    private static final String PARSER_VERSION = "1.0.0-SNAPSHOT";
    private static final String MAPPING_VERSION = "tez-0.9.1-v1";
    private static final long ROW_GROUP_BYTES = 128L * 1024 * 1024;

    public void write(Path file, List<DagRecord> rows) throws IOException {
        Objects.requireNonNull(file, "file");
        Objects.requireNonNull(rows, "rows");
        Schema schema = loadSchema();
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put(SCHEMA_VERSION_KEY, SCHEMA_VERSION);
        metadata.put(PARSER_VERSION_KEY, PARSER_VERSION);
        metadata.put(MAPPING_VERSION_KEY, MAPPING_VERSION);
        metadata.put(ROW_COUNT_KEY, Long.toString(rows.size()));
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(file))
                .withConf(new Configuration(false))
                .withSchema(schema)
                .withDataModel(GenericData.get())
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withRowGroupSize(ROW_GROUP_BYTES)
                .withExtraMetaData(metadata)
                .build()) {
            for (DagRecord row : rows) {
                GenericRecord record = new GenericData.Record(schema);
                for (Schema.Field field : schema.getFields()) {
                    record.put(field.name(), row.get(field.name()));
                }
                if (!GenericData.get().validate(schema, record)) {
                    throw new IOException("DAG record does not match output schema: " + row.get("dagId"));
                }
                writer.write(record);
            }
        } catch (RuntimeException exception) {
            throw new IOException("Cannot write Parquet file: " + file, exception);
        }
        if (validate(file) != rows.size()) {
            throw new IOException("Parquet row count differs from input: " + file);
        }
    }

    /** Used again immediately before publication so incomplete or substituted output is rejected. */
    static long validate(Path file) throws IOException {
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Parquet output is not a regular file: " + file);
        }
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            if (!new AvroSchemaConverter(new Configuration(false)).convert(loadSchema())
                    .equals(reader.getFooter().getFileMetaData().getSchema())) {
                throw new IOException("Parquet output schema does not match DAG schema: " + file);
            }
            Map<String, String> metadata = reader.getFooter().getFileMetaData().getKeyValueMetaData();
            if (!SCHEMA_VERSION.equals(metadata.get(SCHEMA_VERSION_KEY))
                    || !PARSER_VERSION.equals(metadata.get(PARSER_VERSION_KEY))
                    || !MAPPING_VERSION.equals(metadata.get(MAPPING_VERSION_KEY))) {
                throw new IOException("Missing or incompatible Parquet version metadata: " + file);
            }
            String expectedRows = metadata.get(ROW_COUNT_KEY);
            if (expectedRows == null) {
                throw new IOException("Missing expected Parquet row count: " + file);
            }
            long expected = Long.parseLong(expectedRows);
            long actual = reader.getRecordCount();
            if (expected < 0 || actual != expected) {
                throw new IOException("Invalid Parquet row count: " + file);
            }
            return actual;
        } catch (RuntimeException exception) {
            throw new IOException("Invalid Parquet output: " + file, exception);
        }
    }

    private static Schema loadSchema() throws IOException {
        try (InputStream stream = ParquetOutput.class.getResourceAsStream("/dag-metrics.avsc")) {
            if (stream == null) {
                throw new IOException("Missing output schema resource: dag-metrics.avsc");
            }
            return new Schema.Parser().parse(stream);
        }
    }
}
