package io.github.timelineparser.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.timelineparser.fixture.RollingStoreFixture;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import picocli.CommandLine;
import static org.junit.jupiter.api.Assertions.*;

class QueryColumnCliTest {
    @TempDir Path temp;

    @Test void originalSqlSurvivesHadoopFstAndParquetWithMissingSqlAsNull() throws Exception {
        String firstId = "dag_1700000000000_0001_1";
        String secondId = "dag_1700000000000_0001_2";
        String sql = "  -- 쿼리 원문\r\nSELECT '한글',\tname\nFROM sample WHERE id = 7;  ";
        TimelineEntity first = RollingStoreFixture.entity("TEZ_DAG_ID", firstId, 1700000000000L);
        first.addOtherInfo("startTime", 1700000000100L);
        TimelineEntity second = RollingStoreFixture.entity("TEZ_DAG_ID", secondId, 1700000000000L);
        TimelineEntity extra = RollingStoreFixture.entity("TEZ_DAG_EXTRA_INFO", firstId, 1700000000000L);
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("context", "Hive");
        info.put("description", sql);
        extra.addOtherInfo("dagPlan", Collections.singletonMap("dagInfo", new ObjectMapper().writeValueAsString(info)));
        TimelineEntity application = RollingStoreFixture.entity("YARN_APPLICATION", "application_1700000000000_0001", 1700000000000L);
        TimelineEvent finished = new TimelineEvent();
        finished.setEventType("YARN_APPLICATION_FINISHED");
        finished.setTimestamp(1700000002100L);
        application.addEvent(finished);
        Path input = RollingStoreFixture.write(temp.resolve("input"), first, second, extra, application);
        Path output = temp.resolve("output");
        StringWriter log = new StringWriter();
        int code = new CommandLine(new ParseCommand()).setOut(new PrintWriter(log)).setErr(new PrintWriter(log))
                .execute("--input", input.toString(), "--output", output.toString());
        assertEquals(0, code, log::toString);
        Map<String, GenericRecord> rows = new HashMap<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                new LocalInputFile(output.resolve("result.parquet")))
                .withConf(new PlainParquetConfiguration()).build()) {
            GenericRecord row;
            while ((row = reader.read()) != null) rows.put(row.get("dagId").toString(), row);
        }
        assertEquals(2, rows.size());
        assertEquals(sql, rows.get(firstId).get("query").toString());
        assertEquals(1700000000100L, rows.get(firstId).get("startTime"));
        assertNull(rows.get(secondId).get("query"));
        assertFalse(log.toString().contains("SELECT '한글'"), log::toString);
    }
}
