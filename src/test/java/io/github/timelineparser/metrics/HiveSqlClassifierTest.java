package io.github.timelineparser.metrics;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static io.github.timelineparser.metrics.HiveSqlClassifier.Kind.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class HiveSqlClassifierTest {
    @Test void recognizesSelectWithCommentsCtesSubqueriesAndUnions() {
        for (String sql : new String[] {
                "select 1",
                "SELECT ';', '/* INSERT INTO fake */' FROM src; /* final comment */",
                "-- INSERT INTO fake\nSELECT * FROM src; -- CREATE TABLE fake",
                "SELECT 'INSERT OVERWRITE TABLE dst', `has space` FROM `db`.`src`",
                "WITH c AS (SELECT * FROM src), d AS (SELECT * FROM c) SELECT * FROM d",
                "SELECT * FROM (SELECT * FROM src) s",
                "SELECT id FROM src UNION ALL SELECT id FROM other_src",
                "FROM src SELECT *",
                "SELECT * FROM src WHERE id IN (SELECT id FROM other_src)" }) {
            assertEquals(SELECT, HiveSqlClassifier.classify(sql), sql);
        }
    }

    @Test void recognizesCtasWithHiveStorageAndLocationOptions() {
        for (String sql : new String[] {
                "CREATE TABLE dst AS SELECT * FROM src",
                "CREATE TABLE IF NOT EXISTS dst STORED AS ORC LOCATION '/tmp/dst' AS SELECT * FROM src",
                "CREATE TEMPORARY TABLE dst STORED AS PARQUET AS SELECT * FROM src",
                "CREATE TABLE dst TBLPROPERTIES ('comment'='INSERT INTO x') AS SELECT * FROM src",
                "CREATE TABLE dst AS WITH c AS (SELECT * FROM src) SELECT * FROM c",
                "CREATE TABLE dst AS SELECT id FROM src UNION ALL SELECT id FROM other_src" }) {
            assertEquals(CTAS, HiveSqlClassifier.classify(sql), sql);
        }
    }

    @Test void recognizesSingleDestinationInsertInto() {
        for (String sql : new String[] {
                "INSERT INTO TABLE dst SELECT * FROM src",
                "INSERT INTO dst PARTITION (ds='2026-09-09') SELECT * FROM src",
                "WITH c AS (SELECT * FROM src) INSERT INTO TABLE dst SELECT * FROM c",
                "FROM src INSERT INTO TABLE dst SELECT *",
                "INSERT INTO dst SELECT id FROM src UNION ALL SELECT id FROM other_src" }) {
            assertEquals(INSERT_INTO, HiveSqlClassifier.classify(sql), sql);
        }
    }

    @Test void recognizesSingleDestinationInsertOverwrite() {
        for (String sql : new String[] {
                "INSERT OVERWRITE TABLE dst SELECT * FROM src",
                "INSERT OVERWRITE TABLE dst PARTITION (ds='2026-09-09', region) SELECT * FROM src",
                "WITH c AS (SELECT * FROM src) INSERT OVERWRITE TABLE dst SELECT * FROM c",
                "FROM src INSERT OVERWRITE TABLE dst SELECT *" }) {
            assertEquals(INSERT_OVERWRITE, HiveSqlClassifier.classify(sql), sql);
        }
    }

    @Test void recognizesDirectoryOutputs() {
        for (String sql : new String[] {
                "INSERT OVERWRITE DIRECTORY '/tmp/result' SELECT * FROM src",
                "INSERT OVERWRITE LOCAL DIRECTORY '/tmp/result' SELECT * FROM src",
                "INSERT OVERWRITE DIRECTORY '/tmp/result' STORED AS ORC SELECT * FROM src",
                "FROM src INSERT OVERWRITE DIRECTORY '/tmp/result' SELECT *" }) {
            assertEquals(INSERT_DIRECTORY, HiveSqlClassifier.classify(sql), sql);
        }
    }

    @Test void rejectsMultipleOutputsAndMultipleStatements() {
        for (String sql : new String[] {
                "FROM src INSERT OVERWRITE TABLE dst1 SELECT a INSERT INTO TABLE dst2 SELECT b",
                "WITH c AS (SELECT * FROM src) FROM c INSERT INTO dst1 SELECT a INSERT INTO dst2 SELECT b",
                "FROM src INSERT OVERWRITE DIRECTORY '/tmp/a' SELECT a INSERT OVERWRITE DIRECTORY '/tmp/b' SELECT b",
                "SELECT * FROM src; DROP TABLE src",
                "SELECT * FROM src; SELECT * FROM src" }) {
            assertEquals(UNSUPPORTED, HiveSqlClassifier.classify(sql), sql);
        }
    }

    @Test void rejectsOtherStatementTypesAndMissingSql() {
        for (String sql : new String[] { null, "", "  ", "-- comment only", "/* comment only */",
                "EXPLAIN SELECT * FROM src", "EXPLAIN ANALYZE SELECT * FROM src",
                "CREATE TABLE dst (id int)", "CREATE TABLE dst LIKE src",
                "CREATE VIEW dst AS SELECT * FROM src", "DROP TABLE src",
                "UPDATE dst SET id=2", "DELETE FROM dst WHERE id=1",
                "MERGE INTO dst t USING src s ON t.id=s.id WHEN MATCHED THEN UPDATE SET id=s.id" }) {
            assertEquals(UNSUPPORTED, HiveSqlClassifier.classify(sql), sql);
        }
    }

    @Test void invalidSqlDoesNotPrintItsContentsOrThrow() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream original = System.err;
        try (PrintStream captured = new PrintStream(bytes, true, "UTF-8")) {
            System.setErr(captured);
            for (String sql : new String[] { "SELECT # secret_value FROM src", "SELECT FROM secret_table",
                    "SELECT 'unterminated_secret", "INSERT OVERWRITE TABLE secret_table SELECT",
                    "WITH c AS (SELECT FROM secret_table) SELECT * FROM c" }) {
                assertEquals(UNSUPPORTED, HiveSqlClassifier.classify(sql), sql);
            }
        } finally {
            System.setErr(original);
        }
        assertEquals("", bytes.toString("UTF-8"));
    }

    @Test void rejectsSyntaxErrorsRecoveredByHiveParserDelegates() {
        for (String sql : new String[] {
                "SELECT a FROM src GROUP a",
                "SELECT extract(YEAR 1) FROM src",
                "SELECT sum(a) OVER(PARTITION a) FROM src",
                "SELECT * FROM src TABLESAMPLE(BUCKET 1 2 OUT OF 3)",
                "CREATE TABLE dst AS SELECT a FROM src GROUP a",
                "INSERT INTO dst SELECT a FROM src GROUP a" }) {
            assertEquals(UNSUPPORTED, HiveSqlClassifier.classify(sql), sql);
        }
    }
}
