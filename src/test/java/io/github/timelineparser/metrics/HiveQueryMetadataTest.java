package io.github.timelineparser.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.github.timelineparser.metrics.HiveSqlClassifier.Kind.*;
import static org.junit.jupiter.api.Assertions.*;

class HiveQueryMetadataTest {
    private static final String SELECT_SQL = "SELECT * FROM source";
    private static final String CTAS_SQL = "CREATE TABLE target AS SELECT * FROM source";

    @Test void missingSqlReturnsNull() throws Exception {
        assertNull(HiveQueryMetadata.read(null));
        assertNull(HiveQueryMetadata.read(map()));
        assertNull(HiveQueryMetadata.read(info(map("context", "Hive"))));
        assertNull(HiveQueryMetadata.read(info(map("context", "Hive", "description", null))));
        assertNull(HiveQueryMetadata.read(info(map("context", "Hive", "description", "  \n"))));
        assertNull(HiveQueryMetadata.read(map("dagContext", map("context", "HIVE"))));
    }

    @Test void readsHiveDagInfoJson() throws Exception {
        assertEquals(SELECT, HiveQueryMetadata.read(info(map("context", "Hive", "description", SELECT_SQL))));
        assertEquals(CTAS, HiveQueryMetadata.read(info(map("context", "HIVE", "description", CTAS_SQL))));
    }

    @Test void ignoresDescriptionsWithoutHiveProvenance() throws Exception {
        assertNull(HiveQueryMetadata.read(info(map("context", "Pig", "description", SELECT_SQL))));
        assertNull(HiveQueryMetadata.read(info(map("description", SELECT_SQL))));
        assertNull(HiveQueryMetadata.read(map("dagContext", map("context", "Pig", "description", SELECT_SQL))));
        assertNull(HiveQueryMetadata.read(map("dagContext", map("description", SELECT_SQL))));
    }

    @Test void readsContextDescriptionFromEitherHiveProvenanceField() {
        assertEquals(SELECT, HiveQueryMetadata.read(map("dagContext",
                map("context", "HIVE", "description", SELECT_SQL))));
        assertEquals(CTAS, HiveQueryMetadata.read(map("dagContext",
                map("callerType", "HIVE_QUERY_ID", "description", CTAS_SQL))));
    }

    @Test void doesNotInterpretProtobufBlobOrOtherMetadataAsAtsSql() {
        assertNull(HiveQueryMetadata.read(map("dagContext", map("context", "HIVE", "blob", SELECT_SQL))));
        assertNull(HiveQueryMetadata.read(map("description", SELECT_SQL, "dagName", SELECT_SQL)));
    }

    @Test void fallsBackToHiveContextWhenDagInfoHasNoSql() throws Exception {
        Map<String, Object> plan = info(map("context", "Hive", "description", ""));
        plan.put("dagContext", map("context", "HIVE", "description", CTAS_SQL));
        assertEquals(CTAS, HiveQueryMetadata.read(plan));
    }

    @Test void unsupportedAndInvalidSqlAreDistinctFromMissingSql() throws Exception {
        for (String sql : new String[] { "UPDATE target SET id=1", "not valid SQL" }) {
            assertEquals(UNSUPPORTED, HiveQueryMetadata.read(info(map("context", "Hive", "description", sql))));
            assertEquals(UNSUPPORTED, HiveQueryMetadata.read(map("dagContext",
                    map("context", "HIVE", "description", sql))));
        }
    }

    @Test void rejectsNonTextHiveDescriptions() throws Exception {
        assertEquals(UNSUPPORTED, HiveQueryMetadata.read(info(map("context", "Hive", "description", 12))));
        assertEquals(UNSUPPORTED, HiveQueryMetadata.read(map("dagContext", map("context", "HIVE", "description", map()))));
    }

    @Test void rejectsDuplicateJsonFields() {
        assertEquals(UNSUPPORTED, HiveQueryMetadata.read(map("dagInfo",
                "{\"context\":\"Hive\",\"description\":\"SELECT 1\",\"description\":\"SELECT 2\"}")));
        assertEquals(UNSUPPORTED, HiveQueryMetadata.read(map("dagInfo",
                "{\"context\":\"Hive\",\"context\":\"Pig\",\"description\":\"SELECT 1\"}")));
    }

    @Test void rejectsTrailingJsonValuesAndJunk() {
        String json = "{\"context\":\"Hive\",\"description\":\"SELECT 1\"}";
        for (String suffix : new String[] { " {}", " null", " junk" })
            assertEquals(UNSUPPORTED, HiveQueryMetadata.read(map("dagInfo", json + suffix)));
    }

    @Test void rejectsMalformedJsonAndWrongRootShape() {
        for (String json : new String[] { "{broken", "[]", "\"SELECT 1\"", "null" })
            assertEquals(UNSUPPORTED, HiveQueryMetadata.read(map("dagInfo", json)));
    }

    @Test void acceptsTrailingWhitespace() {
        assertEquals(SELECT, HiveQueryMetadata.read(map("dagInfo",
                "{\"context\":\"Hive\",\"description\":\"SELECT 1\"} \n\t")));
    }

    @Test void acceptsMatchingKindsEvenWhenQueryTextDiffers() throws Exception {
        Map<String, Object> plan = info(map("context", "Hive", "description", "SELECT 'secret' FROM source"));
        plan.put("dagContext", map("context", "HIVE", "description", "SELECT 'redacted' FROM source"));
        assertEquals(SELECT, HiveQueryMetadata.read(plan));
    }

    @Test void rejectsConflictingValidKinds() throws Exception {
        Map<String, Object> plan = info(map("context", "Hive", "description", SELECT_SQL));
        plan.put("dagContext", map("context", "HIVE", "description", CTAS_SQL));
        assertEquals(UNSUPPORTED, HiveQueryMetadata.read(plan));
    }

    @Test void keepsValidRawSqlWhenContextWasRedactedToInvalidSql() throws Exception {
        Map<String, Object> plan = info(map("context", "Hive", "description", SELECT_SQL));
        plan.put("dagContext", map("context", "HIVE", "description", "[REDACTED]"));
        assertEquals(SELECT, HiveQueryMetadata.read(plan));
    }

    @Test void doesNotReplaceUnsupportedRawSqlWithContextSql() throws Exception {
        Map<String, Object> plan = info(map("context", "Hive", "description", "UPDATE target SET id=1"));
        plan.put("dagContext", map("context", "HIVE", "description", SELECT_SQL));
        assertEquals(UNSUPPORTED, HiveQueryMetadata.read(plan));
    }

    @Test void malformedRawJsonDoesNotFallBackToContextSql() {
        Map<String, Object> plan = map("dagInfo", "{broken");
        plan.put("dagContext", map("context", "HIVE", "description", SELECT_SQL));
        assertEquals(UNSUPPORTED, HiveQueryMetadata.read(plan));
    }

    private static Map<String, Object> info(Map<String, Object> value) throws Exception {
        return map("dagInfo", new ObjectMapper().writeValueAsString(value));
    }

    private static Map<String, Object> map(Object... pairs) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) values.put((String) pairs[i], pairs[i + 1]);
        return values;
    }
}
