package io.github.timelineparser.input;

import static org.junit.jupiter.api.Assertions.*;
import io.github.timelineparser.fixture.RollingStoreFixture;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.DirectoryStream;
import java.util.*;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LevelDbScannerTest {
    @TempDir Path temp;

    @Test void reconstructsHadoopWrittenBinaryFieldsAndLeavesSourceUntouched() throws Exception {
        TimelineEntity original = RollingStoreFixture.entity("TEZ_DAG_ID", "dag_1700000000000_0001_1", 1700000000000L);
        original.addOtherInfo("user", "분석사용자");
        original.addOtherInfo("startTime", 1700000000100L);
        original.addPrimaryFilter("applicationId", "application_1700000000000_0001");
        original.addRelatedEntity("TEZ_APPLICATION_ATTEMPT", "tez_appattempt_1700000000000_0001_000001");
        TimelineEvent event = new TimelineEvent();
        event.setEventType("DAG_FINISHED"); event.setTimestamp(1700000000999L);
        event.addEventInfo("status", "SUCCEEDED"); original.addEvent(event);
        Path input = RollingStoreFixture.write(temp.resolve("input"), original);
        Map<String, byte[]> before = files(input);
        List<TimelineEntity> actual = new ArrayList<>();
        List<Path> dbs = new LevelDbCatalog().discover(input);
        assertEquals(1, dbs.size());
        try (WorkingCopy copy = WorkingCopy.create(dbs.get(0), temp.resolve("work"))) {
            new LevelDbScanner().scan(copy.path(), actual::add);
        }
        assertEquals(2, actual.size()); // The Hadoop writer creates the reverse-related attempt entity.
        TimelineEntity entity = actual.stream().filter(e -> e.getEntityType().equals("TEZ_DAG_ID")).findFirst().get();
        assertEquals(original.getEntityId(), entity.getEntityId());
        assertEquals(1700000000000L, entity.getStartTime());
        assertEquals("분석사용자", entity.getOtherInfo().get("user"));
        assertEquals(1700000000100L, entity.getOtherInfo().get("startTime"));
        assertTrue(entity.getPrimaryFilters().get("applicationId").contains("application_1700000000000_0001"));
        assertEquals("DAG_FINISHED", entity.getEvents().get(0).getEventType());
        assertEquals(1700000000999L, entity.getEvents().get(0).getTimestamp());
        assertEquals("SUCCEEDED", entity.getEvents().get(0).getEventInfo().get("status"));
        TimelineEntity attempt = actual.stream().filter(e -> e.getEntityType().equals("TEZ_APPLICATION_ATTEMPT")).findFirst().get();
        assertTrue(attempt.getRelatedEntities().get("TEZ_DAG_ID").contains(original.getEntityId()));
        Map<String,byte[]> after = files(input);
        assertEquals(before.keySet(), after.keySet());
        before.forEach((name, bytes) -> assertArrayEquals(bytes, after.get(name), name));
    }

    @Test void rejectsIncompleteDatabaseRatherThanCreatingAnEmptyOne() throws Exception {
        Path broken = Files.createDirectory(temp.resolve("entity-ldb.2026-09-08"));
        Files.write(broken.resolve("CURRENT"), "MANIFEST-000001\n".getBytes("UTF-8"));
        assertThrows(java.io.IOException.class, () -> new LevelDbCatalog().discover(broken));
        assertFalse(Files.exists(broken.resolve("MANIFEST-000001")));
    }

    private static Map<String, byte[]> files(Path root) throws Exception {
        Map<String,byte[]> result = new TreeMap<>();
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            for (Path p : (Iterable<Path>) paths.filter(Files::isRegularFile)::iterator) {
                result.put(root.relativize(p).toString(), Files.readAllBytes(p));
            }
        }
        return result;
    }

    @Test void rejectsMonolithicKeysEvenWhenAnotherEntitySortsBeforeTez() throws Exception {
        Path db = Files.createDirectory(temp.resolve("monolithic"));
        try (org.iq80.leveldb.DB writer = org.fusesource.leveldbjni.JniDBFactory.factory.open(db.toFile(), new org.iq80.leveldb.Options().createIfMissing(true))) {
            writer.put("eAAA\0anything".getBytes("UTF-8"), new byte[0]);
            writer.put("eTEZ_DAG_ID\0anything".getBytes("UTF-8"), new byte[0]);
        }
        try (WorkingCopy copy = WorkingCopy.create(db, temp.resolve("work"))) {
            assertThrows(java.io.IOException.class, () -> new LevelDbScanner().scan(copy.path(), e -> { }));
        }
    }

    @Test void missingActiveWalCannotBecomeAnEmptySuccessfulDataset() throws Exception {
        Path db = Files.createDirectory(temp.resolve("entity-ldb.2023-11-14"));
        try (org.iq80.leveldb.DB writer = org.fusesource.leveldbjni.JniDBFactory.factory.open(db.toFile(), new org.iq80.leveldb.Options().createIfMissing(true))) {
            writer.put("key".getBytes("UTF-8"), "value".getBytes("UTF-8"));
        }
        try (DirectoryStream<Path> files = Files.newDirectoryStream(db, "*.log")) {
            for (Path log : files) Files.delete(log);
        }
        assertThrows(java.io.IOException.class, () -> new LevelDbCatalog().discover(db));
    }

    @Test void nativeEngineAppliesWalUpdatesAndDeletesOverCompactedTables() throws Exception {
        TimelineEntity entity = RollingStoreFixture.entity("TEZ_DAG_ID", "dag_1700000000000_0001_1", 1700000000000L);
        entity.addOtherInfo("user", "old-user"); entity.addOtherInfo("queueName", "old-queue");
        Path root = RollingStoreFixture.write(temp.resolve("source"), entity);
        Path database = new LevelDbCatalog().discover(root).get(0);
        byte[] userKey = null;
        byte[] queueKey = null;
        try (org.iq80.leveldb.DB writer = org.fusesource.leveldbjni.JniDBFactory.factory.open(database.toFile(), new org.iq80.leveldb.Options().createIfMissing(false))) {
            try (org.iq80.leveldb.DBIterator iterator = writer.iterator()) {
                for (iterator.seekToFirst(); iterator.hasNext();) {
                    byte[] key = iterator.next().getKey();
                    String suffix = new String(key, java.nio.charset.StandardCharsets.UTF_8);
                    if (suffix.endsWith("iuser")) userKey = key;
                    if (suffix.endsWith("iqueueName")) queueKey = key;
                }
            }
            assertNotNull(userKey); assertNotNull(queueKey);
            writer.compactRange(null, null);
            org.nustaq.serialization.FSTConfiguration config = org.nustaq.serialization.FSTConfiguration.createDefaultConfiguration();
            config.setShareReferences(false);
            writer.put(userKey, config.asByteArray("latest-user"));
            writer.delete(queueKey);
        }
        List<TimelineEntity> result = new ArrayList<>();
        try (WorkingCopy copy = WorkingCopy.create(database, temp.resolve("work"))) {
            new LevelDbScanner().scan(copy.path(), result::add);
        }
        assertEquals(1, result.size());
        assertEquals("latest-user", result.get(0).getOtherInfo().get("user"));
        assertFalse(result.get(0).getOtherInfo().containsKey("queueName"));
    }

    @Test void skipsUnusedOtherInfoBeforeAttemptingFstDecode() throws Exception {
        TimelineEntity dag = RollingStoreFixture.entity("TEZ_DAG_ID", "dag_1700000000000_0001_1", 1700000000000L);
        Map<String, Object> wanted = new LinkedHashMap<>();
        for (String name : Arrays.asList("applicationId", "user", "queueName", "callerId", "callerType", "status"))
            wanted.put(name, name + "-value");
        for (String name : Arrays.asList("startTime", "endTime", "numCompletedTasks", "numFailedTaskAttempts"))
            wanted.put(name, 12L);
        wanted.put("dagPlan", Collections.singletonMap("dagContext", Collections.singletonMap("callerId", "query")));
        wanted.put("counters", Collections.singletonMap("counterGroups", Collections.emptyList()));
        wanted.forEach(dag::addOtherInfo);
        dag.addOtherInfo("unusedConfiguration", "not needed");
        TimelineEntity attempt = RollingStoreFixture.entity("TEZ_APPLICATION_ATTEMPT", "tez_appattempt_1700000000000_0001_000001", 1700000000000L);
        attempt.addOtherInfo("unusedConfiguration", "not needed");
        Path source = RollingStoreFixture.write(temp.resolve("selective-source"), dag, attempt);
        Path database = new LevelDbCatalog().discover(source).get(0);
        replaceOtherInfoWithInvalidFst(database, "unusedConfiguration", 2);
        List<TimelineEntity> entities = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        LevelDbScanner scanner = new LevelDbScanner(warnings::add);
        try (WorkingCopy copy = WorkingCopy.create(database, temp.resolve("selective-work"))) {
            assertDoesNotThrow(() -> scanner.scan(copy.path(), entities::add));
        }
        assertFalse(scanner.hasDiscardedEntities());
        assertTrue(warnings.isEmpty());
        assertEquals(2, entities.size());
        TimelineEntity restoredDag = entities.stream().filter(e -> "TEZ_DAG_ID".equals(e.getEntityType())).findFirst().get();
        assertEquals(wanted, restoredDag.getOtherInfo());
        TimelineEntity restoredAttempt = entities.stream().filter(e -> "TEZ_APPLICATION_ATTEMPT".equals(e.getEntityType())).findFirst().get();
        assertTrue(restoredAttempt.getOtherInfo().isEmpty());
    }

    @Test void malformedRequiredOtherInfoQuarantinesItsEntity() throws Exception {
        TimelineEntity dag = RollingStoreFixture.entity("TEZ_DAG_ID", "dag_1700000000000_0001_1", 1700000000000L);
        dag.addOtherInfo("user", "analyst");
        Path source = RollingStoreFixture.write(temp.resolve("required-source"), dag);
        Path database = new LevelDbCatalog().discover(source).get(0);
        replaceOtherInfoWithInvalidFst(database, "user", 1);
        try (WorkingCopy copy = WorkingCopy.create(database, temp.resolve("required-work"))) {
            List<TimelineEntity> entities = new ArrayList<>();
            assertDoesNotThrow(() -> new LevelDbScanner().scan(copy.path(), entities::add));
            assertTrue(entities.isEmpty());
        }
    }

    @Test void corruptColumnsSkipWholeEntityAndContinueWithHealthyNeighbors() throws Exception {
        byte[][] suffixes = {"iuser".getBytes("UTF-8"), "zunknown".getBytes("UTF-8"),
                new byte[]{'e', 1, 2}, new byte[]{'i', (byte) 0xff},
                "rmissing-delimiter".getBytes("UTF-8"), new byte[]{'d'}};
        for (int index = 0; index < suffixes.length; index++) {
            Path database = rawDatabase("corrupt-" + index);
            try (org.iq80.leveldb.DB db = open(database)) {
                db.put(rollingKey("a", "iuser".getBytes("UTF-8")), fst("before"));
                db.put(rollingKey("b", "iapplicationId".getBytes("UTF-8")), fst("application-1"));
                db.put(rollingKey("b", suffixes[index]), index == 0
                        ? new byte[]{99, 88, 77, 66, 55} : new byte[]{(byte) 0xff});
                db.put(rollingKey("b", "iqueueName".getBytes("UTF-8")), fst("must-not-escape"));
                db.put(rollingKey("c", "iuser".getBytes("UTF-8")), fst("after"));
            }
            List<TimelineEntity> actual = new ArrayList<>();
            List<String> warnings = new ArrayList<>();
            LevelDbScanner scanner = new LevelDbScanner(warnings::add);
            assertDoesNotThrow(() -> scanner.scan(database, actual::add));
            assertTrue(scanner.hasDiscardedEntities());
            assertEquals(1, warnings.size());
            assertTrue(warnings.get(0).contains(database.toString()));
            assertTrue(warnings.get(0).contains("TEZ_DAG_ID/b"));
            assertTrue(warnings.get(0).contains("key="));
            assertEquals(Arrays.asList("a", "c"), entityIds(actual));
            assertEquals("before", actual.get(0).getOtherInfo().get("user"));
            assertEquals("after", actual.get(1).getOtherInfo().get("user"));
        }
    }

    @Test void malformedHeadersDoNotAbortOrAttachTheirValuesToNeighboringEntities() throws Exception {
        Path database = rawDatabase("bad-headers");
        try (org.iq80.leveldb.DB db = open(database)) {
            db.put("TEZ_DAG_ID\0".getBytes("UTF-8"), fst("orphan"));
            db.put(rollingKey("a", "iuser".getBytes("UTF-8")), fst("before"));
            byte[] malformed = rollingKey("b", new byte[0]);
            malformed = Arrays.copyOf(malformed, malformed.length + 512);
            Arrays.fill(malformed, malformed.length - 513, malformed.length, (byte) 0xff);
            db.put(malformed, fst("orphan"));
            db.put(rollingKey("c", "iuser".getBytes("UTF-8")), fst("after"));
        }
        List<TimelineEntity> actual = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        LevelDbScanner scanner = new LevelDbScanner(warnings::add);
        assertDoesNotThrow(() -> scanner.scan(database, actual::add));
        assertTrue(scanner.hasDiscardedEntities());
        assertEquals(2, warnings.size());
        assertTrue(warnings.get(0).contains(database.toString()));
        assertTrue(warnings.get(0).contains("key=54455a5f4441475f494400"));
        assertTrue(warnings.get(1).contains("...(length="));
        assertTrue(warnings.get(1).length() < database.toString().length() + 300);
        assertEquals(Arrays.asList("a", "c"), entityIds(actual));
        assertEquals(Collections.singletonMap("user", "after"), actual.get(1).getOtherInfo());
    }

    @Test void consumerRecordFailuresDoNotPreventLaterEntities() throws Exception {
        Path database = rawDatabase("bad-consumer-records");
        try (org.iq80.leveldb.DB db = open(database)) {
            for (String id : Arrays.asList("a", "b", "c")) db.put(rollingKey(id, new byte[0]), new byte[0]);
        }
        List<TimelineEntity> actual = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        LevelDbScanner scanner = new LevelDbScanner(warnings::add);
        assertDoesNotThrow(() -> scanner.scan(database, entity -> {
            if (entity.getEntityId().equals("a")) throw new java.io.IOException("bad record a");
            if (entity.getEntityId().equals("b")) throw new IllegalArgumentException("bad record b");
            actual.add(entity);
        }));
        assertEquals(Collections.singletonList("c"), entityIds(actual));
        assertTrue(scanner.hasDiscardedEntities());
        assertEquals(2, warnings.size());
        assertTrue(warnings.get(0).contains("bad record a"));
        assertTrue(warnings.get(1).contains("bad record b"));
    }

    @Test void failingWarningCallbackDoesNotStopHealthyEntities() throws Exception {
        Path database = rawDatabase("bad-warning-callback");
        try (org.iq80.leveldb.DB db = open(database)) {
            db.put(rollingKey("a", "iuser".getBytes("UTF-8")), new byte[0]);
            db.put(rollingKey("b", "iuser".getBytes("UTF-8")), fst("healthy"));
        }
        List<TimelineEntity> entities = new ArrayList<>();
        LevelDbScanner scanner = new LevelDbScanner(message -> { throw new IllegalStateException("broken logger"); });
        assertDoesNotThrow(() -> scanner.scan(database, entities::add));
        assertEquals(Collections.singletonList("b"), entityIds(entities));
        assertTrue(scanner.hasDiscardedEntities());
    }

    @Test void rejectsMissingScanArgumentsBeforeOpeningDatabase() throws Exception {
        Path database = rawDatabase("null-consumer");
        assertThrows(java.io.IOException.class, () -> new LevelDbScanner().scan(null, entity -> { }));
        assertThrows(java.io.IOException.class, () -> new LevelDbScanner().scan(database, null));
    }

    private Path rawDatabase(String name) throws Exception {
        Path database = Files.createDirectory(temp.resolve(name));
        try (org.iq80.leveldb.DB ignored = org.fusesource.leveldbjni.JniDBFactory.factory.open(
                database.toFile(), new org.iq80.leveldb.Options().createIfMissing(true))) { }
        return database;
    }

    private static org.iq80.leveldb.DB open(Path database) throws Exception {
        return org.fusesource.leveldbjni.JniDBFactory.factory.open(
                database.toFile(), new org.iq80.leveldb.Options().createIfMissing(false));
    }

    private static byte[] rollingKey(String id, byte[] suffix) throws Exception {
        java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
        java.io.DataOutputStream key = new java.io.DataOutputStream(buffer);
        key.write("TEZ_DAG_ID\0".getBytes("UTF-8"));
        key.writeLong(1700000000000L ^ Long.MAX_VALUE);
        key.write(id.getBytes("UTF-8")); key.writeByte(0); key.write(suffix);
        return buffer.toByteArray();
    }

    private static byte[] fst(Object value) {
        org.nustaq.serialization.FSTConfiguration config = org.nustaq.serialization.FSTConfiguration.createDefaultConfiguration();
        config.setShareReferences(false);
        return config.asByteArray(value);
    }

    private static List<String> entityIds(List<TimelineEntity> entities) {
        List<String> ids = new ArrayList<>();
        for (TimelineEntity entity : entities) ids.add(entity.getEntityId());
        return ids;
    }

    private static void replaceOtherInfoWithInvalidFst(Path database, String name, int expected) throws Exception {
        try (org.iq80.leveldb.DB db = org.fusesource.leveldbjni.JniDBFactory.factory.open(database.toFile(), new org.iq80.leveldb.Options().createIfMissing(false))) {
            List<byte[]> keys = new ArrayList<>();
            try (org.iq80.leveldb.DBIterator iterator = db.iterator()) {
                for (iterator.seekToFirst(); iterator.hasNext();) {
                    byte[] key = iterator.next().getKey();
                    if (new String(key, java.nio.charset.StandardCharsets.UTF_8).endsWith("i" + name)) keys.add(key);
                }
            }
            assertEquals(expected, keys.size());
            for (byte[] key : keys) db.put(key, new byte[0]);
        }
    }
}
