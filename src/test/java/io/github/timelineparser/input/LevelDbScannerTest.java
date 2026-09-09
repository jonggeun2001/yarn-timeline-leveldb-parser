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
        try (WorkingCopy copy = WorkingCopy.create(database, temp.resolve("selective-work"))) {
            assertDoesNotThrow(() -> new LevelDbScanner().scan(copy.path(), entities::add));
        }
        assertEquals(2, entities.size());
        TimelineEntity restoredDag = entities.stream().filter(e -> "TEZ_DAG_ID".equals(e.getEntityType())).findFirst().get();
        assertEquals(wanted, restoredDag.getOtherInfo());
        TimelineEntity restoredAttempt = entities.stream().filter(e -> "TEZ_APPLICATION_ATTEMPT".equals(e.getEntityType())).findFirst().get();
        assertTrue(restoredAttempt.getOtherInfo().isEmpty());
    }

    @Test void malformedRequiredOtherInfoStillFailsTheScan() throws Exception {
        TimelineEntity dag = RollingStoreFixture.entity("TEZ_DAG_ID", "dag_1700000000000_0001_1", 1700000000000L);
        dag.addOtherInfo("user", "analyst");
        Path source = RollingStoreFixture.write(temp.resolve("required-source"), dag);
        Path database = new LevelDbCatalog().discover(source).get(0);
        replaceOtherInfoWithInvalidFst(database, "user", 1);
        try (WorkingCopy copy = WorkingCopy.create(database, temp.resolve("required-work"))) {
            assertThrows(java.io.IOException.class, () -> new LevelDbScanner().scan(copy.path(), e -> { }));
        }
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
