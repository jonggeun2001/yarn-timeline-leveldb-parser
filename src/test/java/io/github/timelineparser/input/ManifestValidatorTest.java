package io.github.timelineparser.input;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.impl.FileChannelLogWriter;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.LogMonitors;
import org.iq80.leveldb.impl.LogReader;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.impl.VersionEdit;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class ManifestValidatorTest {
    @TempDir Path temp;

    @Test void acceptsNativeEmptyDatabaseWithEmptyCurrentWal() throws Exception {
        Path dir = nativeDatabase("empty", false);
        Path wal = onlyFile(dir, "*.log");
        assertEquals(0, Files.size(wal));
        assertDoesNotThrow(() -> LevelDbCatalog.validate(dir));
    }

    @Test void acceptsPreallocatedManifestFromAnOpenNativeDatabase() throws Exception {
        Path dir = Files.createDirectory(temp.resolve("open-native"));
        try (DB db = JniDBFactory.factory.open(dir.toFile(), new Options().createIfMissing(true))) {
            db.put(new byte[]{1}, new byte[]{2}, new WriteOptions().sync(true));
            Path manifest = manifest(dir);
            byte[] before = Files.readAllBytes(manifest);
            assertDoesNotThrow(() -> LevelDbCatalog.validate(dir));
            try (WorkingCopy copy = WorkingCopy.create(dir, temp.resolve("work"));
                 DB reopened = JniDBFactory.factory.open(copy.path().toFile(),
                         new Options().createIfMissing(false).paranoidChecks(true))) {
                assertArrayEquals(new byte[]{2}, reopened.get(new byte[]{1}));
            }
            assertArrayEquals(before, Files.readAllBytes(manifest));
        }
    }

    @Test void rejectsDeletedCurrentWal() throws Exception {
        Path dir = nativeDatabase("missing-wal", false);
        Files.delete(onlyFile(dir, "*.log"));
        assertThrows(IOException.class, () -> LevelDbCatalog.validate(dir));
    }

    @Test void rejectsDeletedPreviousWalButAcceptsZeroSentinel() throws Exception {
        Path dir = Files.createDirectory(temp.resolve("previous-wal"));
        VersionEdit edit = metadata();
        edit.setPreviousLogNumber(7);
        writeManifest(dir, edit);
        assertThrows(IOException.class, () -> LevelDbCatalog.validate(dir));
        Files.write(dir.resolve("000007.log"), new byte[0]);
        assertDoesNotThrow(() -> LevelDbCatalog.validate(dir));
        edit.setPreviousLogNumber(0);
        writeManifest(dir, edit);
        Files.delete(dir.resolve("000007.log"));
        assertDoesNotThrow(() -> LevelDbCatalog.validate(dir));
    }

    @Test void rejectsDeletedLiveSst() throws Exception {
        Path dir = nativeDatabase("missing-table", true);
        Files.delete(tableFile(dir));
        assertThrows(IOException.class, () -> LevelDbCatalog.validate(dir));
    }

    @Test void rejectsLiveSstSizeMismatch() throws Exception {
        Path dir = nativeDatabase("table-size", true);
        Files.write(tableFile(dir), new byte[]{1}, StandardOpenOption.APPEND);
        assertThrows(IOException.class, () -> LevelDbCatalog.validate(dir));
    }

    @Test void nativeReaderStillRejectsTrailingPartialHeaders() throws Exception {
        Path dir = nativeDatabase("tail", false);
        Path manifest = manifest(dir);
        byte[] original = Files.readAllBytes(manifest);
        for (int length = 1; length <= 6; length++) {
            Files.write(manifest, original);
            byte[] tail = new byte[length];
            Arrays.fill(tail, (byte) 1);
            Files.write(manifest, tail, StandardOpenOption.APPEND);
            assertDoesNotThrow(() -> LevelDbCatalog.validate(dir), "partial header bytes=" + length);
            try (WorkingCopy copy = WorkingCopy.create(dir, temp.resolve("work"))) {
                IOException failure = assertThrows(IOException.class, () -> {
                    try (DB ignored = JniDBFactory.factory.open(copy.path().toFile(),
                            new Options().createIfMissing(false).paranoidChecks(true))) { }
                }, "native reader must reject partial header bytes=" + length);
                assertTrue(failure.getMessage().contains("truncated record"));
            }
        }
    }

    @Test void acceptsNativeFragmentedManifestWithoutChangingItsBytes() throws Exception {
        Path dir = nativeDatabase("fragmented", true);
        Path manifest = manifest(dir);
        byte[] before = Files.readAllBytes(manifest);
        boolean fragmentedRecord = false;
        try (FileChannel channel = FileChannel.open(manifest, StandardOpenOption.READ)) {
            LogReader reader = new LogReader(channel, LogMonitors.throwExceptionMonitor(), true, 0);
            Slice record;
            while ((record = reader.readRecord()) != null) fragmentedRecord |= record.length() > 32768;
        }
        assertTrue(fragmentedRecord, "Native writer must produce a logical record spanning physical blocks");
        assertDoesNotThrow(() -> LevelDbCatalog.validate(dir));
        assertArrayEquals(before, Files.readAllBytes(manifest));
    }

    @Test void rejectsChecksumCorruptionAndTruncatedPayload() throws Exception {
        Path dir = nativeDatabase("corrupt", false);
        Path manifest = manifest(dir);
        byte[] original = Files.readAllBytes(manifest);
        byte[] changed = original.clone();
        changed[7] ^= 1;
        Files.write(manifest, changed);
        assertThrows(IOException.class, () -> LevelDbCatalog.validate(dir));
        Files.write(manifest, Arrays.copyOf(original, original.length - 1));
        assertThrows(IOException.class, () -> LevelDbCatalog.validate(dir));
    }

    @Test void rejectsMissingAndNegativeRequiredMetadata() throws Exception {
        Path dir = Files.createDirectory(temp.resolve("metadata"));
        for (int field = 0; field < 3; field++) {
            VersionEdit missing = new VersionEdit();
            missing.setComparatorName("leveldb.BytewiseComparator");
            if (field != 0) missing.setLogNumber(0);
            if (field != 1) missing.setNextFileNumber(2);
            if (field != 2) missing.setLastSequenceNumber(0);
            writeManifest(dir, missing);
            assertThrows(IOException.class, () -> LevelDbCatalog.validate(dir), "missing metadata " + field);
        }
        for (int field = 0; field < 4; field++) {
            VersionEdit negative = metadata();
            if (field == 0) negative.setLogNumber(-1);
            if (field == 1) negative.setNextFileNumber(-1);
            if (field == 2) negative.setLastSequenceNumber(-1);
            if (field == 3) negative.setPreviousLogNumber(-1);
            writeManifest(dir, negative);
            assertThrows(IOException.class, () -> LevelDbCatalog.validate(dir), "negative metadata " + field);
        }
    }

    @Test void tracksSstDeletionAndLevelMoveAcrossEdits() throws Exception {
        Path dir = Files.createDirectory(temp.resolve("edits"));
        InternalKey key = new InternalKey(Slices.wrappedBuffer(new byte[]{1}), 1, ValueType.VALUE);
        VersionEdit initial = metadata();
        initial.addFile(0, 11, 3, key, key);
        initial.addFile(0, 12, 4, key, key);
        VersionEdit moved = new VersionEdit();
        moved.deleteFile(0, 11);
        moved.addFile(1, 11, 3, key, key);
        moved.deleteFile(0, 12);
        writeManifest(dir, initial, moved);
        Files.write(dir.resolve("000011.ldb"), new byte[3]);
        assertDoesNotThrow(() -> LevelDbCatalog.validate(dir));
        Files.delete(dir.resolve("000011.ldb"));
        assertThrows(IOException.class, () -> LevelDbCatalog.validate(dir));
    }

    private Path nativeDatabase(String name, boolean table) throws Exception {
        Path dir = Files.createDirectory(temp.resolve(name));
        try (DB db = JniDBFactory.factory.open(dir.toFile(), new Options().createIfMissing(true))) {
            if (table) {
                byte[] key = new byte[40000];
                Arrays.fill(key, (byte) 'k');
                db.put(key, new byte[]{1, 2, 3});
                db.compactRange(null, null);
            }
        }
        return dir;
    }

    private static Path onlyFile(Path directory, String glob) throws IOException {
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(directory, glob)) {
            java.util.Iterator<Path> it = paths.iterator();
            assertTrue(it.hasNext(), "Missing fixture file " + glob);
            Path result = it.next();
            assertFalse(it.hasNext(), "Expected one fixture file " + glob);
            return result;
        }
    }

    private static Path tableFile(Path directory) throws IOException {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(directory)) {
            for (Path path : files) {
                String name = path.getFileName().toString();
                if (name.endsWith(".ldb") || name.endsWith(".sst")) return path;
            }
        }
        throw new IOException("Native fixture did not produce a table");
    }

    private static Path manifest(Path directory) throws IOException {
        String current = new String(Files.readAllBytes(directory.resolve("CURRENT")), StandardCharsets.US_ASCII).trim();
        return directory.resolve(current);
    }

    private static VersionEdit metadata() {
        VersionEdit edit = new VersionEdit();
        edit.setComparatorName("leveldb.BytewiseComparator");
        edit.setLogNumber(0);
        edit.setNextFileNumber(20);
        edit.setLastSequenceNumber(0);
        return edit;
    }

    private static void writeManifest(Path directory, VersionEdit... edits) throws IOException {
        Path file = directory.resolve("MANIFEST-000001");
        Files.deleteIfExists(file);
        FileChannelLogWriter writer = new FileChannelLogWriter(file.toFile(), 1);
        try {
            for (VersionEdit edit : edits) writer.addRecord(edit.encode(), false);
        } finally {
            writer.close();
        }
        Files.write(directory.resolve("CURRENT"), "MANIFEST-000001\n".getBytes(StandardCharsets.US_ASCII));
    }
}
