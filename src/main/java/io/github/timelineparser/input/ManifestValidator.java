package io.github.timelineparser.input;

import org.iq80.leveldb.impl.FileMetaData;
import org.iq80.leveldb.impl.Filename;
import org.iq80.leveldb.impl.LogMonitors;
import org.iq80.leveldb.impl.LogReader;
import org.iq80.leveldb.impl.VersionEdit;
import org.iq80.leveldb.util.Slice;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Reads MANIFEST metadata without opening or changing the database.
 * The caller must supply a consistent, immutable copy. In particular, a WAL
 * newer than the MANIFEST may contain writes without being named in it; an
 * omitted such WAL cannot be discovered from the remaining files alone.
 */
public final class ManifestValidator {
    private ManifestValidator() { }

    public static void validate(Path directory, Path manifest) throws IOException {
        State state = new State();
        try (FileChannel channel = FileChannel.open(manifest, StandardOpenOption.READ)) {
            // Native writers may keep zero-filled preallocated space until close.
            // Let the library parse physical records and verify CRCs; re-encoding
            // logical records cannot reproduce the original file allocation.
            // The working copy is also checked by the native reader when opened.
            LogReader reader = new LogReader(channel, LogMonitors.throwExceptionMonitor(), true, 0);
            Slice record;
            while ((record = reader.readRecord()) != null) {
                state.apply(new VersionEdit(record));
            }
            state.validateFiles(directory);
        } catch (RuntimeException e) {
            throw new IOException("Invalid MANIFEST: " + manifest, e);
        }
    }

    private static final class State {
        private Long logNumber;
        private Long previousLogNumber = 0L;
        private Long nextFileNumber;
        private Long lastSequence;
        private final Map<Integer, Map<Long, Long>> tables = new HashMap<>();

        private void apply(VersionEdit edit) throws IOException {
            logNumber = update("log number", edit.getLogNumber(), logNumber);
            previousLogNumber = update("previous log number", edit.getPreviousLogNumber(), previousLogNumber);
            nextFileNumber = update("next file number", edit.getNextFileNumber(), nextFileNumber);
            lastSequence = update("last sequence", edit.getLastSequenceNumber(), lastSequence);
            String comparator = edit.getComparatorName();
            if (comparator != null && !"leveldb.BytewiseComparator".equals(comparator)) {
                throw new IOException("Unsupported LevelDB comparator: " + comparator);
            }
            for (Map.Entry<Integer, Long> deleted : edit.getDeletedFiles().entries()) {
                checkLevel(deleted.getKey());
                nonnegative("deleted table number", deleted.getValue());
                Map<Long, Long> level = tables.get(deleted.getKey());
                if (level != null) level.remove(deleted.getValue());
            }
            for (Map.Entry<Integer, FileMetaData> added : edit.getNewFiles().entries()) {
                checkLevel(added.getKey());
                FileMetaData file = added.getValue();
                nonnegative("table number", file.getNumber());
                nonnegative("table size", file.getFileSize());
                tables.computeIfAbsent(added.getKey(), key -> new HashMap<>())
                        .put(file.getNumber(), file.getFileSize());
            }
        }

        private void validateFiles(Path directory) throws IOException {
            if (logNumber == null || nextFileNumber == null || lastSequence == null) {
                throw new IOException("MANIFEST lacks required log number, next file number, or last sequence: " + directory);
            }
            requireWal(directory, logNumber);
            requireWal(directory, previousLogNumber);
            for (Map<Long, Long> level : tables.values()) {
                for (Map.Entry<Long, Long> table : level.entrySet()) {
                    String basename = String.format(Locale.ROOT, "%06d", table.getKey());
                    Path path = directory.resolve(basename + ".ldb");
                    if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) path = directory.resolve(basename + ".sst");
                    requireRegularFile(path);
                    if (Files.size(path) != table.getValue()) {
                        throw new IOException("MANIFEST table size mismatch: " + path);
                    }
                }
            }
        }

        private static void requireWal(Path directory, long number) throws IOException {
            // Zero means no required WAL. An existing required WAL may be empty.
            if (number != 0) requireRegularFile(directory.resolve(Filename.logFileName(number)));
        }

        private static Long update(String name, Long value, Long previous) throws IOException {
            if (value == null) return previous;
            nonnegative(name, value);
            return value;
        }

        private static void nonnegative(String name, long value) throws IOException {
            if (value < 0) throw new IOException("Negative MANIFEST " + name);
        }

        private static void checkLevel(int level) throws IOException {
            if (level < 0 || level >= 7) throw new IOException("Invalid MANIFEST table level: " + level);
        }

        private static void requireRegularFile(Path path) throws IOException {
            if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Missing or invalid MANIFEST-referenced file: " + path);
            }
        }
    }
}
