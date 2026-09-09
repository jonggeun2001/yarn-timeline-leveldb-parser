package io.github.timelineparser.output;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Objects;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/** Owns one output lock and temporary file until the complete replacement is committed. */
public final class OutputTransaction implements AutoCloseable {
    // Some POSIX systems release all JVM locks on a file when any channel to it is closed.
    // Reserve a canonical directory before opening a second channel in this JVM.
    private static final Set<Path> ACTIVE_OUTPUTS = Collections.newSetFromMap(new ConcurrentHashMap<Path, Boolean>());
    private static final String UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
    private static final Pattern TEMPORARY_NAME = Pattern.compile("\\.result-" + UUID_PATTERN + "\\.tmp");
    private static final Pattern WORK_NAME = Pattern.compile("\\.work-" + UUID_PATTERN);
    private static final byte[] WORK_MARKER = "timeline-parser-work-v1\n".getBytes(StandardCharsets.UTF_8);
    private final Path output;
    private final Path temporary;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private boolean committed;
    private boolean closed;

    private OutputTransaction(Path output, FileChannel lockChannel, FileLock lock) {
        this.output = output;
        this.temporary = output.resolve(".result-" + UUID.randomUUID() + ".tmp");
        this.lockChannel = lockChannel;
        this.lock = lock;
    }

    public static OutputTransaction open(Path output) throws IOException {
        Objects.requireNonNull(output, "output");
        Files.createDirectories(output);
        Path realOutput = output.toRealPath();
        if (!ACTIVE_OUTPUTS.add(realOutput)) {
            throw new LockUnavailableException(realOutput, null);
        }
        FileChannel channel = null;
        try {
            channel = FileChannel.open(realOutput.resolve(".timeline-parser.lock"),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            FileLock lock;
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException exception) {
                throw new LockUnavailableException(realOutput, exception);
            }
            if (lock == null) {
                throw new LockUnavailableException(realOutput, null);
            }
            cleanOrphans(realOutput);
            return new OutputTransaction(realOutput, channel, lock);
        } catch (IOException | RuntimeException | Error exception) {
            try {
                if (channel != null) channel.close();
            } catch (IOException closeFailure) {
                exception.addSuppressed(closeFailure);
            } finally {
                ACTIVE_OUTPUTS.remove(realOutput);
            }
            throw exception;
        }
    }

    public Path temporaryFile() {
        return temporary;
    }

    private static void cleanOrphans(Path output) throws IOException {
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(output)) {
            for (Path entry : entries) {
                String name = entry.getFileName().toString();
                if (TEMPORARY_NAME.matcher(name).matches()
                        && Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)) {
                    Files.deleteIfExists(entry);
                } else if (WORK_NAME.matcher(name).matches()
                        && Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS)) {
                    Path marker = entry.resolve(".timeline-parser-work");
                    if (Files.isRegularFile(marker, LinkOption.NOFOLLOW_LINKS)
                            && Files.size(marker) == WORK_MARKER.length
                            && Arrays.equals(WORK_MARKER, Files.readAllBytes(marker))) {
                        deleteOwnedWork(entry, marker);
                    }
                }
            }
        }
    }

    private static void deleteOwnedWork(Path work, Path marker) throws IOException {
        // walkFileTree does not follow symbolic links. Keep the marker until all data is
        // removed so an interrupted cleanup remains recognizable on the next run.
        Files.walkFileTree(work, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                if (!file.equals(marker)) Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path directory, IOException error) throws IOException {
                if (error != null) throw error;
                if (directory.equals(work)) Files.delete(marker);
                Files.delete(directory);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public Path outputDirectory() {
        return output;
    }

    public void commit() throws IOException {
        if (closed || committed || !lock.isValid()) {
            throw new IOException("Output transaction is closed, committed, or no longer locked: " + output);
        }
        ParquetOutput.validate(temporary);
        // Both files are in this directory. Unsupported atomic replacement is a hard failure;
        // never delete the existing result or fall back to a non-atomic copy.
        Files.move(temporary, output.resolve("result.parquet"),
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        committed = true;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException failure = null;
        try {
            Files.deleteIfExists(temporary);
        } catch (IOException | RuntimeException exception) {
            failure = cleanupFailure(exception);
        }
        try {
            lock.release();
        } catch (IOException | RuntimeException exception) {
            if (failure == null) failure = cleanupFailure(exception);
            else failure.addSuppressed(exception);
        }
        try {
            lockChannel.close();
        } catch (IOException | RuntimeException exception) {
            if (failure == null) failure = cleanupFailure(exception);
            else failure.addSuppressed(exception);
        } finally {
            ACTIVE_OUTPUTS.remove(output);
        }
        // Keep the lock file: deleting it lets another process lock a different inode.
        if (failure != null) {
            throw failure;
        }
    }

    private IOException cleanupFailure(Exception exception) {
        return exception instanceof IOException ? (IOException) exception
                : new IOException("Cannot clean output transaction: " + output, exception);
    }

    public static final class LockUnavailableException extends IOException {
        private static final long serialVersionUID = 1L;

        private LockUnavailableException(Path output, Throwable cause) {
            super("Another parser is using output directory: " + output, cause);
        }
    }
}
