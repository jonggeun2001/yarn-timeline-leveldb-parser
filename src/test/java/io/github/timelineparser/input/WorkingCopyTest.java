package io.github.timelineparser.input;

import io.github.timelineparser.output.OutputTransaction;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

class WorkingCopyTest {
    @TempDir Path temp;

    @Test
    void abandonedCopyRemainsRecognizableAfterCleanupFails() throws Exception {
        assumeTrue(temp.getFileSystem().supportedFileAttributeViews().contains("posix"));
        Path source = Files.createDirectory(temp.resolve("source"));
        try (DB ignored = JniDBFactory.factory.open(source.toFile(), new Options().createIfMissing(true))) {
            // A native empty database still supplies a complete CURRENT, MANIFEST, and WAL.
        }
        Path output = temp.resolve("output");
        WorkingCopy copy = WorkingCopy.create(source, output);
        Path container = copy.path().getParent();
        Path blocked = Files.createDirectory(container.resolve("unremovable"));
        Files.write(blocked.resolve("remaining-data"), new byte[]{1});
        Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(blocked);
        try {
            List<Path> entries = new ArrayList<>();
            try (DirectoryStream<Path> children = Files.newDirectoryStream(container)) {
                children.forEach(entries::add);
            }
            // The regression needs the marker to be visited before the directory that fails.
            assumeTrue(entries.indexOf(container.resolve(".timeline-parser-work")) < entries.indexOf(blocked),
                    "This filesystem visits the blocked directory before the ownership marker");
            Files.setPosixFilePermissions(blocked, EnumSet.of(
                    PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE));
            try {
                assumeFalse(Files.isWritable(blocked), "The current user bypasses directory permissions");
                assertThrows(IOException.class, copy::close);
            } finally {
                Files.setPosixFilePermissions(blocked, permissions);
            }
            try (OutputTransaction ignored = OutputTransaction.open(output)) {
                assertFalse(Files.exists(container), "The next run must reclaim the failed working copy");
            }
        } finally {
            if (Files.exists(blocked)) Files.setPosixFilePermissions(blocked, permissions);
            copy.close();
        }
    }
}
