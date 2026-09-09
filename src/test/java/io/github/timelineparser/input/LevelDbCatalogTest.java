package io.github.timelineparser.input;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class LevelDbCatalogTest {
    @TempDir Path temp;

    @Test void recoveringDiscoveryReportsBrokenDatabaseAndRetainsHealthyCandidates() throws Exception {
        Path root = Files.createDirectory(temp.resolve("input"));
        Path broken = Files.createDirectory(root.resolve("entity-ldb-broken"));
        Path healthy = database(root.resolve("entity-ldb-healthy"));
        List<String> warnings = new ArrayList<>();
        assertEquals(Collections.singletonList(healthy.toRealPath()), new LevelDbCatalog().discover(root, warnings::add));
        assertEquals(1, warnings.size());
        assertTrue(warnings.get(0).contains(broken.toString()));
        assertTrue(warnings.get(0).contains("CURRENT"));
        assertThrows(IOException.class, () -> new LevelDbCatalog().discover(root));
    }

    @Test void recoveringDiscoverySkipsLinkedSubtreesWithoutFollowingThem() throws Exception {
        Path root = Files.createDirectory(temp.resolve("input"));
        Path healthy = database(root.resolve("entity-ldb-healthy"));
        Path outside = database(temp.resolve("outside"));
        Path link = Files.createSymbolicLink(root.resolve("linked-database"), outside);
        List<String> warnings = new ArrayList<>();
        assertEquals(Collections.singletonList(healthy.toRealPath()), new LevelDbCatalog().discover(root, warnings::add));
        assertEquals(1, warnings.size());
        assertTrue(warnings.get(0).contains(link.toString()));
        assertTrue(warnings.get(0).toLowerCase(java.util.Locale.ROOT).contains("symlink"));
        assertThrows(IOException.class, () -> new LevelDbCatalog().discover(root));
    }

    @Test void rejectsRootSymlinkRatherThanFollowingIt() throws Exception {
        Path outside = database(temp.resolve("outside"));
        Path link = Files.createSymbolicLink(temp.resolve("input-link"), outside);
        List<String> warnings = new ArrayList<>();
        assertThrows(IOException.class, () -> new LevelDbCatalog().discover(link, warnings::add));
        assertEquals(1, warnings.size());
        assertTrue(warnings.get(0).contains(link.toString()));
        assertThrows(IOException.class, () -> new LevelDbCatalog().discover(link));
    }

    @Test void reportsMissingInputAndNeverTreatsAllBrokenInputAsSuccessfulEmptyDiscovery() throws Exception {
        Path missing = temp.resolve("missing");
        List<String> warnings = new ArrayList<>();
        assertThrows(IOException.class, () -> new LevelDbCatalog().discover(missing, warnings::add));
        assertEquals(1, warnings.size());
        assertTrue(warnings.get(0).contains(missing.toString()));
        Path broken = Files.createDirectory(temp.resolve("entity-ldb-broken"));
        assertThrows(IOException.class, () -> new LevelDbCatalog().discover(broken, warnings::add));
        assertEquals(2, warnings.size());
        assertThrows(IOException.class, () -> new LevelDbCatalog().discover(Files.createDirectory(temp.resolve("empty")), warnings::add));
        assertThrows(IOException.class, () -> new LevelDbCatalog().discover(null));
        assertThrows(IOException.class, () -> LevelDbCatalog.validate(null));
    }

    @Test void unreadableSubtreeDoesNotHideHealthySiblingDatabase() throws Exception {
        Path root = Files.createDirectory(temp.resolve("input"));
        Path unreadable = Files.createDirectory(root.resolve("unreadable"));
        Path healthy = database(root.resolve("entity-ldb-healthy"));
        java.util.Set<java.nio.file.attribute.PosixFilePermission> permissions = Files.getPosixFilePermissions(unreadable);
        try {
            Files.setPosixFilePermissions(unreadable, Collections.emptySet());
            org.junit.jupiter.api.Assumptions.assumeFalse(Files.isReadable(unreadable), "Requires permission enforcement");
            List<String> warnings = new ArrayList<>();
            assertEquals(Collections.singletonList(healthy.toRealPath()), new LevelDbCatalog().discover(root, warnings::add));
            assertEquals(1, warnings.size());
            assertTrue(warnings.get(0).contains(unreadable.toString()));
            assertTrue(warnings.get(0).contains("AccessDeniedException"));
        } finally {
            Files.setPosixFilePermissions(unreadable, permissions);
        }
    }

    @Test void loggingFailureCannotAbortDiscoveryOfHealthyDatabase() throws Exception {
        Path root = Files.createDirectory(temp.resolve("input"));
        Files.createDirectory(root.resolve("entity-ldb-broken"));
        Path healthy = database(root.resolve("entity-ldb-healthy"));
        assertEquals(Collections.singletonList(healthy.toRealPath()), new LevelDbCatalog().discover(root, message -> {
            throw new IllegalStateException("broken logger");
        }));
    }

    private static Path database(Path path) throws Exception {
        Files.createDirectory(path);
        try (org.iq80.leveldb.DB writer = org.fusesource.leveldbjni.JniDBFactory.factory.open(
                path.toFile(), new org.iq80.leveldb.Options().createIfMissing(true))) {
            writer.put("test".getBytes("UTF-8"), "value".getBytes("UTF-8"));
        }
        return path;
    }
}
