package io.github.timelineparser.input;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
public final class LevelDbCatalog {
    public List<Path> discover(Path input) throws IOException {
        Path root = input.toRealPath();
        if (!Files.isDirectory(root)) throw new IOException("Input must be a LevelDB directory: " + input);
        List<Path> databases = new ArrayList<>();
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                String name = dir.getFileName().toString();
                if (name.startsWith("indexes-ldb") || name.equals("starttime-ldb")
                        || name.equals("domain-ldb") || name.equals("owner-ldb")) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                if (name.startsWith("entity-ldb") || Files.exists(dir.resolve("CURRENT"), LinkOption.NOFOLLOW_LINKS)) {
                    validate(dir);
                    databases.add(dir);
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }
            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (attrs.isSymbolicLink()) throw new IOException("Input symlinks are not supported: " + file);
                return FileVisitResult.CONTINUE;
            }
        });
        if (databases.isEmpty()) throw new IOException("No complete entity LevelDB directories found in " + input);
        databases.sort(Comparator.comparing(Path::toString));
        return Collections.unmodifiableList(databases);
    }

    public static void validate(Path dir) throws IOException {
        Path current = dir.resolve("CURRENT");
        if (!Files.isRegularFile(current, LinkOption.NOFOLLOW_LINKS) || Files.size(current) > 1024) {
            throw new IOException("Missing or invalid LevelDB CURRENT: " + dir);
        }
        String manifest = new String(Files.readAllBytes(current), StandardCharsets.US_ASCII).trim();
        if (!manifest.matches("MANIFEST-[0-9]+")
                || !Files.isRegularFile(dir.resolve(manifest), LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Missing or invalid LevelDB manifest: " + dir + "/" + manifest);
        }
        ManifestValidator.validate(dir, dir.resolve(manifest));
    }
}
