package io.github.timelineparser.input;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public final class LevelDbCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(LevelDbCatalog.class);

    public List<Path> discover(Path input) throws IOException {
        return discover(input, null, false);
    }

    public List<Path> discover(Path input, Consumer<String> onFailure) throws IOException {
        return discover(input, onFailure, true);
    }

    private List<Path> discover(Path input, Consumer<String> onFailure, boolean recover) throws IOException {
        Path root;
        try {
            if (input == null) throw new IOException("Input directory is required");
            if (Files.isSymbolicLink(input)) throw new IOException("Input symlinks are not supported: " + input);
            root = input.toRealPath();
            if (!Files.isDirectory(root)) throw new IOException("Input must be a LevelDB directory: " + input);
        } catch (IOException | SecurityException exception) {
            IOException failure = asIOException(input, exception);
            if (recover) reportFailure(input, failure, onFailure);
            throw failure;
        }
        List<Path> databases = new ArrayList<>();
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                String name = dir.getFileName() == null ? "" : dir.getFileName().toString();
                if (name.startsWith("indexes-ldb") || name.equals("starttime-ldb")
                        || name.equals("domain-ldb") || name.equals("owner-ldb")) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                try {
                    if (name.startsWith("entity-ldb") || Files.exists(dir.resolve("CURRENT"), LinkOption.NOFOLLOW_LINKS)) {
                        validate(dir);
                        databases.add(dir);
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                } catch (IOException | SecurityException exception) {
                    failed(dir, asIOException(dir, exception));
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }
            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (attrs.isSymbolicLink()) failed(file, new IOException("Input symlinks are not supported: " + file));
                return FileVisitResult.CONTINUE;
            }
            @Override public FileVisitResult visitFileFailed(Path file, IOException exception) throws IOException {
                failed(file, exception);
                return FileVisitResult.CONTINUE;
            }
            @Override public FileVisitResult postVisitDirectory(Path dir, IOException exception) throws IOException {
                if (exception != null) failed(dir, exception);
                return FileVisitResult.CONTINUE;
            }
            private void failed(Path path, IOException exception) throws IOException {
                if (!recover) throw exception;
                reportFailure(path, exception, onFailure);
            }
        });
        if (databases.isEmpty()) throw new IOException("No complete entity LevelDB directories found in " + input);
        databases.sort(Comparator.comparing(Path::toString));
        return Collections.unmodifiableList(databases);
    }

    private static IOException asIOException(Path path, Exception exception) {
        return exception instanceof IOException ? (IOException) exception
                : new IOException("Cannot access LevelDB input: " + path, exception);
    }

    private static void reportFailure(Path path, IOException exception, Consumer<String> onFailure) {
        LOG.warn("Skipping LevelDB input " + path, exception);
        if (onFailure == null) return;
        try { onFailure.accept("path=" + path + " cause=" + exception); }
        catch (RuntimeException callbackFailure) {
            LOG.warn("LevelDB discovery failure callback failed for " + path, callbackFailure);
        }
    }

    public static void validate(Path dir) throws IOException {
        if (dir == null || !Files.isDirectory(dir, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Missing or invalid LevelDB directory: " + dir);
        }
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
