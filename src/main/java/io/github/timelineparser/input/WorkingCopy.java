package io.github.timelineparser.input;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
public final class WorkingCopy implements AutoCloseable {
    private final Path path;
    private WorkingCopy(Path path) { this.path = path; }
    public static WorkingCopy create(Path source, Path workRoot) throws IOException {
        Files.createDirectories(workRoot);
        Path container = Files.createDirectory(workRoot.resolve(".work-" + java.util.UUID.randomUUID()));
        WorkingCopy copy = new WorkingCopy(container);
        try {
            Files.write(container.resolve(".timeline-parser-work"), "timeline-parser-work-v1\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            Path destination = Files.createDirectory(container.resolve("database"));
            Files.walkFileTree(source, new SimpleFileVisitor<Path>() {
                @Override public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    Files.createDirectories(destination.resolve(source.relativize(dir)));
                    return FileVisitResult.CONTINUE;
                }
                @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (!attrs.isRegularFile() || attrs.isSymbolicLink()) {
                        throw new IOException("Only regular LevelDB files may be copied: " + file);
                    }
                    Files.copy(file, destination.resolve(source.relativize(file)));
                    BasicFileAttributes after = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                    if (attrs.size() != after.size() || !attrs.lastModifiedTime().equals(after.lastModifiedTime())) {
                        throw new IOException("Input changed while copying: " + file);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
            LevelDbCatalog.validate(destination);
            return copy;
        } catch (IOException | RuntimeException e) {
            try { copy.close(); } catch (IOException cleanup) { e.addSuppressed(cleanup); }
            throw e;
        }
    }
    public Path path() { return path.resolve("database"); }
    @Override public void close() throws IOException {
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return;
        Path marker = path.resolve(".timeline-parser-work");
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                // Keep ownership recognizable until every copied file has been removed.
                if (!file.equals(marker)) Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
            @Override public FileVisitResult postVisitDirectory(Path dir, IOException error) throws IOException {
                if (error != null) throw error;
                if (dir.equals(path)) Files.deleteIfExists(marker);
                Files.delete(dir); return FileVisitResult.CONTINUE;
            }
        });
    }
}
