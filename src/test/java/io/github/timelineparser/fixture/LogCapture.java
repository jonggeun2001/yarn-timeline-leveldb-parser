package io.github.timelineparser.fixture;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/** Captures the existing SLF4J Simple backend's stderr output for CLI assertions. */
public final class LogCapture implements AutoCloseable {
    private final PrintStream previous = System.err;
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final PrintStream capture = new PrintStream(bytes, true);

    public LogCapture() { System.setErr(capture); }
    public String text() { return new String(bytes.toByteArray(), StandardCharsets.UTF_8); }
    @Override public void close() {
        System.setErr(previous);
        capture.close();
    }
}
