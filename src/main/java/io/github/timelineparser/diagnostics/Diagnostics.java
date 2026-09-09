package io.github.timelineparser.diagnostics;

/** Formats untrusted input values for diagnostic messages; logging is handled by SLF4J. */
public final class Diagnostics {
    private static final int MAX_LINE_LENGTH = 4096;

    private Diagnostics() { }

    public static String describe(Throwable exception) {
        StringBuilder message = new StringBuilder();
        Throwable current = exception;
        for (int depth = 0; current != null && depth < 8; depth++) {
            if (depth > 0) message.append(" <- ");
            message.append(current.getClass().getSimpleName()).append(": ")
                    .append(singleLine(current.getMessage()));
            if (current.getSuppressed().length > 0)
                message.append(" [suppressed=").append(current.getSuppressed().length).append(']');
            current = current.getCause();
        }
        return singleLine(message.toString());
    }

    public static String singleLine(String message) {
        if (message == null) return "<no message>";
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < message.length(); i++) {
            if (result.length() >= MAX_LINE_LENGTH) {
                result.append("...[truncated]");
                break;
            }
            char value = message.charAt(i);
            if (value == '\n') result.append("\\n");
            else if (value == '\r') result.append("\\r");
            else if (value == '\t') result.append("\\t");
            else if (Character.isISOControl(value)) result.append('?');
            else result.append(value);
        }
        return result.toString();
    }

}
