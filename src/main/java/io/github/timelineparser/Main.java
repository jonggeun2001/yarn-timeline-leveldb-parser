package io.github.timelineparser;

import io.github.timelineparser.cli.ParseCommand;
import picocli.CommandLine;

public final class Main {
    private Main() { }
    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel",
                System.getProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn"));
        System.exit(new CommandLine(new ParseCommand()).execute(args));
    }
}
