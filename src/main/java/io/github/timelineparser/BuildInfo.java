package io.github.timelineparser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** Maven-filtered version shared by the CLI and Parquet metadata. */
public final class BuildInfo {
    private static final String VERSION = loadVersion();

    private BuildInfo() { }

    public static String version() {
        return VERSION;
    }

    private static String loadVersion() {
        try (InputStream input = BuildInfo.class.getResourceAsStream("/build-info.properties")) {
            if (input == null) throw new IOException("Missing build-info.properties; build with Maven");
            Properties properties = new Properties();
            properties.load(input);
            String version = properties.getProperty("version", "").trim();
            if (version.isEmpty() || version.contains("${")) {
                throw new IOException("Missing or unfiltered Maven project version");
            }
            return version;
        } catch (IOException exception) {
            throw new IllegalStateException("Cannot read build version", exception);
        }
    }
}
