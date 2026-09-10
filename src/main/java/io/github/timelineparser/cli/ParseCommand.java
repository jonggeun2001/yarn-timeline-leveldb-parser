package io.github.timelineparser.cli;

import io.github.timelineparser.application.BatchRunner;
import io.github.timelineparser.BuildInfo;
import io.github.timelineparser.diagnostics.Diagnostics;
import io.github.timelineparser.metrics.ResultRowsResolver;
import io.github.timelineparser.output.OutputTransaction;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

@Command(name = "timeline-parser", mixinStandardHelpOptions = true, versionProvider = ParseCommand.VersionProvider.class,
        description = "Convert a consistent local Rolling Timeline LevelDB snapshot to result.parquet.")
public final class ParseCommand implements Callable<Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseCommand.class);
    public static final class VersionProvider implements picocli.CommandLine.IVersionProvider {
        @Override public String[] getVersion() { return new String[]{BuildInfo.version()}; }
    }

    @Option(names = "--input", required = true, description = "Local entity LevelDB directory or its parent.")
    private Path input;
    @Option(names = "--output", required = true, description = "Local directory for atomic result.parquet replacement.")
    private Path output;
    @Option(names = "--completed-applications", description = "UTF-8 file of verified completed application IDs, one per line.")
    private Path completedApplications;
    @Option(names = "--result-rows-mapping", description = "Optional JSON file overriding result-row counters and kinds by DAG ID.")
    private Path resultRowsMapping;
    @Spec private CommandSpec spec;

    @Override public Integer call() {
        Path realInput;
        Path realOutput;
        Set<String> completed;
        ResultRowsResolver resolver;
        try {
            if (input == null || output == null) throw new IOException("Both --input and --output are required");
            realInput = input.toRealPath();
            if (!Files.isDirectory(realInput)) throw new IOException("Input must be a directory: " + input);
            realOutput = resolveFuturePath(output.toAbsolutePath().normalize());
            if (realInput.startsWith(realOutput) || realOutput.startsWith(realInput))
                throw new IOException("Input and output directories must not overlap");
            if (Files.exists(realOutput) && !Files.isDirectory(realOutput))
                throw new IOException("Output must be a directory: " + output);
            completed = readCompletedApplications();
            resolver = resultRowsMapping == null ? new ResultRowsResolver() : ResultRowsResolver.fromFile(resultRowsMapping);
        } catch (IOException | RuntimeException exception) {
            return fail(2, "Invalid arguments", exception);
        }
        try {
            new BatchRunner().run(realInput, realOutput, completed, resolver, outputWriter());
            return 0;
        } catch (OutputTransaction.LockUnavailableException exception) {
            return fail(3, "Output is locked", exception);
        } catch (IOException | RuntimeException | LinkageError exception) {
            return fail(1, "Processing failed", exception);
        }
    }

    private Set<String> readCompletedApplications() throws IOException {
        if (completedApplications == null) return null;
        Set<String> result = new HashSet<>();
        try (BufferedReader reader = Files.newBufferedReader(completedApplications, StandardCharsets.UTF_8)) {
            String line;
            int number = 0;
            while ((line = reader.readLine()) != null) {
                number++;
                line = line.trim();
                if (line.isEmpty()) continue;
                if (!line.matches("application_[0-9]+_[0-9]+"))
                    throw new IOException("Invalid completed application ID at line " + number);
                try { result.add(ApplicationId.fromString(line).toString()); }
                catch (IllegalArgumentException exception) {
                    throw new IOException("Out-of-range completed application ID at line " + number, exception);
                }
            }
        }
        return result;
    }

    private static Path resolveFuturePath(Path path) throws IOException {
        if (Files.exists(path)) return path.toRealPath();
        Path parent = path.getParent();
        if (parent == null) throw new IOException("Cannot resolve output directory: " + path);
        return resolveFuturePath(parent).resolve(path.getFileName());
    }

    private int fail(int code, String message, Throwable exception) {
        LOG.error("{} input={} output={}", message, Diagnostics.singleLine(String.valueOf(input)),
                Diagnostics.singleLine(String.valueOf(output)), exception);
        PrintWriter errors = spec == null ? new PrintWriter(System.err, true) : spec.commandLine().getErr();
        try {
            errors.println(message + ": " + Diagnostics.describe(exception));
            errors.flush();
            if (errors.checkError()) LOG.warn("Unable to write CLI error summary; see application log");
        } catch (RuntimeException failure) {
            LOG.warn("Unable to write CLI error summary; see application log", failure);
        }
        return code;
    }

    private PrintWriter outputWriter() {
        return spec == null ? new PrintWriter(System.out, true) : spec.commandLine().getOut();
    }
}
