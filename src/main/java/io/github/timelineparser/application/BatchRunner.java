package io.github.timelineparser.application;

import io.github.timelineparser.domain.DagRecord;
import io.github.timelineparser.diagnostics.Diagnostics;
import io.github.timelineparser.input.LevelDbCatalog;
import io.github.timelineparser.input.LevelDbScanner;
import io.github.timelineparser.input.WorkingCopy;
import io.github.timelineparser.metrics.DagCollector;
import io.github.timelineparser.metrics.ResultRowsResolver;
import io.github.timelineparser.output.OutputTransaction;
import io.github.timelineparser.output.ParquetOutput;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Recomputes the selected snapshot and publishes only after every database succeeds. */
public final class BatchRunner {
    private static final Logger LOG = LoggerFactory.getLogger(BatchRunner.class);
    public void run(Path input, Path output, Set<String> completedApplications,
                    ResultRowsResolver resolver, PrintWriter log) throws IOException {
        long started = System.nanoTime();
        if (input == null || output == null || resolver == null)
            throw new IOException("Input, output and result row resolver are required");
        boolean published = false;
        try (OutputTransaction transaction = OutputTransaction.open(output)) {
            int[] inputFailures = {0};
            List<Path> databases = new LevelDbCatalog().discover(input, failure -> {
                inputFailures[0]++;
            });
            String[] activeSource = {"all-databases"};
            Consumer<String> recordWarnings = warning -> LOG.warn("stage=collect database={} {}",
                    Diagnostics.singleLine(activeSource[0]), warning);
            DagCollector collector = new DagCollector(resolver, recordWarnings);
            LevelDbScanner scanner = new LevelDbScanner();
            for (int index = 0; index < databases.size(); index++) {
                Path source = databases.get(index);
                activeSource[0] = source.toString();
                reportProgress(log, String.format("Scanning database %d/%d: %s", index + 1, databases.size(), source));
                try (WorkingCopy copy = WorkingCopy.create(source, transaction.outputDirectory())) {
                    scanner.scan(copy.path(), collector::accept);
                } catch (IOException | RuntimeException | LinkageError exception) {
                    inputFailures[0]++;
                    LOG.warn("stage=scan database={} action=skip_database", Diagnostics.singleLine(source.toString()), exception);
                }
            }
            if (inputFailures[0] > 0)
                throw new IOException("Input failures=" + inputFailures[0] + "; incomplete snapshot, previous output preserved");
            activeSource[0] = "all-databases";
            List<DagRecord> records = collector.finish(completedApplications);
            if (records.isEmpty() && (collector.hasDiscardedEntities() || scanner.hasDiscardedEntities()))
                throw new IOException("No output rows remain after discarding invalid entities; previous output preserved");
            Map<String, Long> missing = new TreeMap<>();
            for (DagRecord record : records) record.getValues().forEach((field, value) -> {
                if (value == null) missing.merge(field, 1L, Long::sum);
            });
            long applications = records.stream().map(row -> row.get("applicationId")).distinct().count();
            Path publishedPath = transaction.outputDirectory().resolve("result.parquet");
            new ParquetOutput().write(transaction.temporaryFile(), records);
            long outputBytes = Files.size(transaction.temporaryFile());
            String summary = String.format("Completed: databases=%d entries=%d decodedBytes=%d applications=%d collectedDags=%d excludedDags=%d rows=%d elapsedMillis=%d outputBytes=%d output=%s",
                    databases.size(), scanner.getEntries(), scanner.getBytes(), applications, collector.getDagCount(),
                    collector.getDagCount() - records.size(), records.size(),
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started), outputBytes, publishedPath);
            transaction.commit();
            published = true;
            reportProgress(log, summary);
            reportProgress(log, "Null or invalid optional fields: " + missing);
        } catch (IOException | RuntimeException exception) {
            if (!published) throw exception;
            LOG.warn("stage=cleanup output={} action=keep_committed_output", Diagnostics.singleLine(output.toString()), exception);
        }
    }

    private static void reportProgress(PrintWriter output, String message) {
        String line = Diagnostics.singleLine(message);
        if (output == null) {
            LOG.info("{}", line);
            return;
        }
        try {
            output.println(line);
            output.flush();
            if (output.checkError()) LOG.warn("Unable to write CLI progress output");
        } catch (RuntimeException failure) {
            LOG.warn("Unable to write CLI progress output", failure);
        }
    }
}
