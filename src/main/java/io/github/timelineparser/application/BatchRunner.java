package io.github.timelineparser.application;

import io.github.timelineparser.domain.DagRecord;
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

/** Recomputes the selected snapshot and publishes only after every database succeeds. */
public final class BatchRunner {
    public void run(Path input, Path output, Set<String> completedApplications,
                    ResultRowsResolver resolver, PrintWriter log) throws IOException {
        long started = System.nanoTime();
        try (OutputTransaction transaction = OutputTransaction.open(output)) {
            List<Path> databases = new LevelDbCatalog().discover(input);
            DagCollector collector = new DagCollector(resolver);
            LevelDbScanner scanner = new LevelDbScanner();
            for (int index = 0; index < databases.size(); index++) {
                Path source = databases.get(index);
                log.printf("Scanning database %d/%d: %s%n", index + 1, databases.size(), input.relativize(source));
                log.flush();
                try (WorkingCopy copy = WorkingCopy.create(source, transaction.outputDirectory())) {
                    scanner.scan(copy.path(), collector::accept);
                } catch (IOException exception) {
                    throw new IOException("Failed to process input database " + source + ": " + exception.getMessage(), exception);
                }
            }
            List<DagRecord> records = collector.finish(completedApplications);
            new ParquetOutput().write(transaction.temporaryFile(), records);
            transaction.commit();
            Map<String, Long> missing = new TreeMap<>();
            for (DagRecord record : records) record.getValues().forEach((field, value) -> {
                if (value == null) missing.merge(field, 1L, Long::sum);
            });
            long applications = records.stream().map(row -> row.get("applicationId")).distinct().count();
            Path published = transaction.outputDirectory().resolve("result.parquet");
            log.printf("Completed: databases=%d entries=%d decodedBytes=%d applications=%d collectedDags=%d excludedDags=%d rows=%d elapsedMillis=%d outputBytes=%d output=%s%n",
                    databases.size(), scanner.getEntries(), scanner.getBytes(), applications, collector.getDagCount(),
                    collector.getDagCount() - records.size(), records.size(),
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started), Files.size(published), published);
            log.printf("Null or invalid optional fields: %s%n", missing);
            log.flush();
        }
    }
}
