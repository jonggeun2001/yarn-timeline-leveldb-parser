package io.github.timelineparser.input;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.*;
import io.github.timelineparser.compat.FstValueDecoder;
import io.github.timelineparser.compat.RollingKeyDecoder;
import io.github.timelineparser.compat.RollingKeyDecoder.Header;
import io.github.timelineparser.compat.RollingKeyDecoder.Cursor;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.*;
public final class LevelDbScanner {
    @FunctionalInterface public interface EntityConsumer { void accept(TimelineEntity entity) throws IOException; }
    private static final List<String> TYPES = Arrays.asList("TEZ_DAG_ID", "TEZ_DAG_EXTRA_INFO", "TEZ_APPLICATION_ATTEMPT", "YARN_APPLICATION");
    // Keep aligned with DagCollector's scalar metadata and structured inputs.
    // Application/attempt configurations and unrelated payloads are not decoded.
    private static final Set<String> OTHER_INFO_FIELDS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "applicationId", "user", "queueName", "callerId", "callerType", "status",
            "startTime", "endTime", "numCompletedTasks", "numFailedTaskAttempts", "dagPlan", "counters")));
    private final RollingKeyDecoder keys = new RollingKeyDecoder();
    private final FstValueDecoder values = new FstValueDecoder();
    private long entries;
    private long bytes;
    public long getEntries() { return entries; }
    public long getBytes() { return bytes; }

    public void scan(Path database, EntityConsumer consumer) throws IOException {
        Options options = new Options().createIfMissing(false).paranoidChecks(true)
                .cacheSize(8 * 1024 * 1024).maxOpenFiles(128);
        try (DB db = JniDBFactory.factory.open(database.toFile(), options)) {
            rejectMonolithicLayout(db);
            for (String type : TYPES) {
                byte[] prefix = (type + "\0").getBytes(StandardCharsets.UTF_8);
                try (DBIterator it = db.iterator(new ReadOptions().verifyChecksums(true).fillCache(false))) {
                    it.seek(prefix);
                    Header previous = null;
                    TimelineEntity entity = null;
                    while (it.hasNext()) {
                        Map.Entry<byte[],byte[]> entry = it.next();
                        if (!startsWith(entry.getKey(), prefix)) break;
                        entries++; bytes += entry.getKey().length + entry.getValue().length;
                        Header header = keys.decode(entry.getKey());
                        if (!header.sameEntity(previous)) {
                            if (entity != null) consumer.accept(entity);
                            entity = new TimelineEntity();
                            entity.setEntityType(header.type); entity.setEntityId(header.id); entity.setStartTime(header.startTime);
                            previous = header;
                        }
                        try { apply(entity, header, entry.getKey(), entry.getValue()); }
                        catch (IOException | RuntimeException e) {
                            throw new IOException("Cannot decode " + type + "/" + header.id + " in " + database, e);
                        }
                    }
                    if (entity != null) consumer.accept(entity);
                }
            }
        } catch (DBException | LinkageError e) {
            throw new IOException("Cannot read LevelDB " + database + ": " + e.getMessage(), e);
        }
    }

    private void rejectMonolithicLayout(DB db) throws IOException {
        try (DBIterator iterator = db.iterator()) {
            for (String type : TYPES) {
                byte[] prefix = ("e" + type + "\0").getBytes(StandardCharsets.UTF_8);
                iterator.seek(prefix);
                if (iterator.hasNext() && startsWith(iterator.next().getKey(), prefix)) {
                    throw new IOException("Monolithic LeveldbTimelineStore layout is not supported; expected rolling entity DB");
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void apply(TimelineEntity entity, Header header, byte[] key, byte[] value) throws IOException {
        if (key.length == header.prefixLength) return;
        int column = key[header.prefixLength] & 255;
        Cursor cursor = new Cursor(key, header.prefixLength + 1);
        switch (column) {
            case 'i':
                String infoName = cursor.remainingString();
                if (OTHER_INFO_FIELDS.contains(infoName)) entity.addOtherInfo(infoName, values.decode(value));
                break;
            case 'f':
                String name = cursor.delimitedString();
                entity.addPrimaryFilter(name, values.decode(cursor.remainingBytes())); break;
            case 'r': entity.addRelatedEntity(cursor.delimitedString(), cursor.remainingString()); break;
            case 'e':
                TimelineEvent event = new TimelineEvent();
                event.setTimestamp(cursor.reverseLong()); event.setEventType(cursor.remainingString());
                Object info = values.decode(value);
                if (info != null && !(info instanceof Map)) throw new IOException("Event info is not a map");
                event.setEventInfo((Map<String,Object>) info); entity.addEvent(event); break;
            case 'd': entity.setDomainId(RollingKeyDecoder.text(value, 0, value.length)); break;
            default: throw new IOException("Unsupported rolling entity column: " + column);
        }
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) if (key[i] != prefix[i]) return false;
        return true;
    }
}
