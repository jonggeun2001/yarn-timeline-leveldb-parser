package io.github.timelineparser.fixture;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntities;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timeline.RollingLevelDBTimelineStore;

/** Creates real databases with the unmodified Hadoop 3.1.1 writer. */
public final class RollingStoreFixture {
    private RollingStoreFixture() { }

    public static Path write(Path directory, TimelineEntity... entities) throws IOException {
        Configuration conf = new Configuration(false);
        conf.set(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH, directory.toString());
        conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_TTL_ENABLE, false);
        RollingLevelDBTimelineStore store = new RollingLevelDBTimelineStore();
        try {
            store.init(conf);
            store.start();
            TimelineEntities batch = new TimelineEntities();
            for (TimelineEntity entity : entities) {
                entity.setDomainId("DEFAULT");
                batch.addEntity(entity);
            }
            TimelinePutResponse response = store.put(batch);
            if (!response.getErrors().isEmpty()) {
                throw new IOException("Fixture writer rejected entities: " + response.getErrors());
            }
        } finally {
            store.stop();
        }
        return directory.resolve("leveldb-timeline-store");
    }

    public static TimelineEntity entity(String type, String id, long start) {
        TimelineEntity entity = new TimelineEntity();
        entity.setEntityType(type);
        entity.setEntityId(id);
        entity.setStartTime(start);
        return entity;
    }
}
