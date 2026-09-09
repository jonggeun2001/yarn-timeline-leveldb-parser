/* Hadoop RollingLevelDBTimelineStore FST compatibility settings, Apache-2.0. */
package io.github.timelineparser.compat;

import java.io.IOException;
import java.util.LinkedHashMap;
import org.nustaq.serialization.FSTConfiguration;

public final class FstValueDecoder {
    private final FSTConfiguration current = FSTConfiguration.createDefaultConfiguration();
    private final FSTConfiguration legacy = FSTConfiguration.createDefaultConfiguration();

    public FstValueDecoder() {
        current.setShareReferences(false);
        legacy.setShareReferences(false);
        legacy.getClassRegistry().registerClass(LinkedHashMap.class, 83, legacy);
    }

    public Object decode(byte[] value) throws IOException {
        if (value.length == 0) throw new IOException("Missing FST-encoded value");
        try {
            return current.getObjectInput(value).readObject();
        } catch (Exception first) {
            try { return legacy.getObjectInput(value).readObject(); }
            catch (Exception second) {
                IOException error = new IOException("Unable to decode Hadoop FST value", second);
                error.addSuppressed(first);
                throw error;
            }
        }
    }
}
