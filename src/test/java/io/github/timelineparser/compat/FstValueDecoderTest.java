package io.github.timelineparser.compat;

import org.junit.jupiter.api.Test;
import org.nustaq.serialization.FSTConfiguration;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class FstValueDecoderTest {
    @Test void readsLegacyHadoopLinkedHashMapClassRegistration() throws Exception {
        FSTConfiguration legacy = FSTConfiguration.createDefaultConfiguration();
        legacy.setShareReferences(false);
        legacy.getClassRegistry().registerClass(LinkedHashMap.class, 83, legacy);
        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("user", "분석가"); expected.put("cpu", 123L);
        assertEquals(expected, new FstValueDecoder().decode(legacy.asByteArray(expected)));
    }

    @Test void rejectsMalformedValuesWithoutPrintingRawBytes() throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream previous = System.out;
        try (PrintStream replacement = new PrintStream(captured, true, "UTF-8")) {
            System.setOut(replacement);
            assertThrows(IOException.class, () -> new FstValueDecoder().decode(new byte[]{99, 88, 77, 66, 55}));
            assertThrows(IOException.class, () -> new FstValueDecoder().decode(new byte[0]));
        } finally { System.setOut(previous); }
        assertEquals("", captured.toString("UTF-8"));
    }
}
