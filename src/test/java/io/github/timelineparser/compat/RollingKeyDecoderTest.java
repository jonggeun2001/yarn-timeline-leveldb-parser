package io.github.timelineparser.compat;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RollingKeyDecoderTest {
    @Test void rejectsMissingKeyAsDecodeFailure() {
        assertThrows(IOException.class, () -> new RollingKeyDecoder().decode(null));
    }

    @Test void rejectsInvalidTextRangesAsDecodeFailures() {
        assertThrows(IOException.class, () -> RollingKeyDecoder.text(null, 0, 0));
        assertThrows(IOException.class, () -> RollingKeyDecoder.text(new byte[2], -1, 1));
        assertThrows(IOException.class, () -> RollingKeyDecoder.text(new byte[2], 1, Integer.MAX_VALUE));
    }
}
