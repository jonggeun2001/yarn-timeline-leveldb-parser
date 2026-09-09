/*
 * Key layout and reverse-long decoding adapted from Apache Hadoop 3.1.1
 * LeveldbUtils / GenericObjectMapper, licensed under Apache License 2.0.
 * See THIRD-PARTY-NOTICES.md and LICENSE.
 */
package io.github.timelineparser.compat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.util.Arrays;

public final class RollingKeyDecoder {
    public Header decode(byte[] key) throws IOException {
        Cursor cursor = new Cursor(key, 0);
        String type = cursor.delimitedString();
        long start = cursor.reverseLong();
        String id = cursor.delimitedString();
        if (type.isEmpty() || id.isEmpty()) throw new IOException("Empty entity type or ID");
        return new Header(type, id, start, cursor.offset());
    }

    public static final class Header {
        public final String type;
        public final String id;
        public final long startTime;
        public final int prefixLength;
        Header(String type, String id, long start, int prefixLength) {
            this.type = type; this.id = id; this.startTime = start; this.prefixLength = prefixLength;
        }
        public boolean sameEntity(Header other) {
            return other != null && type.equals(other.type) && id.equals(other.id) && startTime == other.startTime;
        }
    }

    public static final class Cursor {
        private final byte[] bytes;
        private int offset;
        public Cursor(byte[] bytes, int offset) { this.bytes = bytes; this.offset = offset; }
        public int offset() { return offset; }
        public String delimitedString() throws IOException {
            int start = offset;
            while (offset < bytes.length && bytes[offset] != 0) offset++;
            if (offset == bytes.length) throw new IOException("Missing binary key string delimiter");
            String value = text(bytes, start, offset - start);
            offset++;
            return value;
        }
        public long reverseLong() throws IOException {
            if (offset + 8 > bytes.length) throw new IOException("Truncated reverse-ordered timestamp");
            long value = 0;
            for (int i = 0; i < 8; i++) value = (value << 8) | (bytes[offset++] & 0xffL);
            return value ^ Long.MAX_VALUE;
        }
        public byte[] remainingBytes() { return Arrays.copyOfRange(bytes, offset, bytes.length); }
        public String remainingString() throws IOException { return text(bytes, offset, bytes.length - offset); }
    }

    public static String text(byte[] bytes, int offset, int length) throws IOException {
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes, offset, length)).toString();
        } catch (CharacterCodingException e) {
            throw new IOException("Invalid UTF-8 in LevelDB key", e);
        }
    }
}
