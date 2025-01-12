package com.kochudb.shared;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public record Response(byte[] key, byte[] value, byte[] lastModified) implements Serializable {

    public Response(String key, String value, byte[] lastModified) {
        this(key.getBytes(), value.getBytes(), lastModified);
    }

    public static long bytesToLong(final byte[] bytes) {
        long aLong = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            aLong = (aLong << 8) + (bytes[i] & 0xFF);
        }
        return aLong;
    }

    private static String toISO(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        return DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of("UTC")).format(instant);
    }

    public String print() {
        return new String(value, StandardCharsets.UTF_8) + " ( lastModified at " + (toISO(bytesToLong(lastModified)))
                + " )";
    }

    @Override
    public String toString() {
        return "[key=" + new String(key, StandardCharsets.UTF_8) + ", value="
                + new String(value, StandardCharsets.UTF_8) + ", lastModified=" + (toISO(bytesToLong(lastModified)))
                + "]";
    }

}
