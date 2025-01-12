package com.kochudb.shared;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public record Request(byte[] command, byte[] key, byte[] value) implements Serializable {

    public Request(String command, String key, byte[] value) {
        this(command.getBytes(), key.getBytes(), value);
    }

    @Override
    public String toString() {
        return "[command=" + new String(command, StandardCharsets.UTF_8) + ", key="
                + new String(key, StandardCharsets.UTF_8) + ", value=" + new String(value, StandardCharsets.UTF_8)
                + "]";
    }

}
