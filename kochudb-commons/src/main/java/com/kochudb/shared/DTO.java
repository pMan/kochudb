package com.kochudb.shared;

import java.io.Serializable;

public record DTO(byte[] command, byte[] key, byte[] value, byte[] response) implements Serializable {

    public DTO(String command, String key, byte[] value) {
        this(command.getBytes(), key.getBytes(), value, null);
    }

    public DTO(byte[] command, byte[] key, byte[] value) {
        this(command, key, value, null);
    }

}
