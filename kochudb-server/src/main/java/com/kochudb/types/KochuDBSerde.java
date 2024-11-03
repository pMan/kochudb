package com.kochudb.types;

import java.io.Serializable;

public interface KochuDBSerde<T> extends Comparable<T>, Serializable {

    public byte[] serialize();

    public static <T> T deserialize(byte[] bytes) {
        throw new RuntimeException("Deserialization not implemented");
    }

    public int length();

}
