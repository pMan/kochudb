package com.kochudb.types;

import java.io.Serializable;

/**
 * 
 * @author pman
 *
 * @param <T> any type that will be stored onto disk by KochuDB. ByteArray is
 *            used by default.
 */
public interface KochuDBSerde<T> extends Comparable<T>, Serializable {

    public byte[] serialize();

    public static <T> T deserialize(byte[] bytes) {
        throw new RuntimeException("Deserialization not implemented");
    }

    public int length();

}
