package com.kochudb.types;

import java.io.Serializable;

/**
 * A comparable, serializable, immutable byte[] that implements KochuDBSerde
 */
public class ByteArray implements Comparable<ByteArray>, Serializable {

    private static final long serialVersionUID = 1L;
    byte[] byteArray;

    public ByteArray(String key) {
        byteArray = key == null ? new byte[] {} : key.getBytes();
    }

    public ByteArray(byte[] bytes) {
        byteArray = bytes;
    }

    public ByteArray() {
        byteArray = new byte[] {};
    }

    public int length() {
        return byteArray.length;
    }

    public byte[] bytes() {
        return byteArray;
    }

    public static ByteArray deserialize(byte[] bytes) {
        return new ByteArray(bytes);
    }

    @Override
    public int compareTo(ByteArray o) {
        byte[] oByteArray = o.bytes();
        for (int i = 0, j = 0; i < this.byteArray.length && j < o.length(); i++, j++) {
            int a = (byteArray[i] & 0xff);
            int b = (oByteArray[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return this.byteArray.length - o.length();
    }

    @Override
    public String toString() {
        return new String(byteArray);
    }

}
