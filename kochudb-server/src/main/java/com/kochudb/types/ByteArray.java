package com.kochudb.types;

/**
 * A comparable, serializable, immutable byte[] that implements KochuDBSerde
 */
public class ByteArray implements KochuDBSerde<ByteArray> {

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

    @Override
    public int length() {
        return byteArray.length;
    }

    @Override
    public byte[] serialize() {
        return byteArray;
    }

    public static ByteArray deserialize(byte[] bytes) {
        return new ByteArray(bytes);
    }

    @Override
    public int compareTo(ByteArray o) {
        byte[] oByteArray = o.serialize();
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
