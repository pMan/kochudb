package com.kochudb.types;

/**
 * A comparable, serializable, immutable byte[]
 */
public class ByteArrayKey extends ByteArray implements Comparable<ByteArrayKey> {
    
    public ByteArrayKey(String key) {
        byteArray = key.getBytes();
    }
    
    public ByteArrayKey(byte[] bytes) {
        byteArray = bytes;
    }
    
    public ByteArrayKey() {
        byteArray = new byte[] {};
    }

    @Override
    public int compareTo(ByteArrayKey o) {
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

}
