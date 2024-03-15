package com.kochudb.types;

import com.kochudb.serde.Serde;

/**
 * A comparable, serializable, immutable byte[]
 */
public class ByteArrayKey implements Comparable<ByteArrayKey>, Serde<ByteArrayKey> {

	byte[] byteArray;
    
    public ByteArrayKey(String key) {
        byteArray = key.getBytes();
    }
    
    public ByteArrayKey(byte[] bytes) {
        byteArray = bytes;
    }
    
    public ByteArrayKey() {
        byteArray = new byte[] {};
    }

    public int length() {
        return byteArray.length;
    }
    
    @Override
    public int compareTo(ByteArrayKey o) {
        for (int i = 0, j = 0; i < this.byteArray.length && j < o.length(); i++, j++) {
            int a = (this.byteArray[i] & 0xff);
            int b = (o.getBytes()[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return this.byteArray.length - o.length();
    }

    public byte[] getBytes() {
        return byteArray;
    }
    
    @Override
    public byte[] serialize() {
        return byteArray;
    }
    
    @Override
    public ByteArrayKey deserialize(byte[] bytes) {
    	return new ByteArrayKey(bytes);
    }

}
