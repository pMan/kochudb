package com.kochudb.types;

/**
 * A serializable, immutable byte[]
 */
public class ByteArray {

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
    
    public byte[] serialize() {
        return byteArray;
    }

	public static ByteArray deserialize(byte[] bytes) {
		return new ByteArray(bytes);
	}

    @Override
    public String toString() {
    	return new String(byteArray);
    }
}
