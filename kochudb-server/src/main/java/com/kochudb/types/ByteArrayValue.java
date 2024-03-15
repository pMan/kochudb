package com.kochudb.types;

import com.kochudb.serde.Serde;

/**
 * A serializable, immutable byte[]
 */
public class ByteArrayValue implements Serde<ByteArrayValue> {

	byte[] byteArray;
    
    public ByteArrayValue(String key) {
        byteArray = key == null ? new byte[] {} : key.getBytes();
    }
    
    public ByteArrayValue(byte[] bytes) {
        byteArray = bytes;
    }
    
    public ByteArrayValue() {
        byteArray = new byte[] {};
    }

    public int length() {
        return byteArray.length;
    }
    
    public byte[] getBytes() {
        return byteArray;
    }
    
    @Override
    public byte[] serialize() {
        return byteArray;
    }
    
    @Override
    public ByteArrayValue deserialize(byte[] bytes) {
    	return new ByteArrayValue(bytes);
    }

}
