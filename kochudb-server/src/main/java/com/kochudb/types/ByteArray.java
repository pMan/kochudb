package com.kochudb.types;

/**
 * A comparable byte[]
 */
public class ByteArray implements Comparable<ByteArray> {

	byte[] byteArray;
	
	public ByteArray(String s) {
		byteArray = s.getBytes();
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
	
	@Override
	public int compareTo(ByteArray o) {
		for (int i = 0, j = 0; i < this.byteArray.length && j < o.byteArray.length; i++, j++) {
            int a = (this.byteArray[i] & 0xff);
            int b = (o.byteArray[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
		return this.byteArray.length - o.byteArray.length;
	}

	public byte[] getBytes() {
		return byteArray;
	}

}
