package com.kochudb.types;

/**
 * Skip list Element
 */
public class SkipListNode {
	
	// key of type comparable byte[]
	ByteArray key;
	
	// value of type byte[]
	byte[] val;

	// references to all four neighbors
	public SkipListNode left, right, up, down;

	/**
	 * An element in the SkipList, stores key and value
	 * 
	 * @param key key
	 * @param val value
	 */
	public SkipListNode(ByteArray key, byte[] val) {
		this.key = key;
		this.val = val;

		left = null;
		right = null;
		up = null;
		down = null;
	}
	
	public ByteArray getKey() {
		return key;
	}
	
	public byte[] getValue() {
		return val;
	}
}
