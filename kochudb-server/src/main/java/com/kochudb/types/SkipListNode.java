package com.kochudb.types;

/**
 * Skip list Element
 */
public class SkipListNode {
    
    // key of type comparable byte[]
    ByteArrayKey key;
    
    // value of type ByteArrayValue
    ByteArray val;

    // references to all four neighbors
    public SkipListNode left, right, up, down;

    /**
     * An element in the SkipList, stores key and value
     * 
     * @param key key
     * @param val value
     */
    public SkipListNode(ByteArrayKey key, ByteArray val) {
        this.key = key;
        this.val = val;

        left = null;
        right = null;
        up = null;
        down = null;
    }
    
    public ByteArrayKey getKey() {
        return key;
    }
    
    public ByteArray getValue() {
        return val;
    }
    
    @Override
    public String toString() {
    	return new String(key.serialize());
    }
}
