package com.kochudb.storage;

import com.kochudb.types.ByteArray;

/**
 * Skip list Element
 */
public class SkipListNode {
    
    // key/value of type ByteArray
    ByteArray key, val;

    // references to all four neighbors
    public SkipListNode left, right, up, down;

    /**
     * An element in the SkipList, stores key and value
     * 
     * @param key key
     * @param val value
     */
    public SkipListNode(ByteArray key, ByteArray val) {
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
    
    public ByteArray getValue() {
        return val;
    }
    
    @Override
    public String toString() {
    	return new String(key.serialize());
    }
}
