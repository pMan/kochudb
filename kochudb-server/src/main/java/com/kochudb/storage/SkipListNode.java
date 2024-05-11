package com.kochudb.storage;

import com.kochudb.types.ByteArray;

/**
 * Skip list Element
 */
public class SkipListNode {

    // key/value of type ByteArray
    ByteArray key, val;

    public boolean deleted;

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
        deleted = false;
    }

    public ByteArray getKey() {
        return key;
    }

    public ByteArray getValue() {
        return val;
    }

    public void setValue(ByteArray val) {
        this.val = val;
    }

    public void delete() {
        deleted = true;
    }

    public void undelete() {
        deleted = false;
    }

    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public String toString() {
        return new String(key.serialize());
    }
}
