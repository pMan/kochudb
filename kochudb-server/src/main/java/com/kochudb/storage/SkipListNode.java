package com.kochudb.storage;

import com.kochudb.types.ByteArray;
import com.kochudb.types.KochuDoc;

/**
 * Skip list Element
 */
public class SkipListNode implements Comparable<KochuDoc> {

    KochuDoc data;

    public boolean deleted;

    // references to all four neighbors
    public SkipListNode left, right, up, down;

    /**
     * An element in the SkipList, stores key and value
     * 
     * @param key key
     * @param val value
     */
    public SkipListNode(KochuDoc data) {
        this.data = data;

        left = null;
        right = null;
        up = null;
        down = null;
        deleted = false;
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

    public ByteArray getKey() {
        return data.getKey();
    }

    public ByteArray getValue() {
        return data.getValue();
    }

    @Override
    public int compareTo(KochuDoc o) {
        return getKey().compareTo(o.getKey());
    }

    @Override
    public String toString() {
        return data == null ? "null" : data.toString();
    }

}
