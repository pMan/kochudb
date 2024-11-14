package com.kochudb.storage;

import com.kochudb.types.ByteArray;
import com.kochudb.types.KochuDBSerde;

/**
 * Skip list Element
 */
public class SkipListNode<T extends KochuDBSerde<T>> implements Comparable<T> {

    KochuDBSerde<T> data;

    public boolean deleted;

    // references to all four neighbors
    public SkipListNode<T> left, right, up, down;

    /**
     * An element in the SkipList, stores key and value
     * 
     * @param key key
     * @param val value
     */
    public SkipListNode(KochuDBSerde<T> data) {
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
        return data.key();
    }

    public ByteArray getValue() {
        return data.value();
    }

    @Override
    public int compareTo(T o) {
        return getKey().compareTo(o.key());
    }

    @Override
    public String toString() {
        return data.toString();
    }

}
