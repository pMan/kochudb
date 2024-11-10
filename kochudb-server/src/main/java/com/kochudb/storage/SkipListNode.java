package com.kochudb.storage;

import java.nio.charset.StandardCharsets;

import com.kochudb.types.ByteArray;
import com.kochudb.types.KochuDBSerde;

/**
 * Skip list Element
 */
public class SkipListNode<T extends KochuDBSerde<T>> {

    T data;

    public boolean deleted;

    // references to all four neighbors
    public SkipListNode<T> left, right, up, down;

    /**
     * An element in the SkipList, stores key and value
     * 
     * @param key key
     * @param val value
     */
    public SkipListNode(T node) {
        this.data = node;

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
    public String toString() {
        return new String(getKey().bytes(), StandardCharsets.UTF_8) + ", "
                + new String(getValue().bytes(), StandardCharsets.UTF_8);
    }

}
