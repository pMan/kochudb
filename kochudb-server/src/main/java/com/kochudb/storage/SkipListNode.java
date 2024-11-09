package com.kochudb.storage;

import com.kochudb.types.KochuDBSerde;

/**
 * Skip list Element
 */
public class SkipListNode<K extends KochuDBSerde<K>, V extends KochuDBSerde<V>> {

    // key/value of type ByteArray
    K key;
    V val;

    public boolean deleted;

    // references to all four neighbors
    public SkipListNode<K, V> left, right, up, down;

    /**
     * An element in the SkipList, stores key and value
     * 
     * @param key key
     * @param val value
     */
    public SkipListNode(K key, V val) {
        this.key = key;
        this.val = val;

        left = null;
        right = null;
        up = null;
        down = null;
        deleted = false;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return val;
    }

    public void setValue(V val) {
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
        return new String(key.serialize()) + ", " + new String(val.serialize());
    }
}
