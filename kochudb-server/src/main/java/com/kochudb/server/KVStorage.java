package com.kochudb.server;

import com.kochudb.types.ByteArray;

/**
 * Key-Value based storage engine
 * 
 * @param <K> comparable, serializable, immutable key
 * @param <V> serializable, immutable key
 */
public interface KVStorage<K extends ByteArray, V extends ByteArray> {
    
    public V get(K key);

    public byte[] set(K key, V val);

    public byte[] del(K key);
}
