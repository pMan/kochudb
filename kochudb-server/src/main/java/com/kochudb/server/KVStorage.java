package com.kochudb.server;

import com.kochudb.types.ByteArray;
import com.kochudb.types.ByteArrayKey;

/**
 * Key-Value based storage engine
 * 
 * @param <K> comparable, serializable key
 * @param <V> serializable value
 */
public interface KVStorage<K extends ByteArrayKey, V extends ByteArray> {
    
    public V get(K key);

    public byte[] set(K key, V val);

    public byte[] del(K key);
}
