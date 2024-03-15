package com.kochudb.server;

/**
 * Key-Value based storage engine
 * 
 * @param <K> comparable key
 * @param <V> serializable value
 */
public interface KVStorage<K extends Comparable<K>, V> {
    
    public V get(K key);

    public byte[] set(K key, V val);

    public byte[] del(K key);
}
