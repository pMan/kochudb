package com.kochudb.server;

import com.kochudb.types.KochuDBSerde;

/**
 * Key-Value based storage engine
 * 
 * @param <K> comparable, serializable, immutable key
 * @param <V> serializable, immutable key
 */
public interface KVStorage<K extends KochuDBSerde<K>, V extends KochuDBSerde<V>> {

    public V get(K key);

    public byte[] set(K key, V val);

    public byte[] del(K key);
}
