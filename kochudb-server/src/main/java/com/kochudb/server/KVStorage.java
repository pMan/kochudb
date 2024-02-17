package com.kochudb.server;

import java.io.Serializable;

/**
 * Key-Value based storage engine
 * 
 * @param <K> comparable key
 * @param <V> serializable value
 */
public interface KVStorage<K extends Comparable<K>, V> {
	
	public V get(K key);

	public V set(K key, V val);

	public V del(K key);
}
