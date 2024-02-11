package com.kochudb.types;

/**
 * Key-Value based storage engine
 * 
 * @param <K> comparable key
 * @param <V> value
 */
public interface KVStorage<K extends Comparable<K>, V> {
	
	public String get(K key);

	public String set(K key, V val);

	public String del(K key);
}
