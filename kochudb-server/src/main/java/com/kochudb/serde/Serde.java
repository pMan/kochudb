package com.kochudb.serde;

public interface Serde<T> {
	
	byte[] serialize();
	
	T deserialize(byte[] bytes);
}
