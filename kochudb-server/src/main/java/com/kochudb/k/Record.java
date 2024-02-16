package com.kochudb.k;

/**
 * for representing record key and value
 */
public enum Record {
	
	KEY(1), VALUE(4);

	public final int length;
	
	/**
	 * hold for key/value of a Record. Also stores the length
	 * of the header of key/value
	 * 
	 * @param len length of key/value header
	 */
	Record(int len) {
		this.length = len;
	}
}
