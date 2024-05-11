package com.kochudb.k;

/**
 * Enum representing a record in the key-value store. It holds properties of KEY
 * and VALUE. Currently, KEY has length 1 byte, and VALUE has length 4 bytes.
 */
public enum Record {
    /**
     * enum - key of the data store
     */
    KEY(1),

    /**
     * enum - value of the data store
     */
    VALUE(4);

    public final int length;

    /**
     * hold for key/value of a Record. Also stores the length of the header of
     * key/value
     * 
     * @param len length of key/value header
     */
    Record(int len) {
        this.length = len;
    }
}
