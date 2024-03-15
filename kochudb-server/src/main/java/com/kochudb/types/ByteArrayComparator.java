package com.kochudb.types;

import java.util.Comparator;

/**
 * A comparator for ByteArray
 */
public class ByteArrayComparator implements Comparator<ByteArrayKey> {
    
    /**
     * Compare two {@linkplain ByteArrayKey ByteArray} instances lexicographically
     */
    public ByteArrayComparator() {
        super();
    }

    @Override
    public int compare(ByteArrayKey left, ByteArrayKey right) {
        return left.compareTo(right);
    }
}
