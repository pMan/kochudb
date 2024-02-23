package com.kochudb.types;

import java.util.Comparator;

/**
 * A comparator for ByteArray
 */
public class ByteArrayComparator implements Comparator<ByteArray> {
    
    /**
     * Compare two {@linkplain ByteArray ByteArray} instances lexicographically
     */
    public ByteArrayComparator() {
        super();
    }

    @Override
    public int compare(ByteArray left, ByteArray right) {
        return left.compareTo(right);
    }
}
