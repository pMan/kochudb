package com.kochudb.types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ByteArrayComparatorTest {

    private ByteArrayComparator byteArrayComparatorUnderTest;

    @BeforeEach
    void setUp() {
        byteArrayComparatorUnderTest = new ByteArrayComparator();
    }

    @Test
    void testCompare() {
        // Setup
        final ByteArray left = new ByteArray("t");
        final ByteArray right = new ByteArray("t");

        // Run the test
        final int result = byteArrayComparatorUnderTest.compare(left, right);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    void testCompare_ThrowsNullPointerException() {
        // Setup
        final ByteArray left = null;
        final ByteArray right = null;

        // Run the test
        assertThrows(NullPointerException.class, () -> byteArrayComparatorUnderTest.compare(left, right));
    }
}
