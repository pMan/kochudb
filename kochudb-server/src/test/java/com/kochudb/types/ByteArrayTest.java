package com.kochudb.types;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ByteArrayTest {

    private ByteArray byteArrayUnderTest;

    @BeforeEach
    void setUp() {
        byteArrayUnderTest = new ByteArray("t".getBytes());
    }

    @Test
    void testLength() {
        assertEquals(1, byteArrayUnderTest.length());
    }

    @Test
    void testCompareTo() {
        // Setup
        final ByteArray o = new ByteArray("t");

        // Run the test
        final int result = byteArrayUnderTest.compareTo(o);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    void testCompareTo_ThrowsNullPointerException() {
        // Setup
        final ByteArray o = null;

        // Run the test
        assertThrows(NullPointerException.class, () -> byteArrayUnderTest.compareTo(o));
    }

    @Test
    void testGetBytes() {
        assertArrayEquals("t".getBytes(), byteArrayUnderTest.bytes());
    }
}
