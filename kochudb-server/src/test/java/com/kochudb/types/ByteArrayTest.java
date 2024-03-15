package com.kochudb.types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ByteArrayTest {

    private ByteArrayKey byteArrayUnderTest;

    @BeforeEach
    void setUp() {
        byteArrayUnderTest = new ByteArrayKey("t".getBytes());
    }

    @Test
    void testLength() {
        assertEquals(1, byteArrayUnderTest.length());
    }

    @Test
    void testCompareTo() {
        // Setup
        final ByteArrayKey o = new ByteArrayKey("t");

        // Run the test
        final int result = byteArrayUnderTest.compareTo(o);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    void testCompareTo_ThrowsNullPointerException() {
        // Setup
        final ByteArrayKey o = null;

        // Run the test
        assertThrows(NullPointerException.class, () -> byteArrayUnderTest.compareTo(o));
    }

    @Test
    void testGetBytes() {
        assertArrayEquals("t".getBytes(), byteArrayUnderTest.getBytes());
    }
}
