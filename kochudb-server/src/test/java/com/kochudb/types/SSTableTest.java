package com.kochudb.types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SSTableTest {

    private SSTable ssTableUnderTest;

    @BeforeEach
    void setUp() {
        ssTableUnderTest = new SSTable(new File("."));
    }

    @Test
    void testSearch() {
        // Setup
        final ByteArray key = new ByteArray("t");

        // Run the test
        final byte[] result = ssTableUnderTest.search(key);

        //assertNull(ssTableUnderTest.search(key));
        // Verify the results
        assertArrayEquals(new byte[]{}, result);
    }
}
