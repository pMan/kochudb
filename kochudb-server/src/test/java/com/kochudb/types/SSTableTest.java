package com.kochudb.types;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.File;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kochudb.storage.SSTable;

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
        final ByteArray result = ssTableUnderTest.search(key);

        //assertNull(ssTableUnderTest.search(key));
        // Verify the results
        assertArrayEquals(new byte[]{}, result.serialize());
    }
}
