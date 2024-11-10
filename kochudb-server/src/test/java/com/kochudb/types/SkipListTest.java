package com.kochudb.types;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kochudb.storage.SkipList;
import com.kochudb.storage.SkipListNode;

class SkipListTest {

    private SkipList<Record> skipListUnderTest;

    @BeforeEach
    void setUp() {
        skipListUnderTest = new SkipList<Record>();
    }

    @Test
    void testFind() {
        // Setup
        final ByteArray key = new ByteArray("t");

        // Run the test
        final SkipListNode<Record> result = skipListUnderTest.find(new Record(key, null, 0L));

        assertNull(result);

        assertNotNull(result);
    }

    @Test
    void testGet() {
        // Setup
        final ByteArray key = new ByteArray("t");

        // Run the test
        // final SkipListNode result = skipListUnderTest.get(key);

        assertNull(skipListUnderTest.find(new Record(new ByteArray(), null, 0L)));
    }

    @Test
    void testContainsKey() {
        // Setup
        final ByteArray key = new ByteArray("t");

        // Run the test
        final boolean result = skipListUnderTest.containsKey(new Record(new ByteArray(), null, 0L));

        // Verify the results
        assertFalse(result);
    }

    @Test
    void testPut() {
        // Setup
        final ByteArray key = new ByteArray("t");

        // Run the test
        skipListUnderTest
                .put(new Record(new ByteArray("key".getBytes()), new ByteArray("value".getBytes()), 0L));

        // Verify the results
        assertArrayEquals("value".getBytes(),
                skipListUnderTest.get(new Record("key".getBytes(), null, 0L)).getValue().byteArray);
    }

    @Test
    void testDel() {
        // Setup
        final ByteArray key = new ByteArray("t");
        skipListUnderTest.put(new Record(new ByteArray(), null, 0L));

        // Run the test
        final boolean result = skipListUnderTest.del(new Record(new ByteArray("k".getBytes()), null, 0L));
        final boolean neg = skipListUnderTest
                .del(new Record(new ByteArray("non-exising".getBytes()), null, 0L));

        // Verify the results
        assertTrue(result);
        assertFalse(neg);
    }

    @Test
    void testIterator() {
        // Setup
        // Run the test
        final Iterator<SkipListNode<Record>> result = skipListUnderTest.iterator();

        // Verify the results
    }

    @Test
    void testToString() {
        assertEquals("\nhead tail \n", skipListUnderTest.toString());
    }
}
