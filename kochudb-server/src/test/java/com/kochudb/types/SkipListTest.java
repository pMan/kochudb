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

class SkipListTest {

    private SkipList skipListUnderTest;

    @BeforeEach
    void setUp() {
        skipListUnderTest = new SkipList();
    }

    @Test
    void testFind() {
        // Setup
        final ByteArray key = new ByteArray("t");

        // Run the test
        final SkipListNode result = skipListUnderTest.find(key);

        assertNull(result.key);
        assertNull(result.val);

        assertNotNull(result);
    }

    @Test
    void testGet() {
        // Setup
        final ByteArray key = new ByteArray("t");

        // Run the test
        //final SkipListNode result = skipListUnderTest.get(key);

        assertNull(skipListUnderTest.get(key));
    }

    @Test
    void testContainsKey() {
        // Setup
        final ByteArray key = new ByteArray("t");

        // Run the test
        final boolean result = skipListUnderTest.containsKey(key);

        // Verify the results
        assertFalse(result);
    }

    @Test
    void testPut() {
        // Setup
        final ByteArray key = new ByteArray("t");

        // Run the test
        skipListUnderTest.put(key, new ByteArray("content".getBytes()));

        // Verify the results
        assertArrayEquals("content".getBytes(), skipListUnderTest.get(key).getValue().serialize());
    }

    @Test
    void testDel() {
        // Setup
        final ByteArray key = new ByteArray("t");
        skipListUnderTest.put(key, new ByteArray());

        // Run the test
        final boolean result = skipListUnderTest.del(key);
        final boolean neg = skipListUnderTest.del(new ByteArray("non-existing-key"));

        // Verify the results
        assertTrue(result);
        assertFalse(neg);
    }

    @Test
    void testUnlinkSkipListNode() {
        // Setup
        final SkipListNode node = new SkipListNode(new ByteArray("t"), new ByteArray("content".getBytes()));

        final SkipListNode head = new SkipListNode(null, null);
        final SkipListNode tail = new SkipListNode(null, null);
        head.right = node;
        node.left = head;
        node.right = tail;
        tail.left = node;

        // Run the test
        skipListUnderTest.unlinkSkipListNode(node);

        // Verify the results
        assertEquals(skipListUnderTest.head.right, skipListUnderTest.tail);
    }

    @Test
    void testAddNewLayer() {
        // Setup
        // Run the test
        skipListUnderTest.addNewLayer();

        // Verify the results
        assertEquals(skipListUnderTest.head.right, skipListUnderTest.tail);
    }

    @Test
    void testAddNewSkipListNodeToTower() {
        // Setup
        final SkipListNode p = new SkipListNode(new ByteArray("t"), new ByteArray("content".getBytes()));
        final SkipListNode q = new SkipListNode(new ByteArray("t"), new ByteArray("content".getBytes()));
        final SkipListNode dummy = new SkipListNode(new ByteArray("t"), null);
        final SkipListNode tail = new SkipListNode(null, null);
        p.right = tail;

        // Run the test
        final SkipListNode result = skipListUnderTest.addNewSkipListNodeToTower(p, q);

        // Verify the results
        assertEquals(q.getKey(), q.up.getKey());
        assertEquals(p.right, q.up);
    }

    @Test
    void testInsertRight() {
        // Setup
        final SkipListNode p = new SkipListNode(new ByteArray("t"), new ByteArray("content".getBytes()));
        final SkipListNode q = new SkipListNode(new ByteArray("t"), new ByteArray("content".getBytes()));

        final SkipListNode tail = new SkipListNode(null, null);
        p.right = tail;

        // Run the test
        skipListUnderTest.insertRight(p, q);

        // Verify the results
    }

    @Test
    void testIterator() {
        // Setup
        // Run the test
        final Iterator<SkipListNode> result = skipListUnderTest.iterator();

        // Verify the results
    }

    @Test
    void testToString() {
        assertEquals("head tail \n", skipListUnderTest.toString());
    }
}
