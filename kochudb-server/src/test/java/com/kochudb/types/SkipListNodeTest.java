package com.kochudb.types;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import com.kochudb.storage.SkipListNode;

@TestInstance(Lifecycle.PER_CLASS)
class SkipListNodeTest {

    SkipListNode node;

    @BeforeAll
    public void setUpBeforeAll() {
        node = new SkipListNode(new ByteArray("Key".getBytes()), new ByteArray("Value".getBytes()));
        node.right = new SkipListNode(null, null);
    }

    @BeforeEach
    void setUp() throws Exception {
    }

    @Test
    void test() {

        assertTrue(new ByteArray("Key".getBytes()).compareTo(node.getKey()) == 0);

        assertArrayEquals("Value".getBytes(), node.getValue().serialize());

        assertNull(node.down);

        assertNotNull(node.right);
    }

}
