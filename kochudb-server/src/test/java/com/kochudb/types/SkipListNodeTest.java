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

    SkipListNode<Record> node;

    @BeforeAll
    public void setUpBeforeAll() {
        node = new SkipListNode<Record>(
                new Record(new ByteArray("Key".getBytes()), new ByteArray("Value".getBytes()), 0L));
        node.right = new SkipListNode<Record>(new Record(new ByteArray(), null, 0L));
    }

    @BeforeEach
    void setUp() throws Exception {
    }

    @Test
    void test() {

        assertTrue(node.getValue().compareTo(new ByteArray("Key".getBytes())) == 0);

        assertArrayEquals("Value".getBytes(), node.getValue().bytes());

        assertNull(node.down);

        assertNotNull(node.right);
    }

}
