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
		node = new SkipListNode(new KochuDoc("Key".getBytes(), "Value".getBytes(), 0L));
		node.right = new SkipListNode(new KochuDoc(new byte[] {}, null, 0L));
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@Test
	void test() {

		assertTrue(node.compareTo(new KochuDoc(new ByteArray("Key".getBytes()), null, 0L)) == 0);

		assertArrayEquals("Value".getBytes(), node.getValue().bytes());

		assertNull(node.down);

		assertNotNull(node.right);
	}

}
