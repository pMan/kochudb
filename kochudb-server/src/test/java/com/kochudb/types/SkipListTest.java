package com.kochudb.types;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Iterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kochudb.storage.SkipList;
import com.kochudb.storage.SkipListNode;

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
		final SkipListNode result = skipListUnderTest.find(new KochuDoc(key, new ByteArray(), 0L));

		assertNotNull(result);

		assertNotNull(result);
	}

	@Test
	void testGet() {
		// Setup
		final ByteArray key = new ByteArray("t");

		// Run the test
		// final SkipListNode result = skipListUnderTest.get(key);

		assertNotNull(skipListUnderTest.find(new KochuDoc(new ByteArray(), new ByteArray(), 0L)));
	}

	@Test
	void testContainsKey() {
		// Setup
		final ByteArray key = new ByteArray("t");

		// Run the test
		final boolean result = skipListUnderTest.containsKey(new KochuDoc(new ByteArray(), new ByteArray(), 0L));

		// Verify the results
		assertFalse(result);
	}

	@Test
	void testPut() {
		// Setup
		final ByteArray key = new ByteArray("t");

		// Run the test
		skipListUnderTest.put(new KochuDoc(new ByteArray("key".getBytes()), new ByteArray("value".getBytes()), 0L));

		// Verify the results
		assertArrayEquals("value".getBytes(),
				skipListUnderTest.get(new KochuDoc("key".getBytes(), null, 0L)).getValue().byteArray);
	}

	@Test
	void testDel() {
		// Setup
		final ByteArray key = new ByteArray("t");
		skipListUnderTest.put(new KochuDoc(key, new ByteArray(), 0L));

		// Run the test
		final boolean result = skipListUnderTest.del(new KochuDoc(new ByteArray("k".getBytes()), new ByteArray(), 0L));
		final boolean neg = skipListUnderTest
				.del(new KochuDoc(new ByteArray("non-exising".getBytes()), new ByteArray(), 0L));

		// Verify the results
		assertFalse(result);
		assertFalse(neg);
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
		assertEquals("headtail", skipListUnderTest.toString().replaceAll("[\\n\\t ]+", ""));
	}
}
