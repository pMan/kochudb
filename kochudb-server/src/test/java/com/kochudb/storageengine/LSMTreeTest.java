package com.kochudb.storageengine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.kochudb.types.ByteArray;
import com.kochudb.types.LSMTree;

class LSMTreeTest {

	static LSMTree lsmt;
	static Properties props = new Properties();
	
	@BeforeAll
	static void testLSMTree() {
		try {
			props.put("data.dir", "data-test");
			LSMTreeTest.lsmt = new LSMTree(props);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@AfterAll
	static void tearDownLSMTree() {
		new File((String) props.get("data.dir")).delete();
	}

	@Test
	void testSetAndGet() {
		ByteArray key = new ByteArray("Key".getBytes());
		byte[] val = "Val".getBytes();
		String result = LSMTreeTest.lsmt.set(key, val);
		
		assertEquals("ok", result);
		assertEquals("Val", LSMTreeTest.lsmt.get(key));
	}
	
	@Test
	void testGetNonExistingKey() {
		assertEquals("", LSMTreeTest.lsmt.get(new ByteArray("non-existing-key")));
	}
}
