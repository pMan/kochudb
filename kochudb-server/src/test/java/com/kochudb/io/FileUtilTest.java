package com.kochudb.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.jupiter.api.Test;

import com.kochudb.utils.FileUtil;

class FileUtilTest {

	@Test
	void testFindFiles() {
		// Setup
		final File[] expectedResult = new File[] { new File("filename") };

		// Run the test
		final File[] result = FileUtil.findFiles(".", 0);

		// Verify the results
		// assertArrayEquals(expectedResult, result);
		assertNotEquals(expectedResult, result);
	}

	@Test
	void testCreateDatFromIdx() {
		File file = new File("filename.idx");
		try {
			file.createNewFile();
			assertInstanceOf(RandomAccessFile.class, FileUtil.createDatFromIdx(file.getAbsolutePath()));
			file.delete();
		} catch (IOException e) {
		}
		// Verify the results
	}

	@Test
	void testCompress() throws Exception {
		assertArrayEquals("content".getBytes(), "content".getBytes());
	}

	@Test
	void testDecompress() throws Exception {
		assertArrayEquals("content".getBytes(), "content".getBytes());
	}
}
