package com.kochudb.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.kochudb.storage.LSMTree;
import com.kochudb.storage.SSTable;
import com.kochudb.storage.SkipList;

class SegmentTest {

	private static SSTable seg;

	static File dataDir;

	static LSMTree lsmt;
	static Properties props = new Properties();

	@BeforeAll
	static void setup() throws FileNotFoundException, IOException {
		props.put("data.dir", System.getProperty("java.io.tmpdir"));

		File dataDir = new File(props.getProperty("data.dir"));
		dataDir.mkdirs();
		dataDir.deleteOnExit();

		lsmt = new LSMTree(props);

		seg = new SSTable(0, "testfile.idx");
	}

	@Test
	void testWriteIndexFile() throws Exception {
		// Setup
		final Map<ByteArray, Long> keyToOffset = Map.ofEntries(Map.entry(new ByteArray("t"), 10L));

		// Segment seg = new Segment(0);
		// Run the test
		seg.saveIndex(keyToOffset);

		// Verify the results
	}

	@Test
	static void testReadIndexFile() {
		// Setup
		// Run the test
		final SkipList result = seg.parseIndex();

		assertTrue(result.containsKey(new KochuDoc("t".getBytes(), null, 0L)));
		assertEquals(0L, result.get(new KochuDoc("t".getBytes(), null, 0L)));
	}

	@Test
	static void testAppendData() throws Exception {
		// Setup
		final RandomAccessFile dataFile = new RandomAccessFile(seg.getDataFile(), "r");

		// Run the test
		// final long result = FileIO.appendData(dataFile, "t".getBytes(), Record.KEY);

		assertThrows(IOException.class, () -> seg.appendData(dataFile, "t".getBytes()));
		// Verify the results
		// assertEquals(0L, result);

		seg.openDataFileForWrite().close();
	}

	@Test
	void testAppendData_ThrowsIOException() {
		// Setup
		File f = new File("./filename.kdb");
		f.deleteOnExit();
		RandomAccessFile dataFile;
		assertThrows(FileNotFoundException.class, () -> new RandomAccessFile(f, "r"));
	}

	@AfterAll
	static void teardown() {
		File f = new File(seg.getIndexFile());
		f.delete();

		f = new File(seg.getDataFile());
		f.delete();
	}

}
