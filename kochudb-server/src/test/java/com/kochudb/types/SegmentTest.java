package com.kochudb.types;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import com.kochudb.storage.Segment;

class SegmentTest {

    private static Segment seg;

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

        seg = new Segment(0, "testfile.idx", "testfile.kdb");
    }

    @Test
    void testWriteIndexFile() throws Exception {
        // Setup
        final Map<ByteArray, Long> keyToOffset = Map.ofEntries(Map.entry(new ByteArray("t"), 10L));

        // Segment seg = new Segment(0);
        // Run the test
        seg.saveIndexFile(keyToOffset);

        // Verify the results
    }

    @Test
    static void testReadIndexFile() {
        // Setup
        // Run the test
        final Map<ByteArray, Long> result = seg.parseIndexFile();

        assertTrue(result.containsKey(new ByteArray("t")));
        assertEquals(0L, result.get(new ByteArray("t")));
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

        dataFile.close();
    }

    @Test
    void testAppendData_ThrowsIOException() throws Exception {
        // Setup
        final RandomAccessFile dataFile = new RandomAccessFile("./filename", "r");

        // Run the test
        assertThrows(IOException.class, () -> seg.appendData(dataFile, "content".getBytes()));

        dataFile.close();
    }

    @Test
    void testReadBytes() throws Exception {
        // Setup
        final RandomAccessFile raf = new RandomAccessFile("filename", "r");

        // Run the test
        final byte[] result = seg.readBytes(raf, 0L, 1);
        raf.close();

        // Verify the results
        assertArrayEquals(new byte[] { 1 }, result);
    }

    @Test
    void testReadBytes_ThrowsIOException() throws Exception {
        // Setup
        final RandomAccessFile raf = new RandomAccessFile("./filename", "r");

        // Run the test
        assertThrows(FileNotFoundException.class,
                () -> seg.readBytes(new RandomAccessFile("./filename/123", "r"), 0L, 1));

        raf.close();
    }

    @AfterAll
    static void teardown() throws IOException {
    }

}
