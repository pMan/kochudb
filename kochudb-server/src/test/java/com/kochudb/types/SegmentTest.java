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
    void testAppendData_ThrowsIOException() throws Exception {
        // Setup
        File f = new File("./filename.kdb");
        f.deleteOnExit();
        final RandomAccessFile dataFile = new RandomAccessFile(f, "r");

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
        assertArrayEquals(new byte[] { 99 }, result);
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
        File f = new File(seg.getIndexFile());
        f.delete();

        f = new File(seg.getDataFile());
        f.delete();
    }

}
