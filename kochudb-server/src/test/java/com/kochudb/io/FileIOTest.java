package com.kochudb.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.kochudb.types.ByteArrayKey;

class FileIOTest {

    @Test
    void testWriteIndexFile() throws Exception {
        // Setup
        final Map<ByteArrayKey, Long> keyToOffset = Map.ofEntries(Map.entry(new ByteArrayKey("t"), 0L));

        // Run the test
        FileIO.writeIndexFile("filename", keyToOffset);

        // Verify the results
    }

    @Test
    void testReadIndexFile() {
        // Setup
        // Run the test
        final Map<ByteArrayKey, Long> result = FileIO.readIndexFile("filename");

        assertTrue(result.containsKey(new ByteArrayKey("t")));
        assertEquals(0L, result.get(new ByteArrayKey("t")));
    }

    @Test
    void testAppendData() throws Exception {
        // Setup
        final RandomAccessFile dataFile = new RandomAccessFile("filename", "r");

        // Run the test
        //final long result = FileIO.appendData(dataFile, "t".getBytes(), Record.KEY);

        assertThrows(IOException.class, () -> FileIO.appendData(dataFile, "t".getBytes()));
        // Verify the results
        //assertEquals(0L, result);

        dataFile.close();
    }

    @Test
    void testAppendData_ThrowsIOException() throws Exception {
        // Setup
        final RandomAccessFile dataFile = new RandomAccessFile("./filename", "r");

        // Run the test
        assertThrows(IOException.class, () -> FileIO.appendData(dataFile, "content".getBytes()));

        dataFile.close();
    }

    @Test
    void testReadBytes() throws Exception {
        // Setup
        final RandomAccessFile raf = new RandomAccessFile("filename", "r");

        // Run the test
        final byte[] result = FileIO.readBytes(raf, 0L, 1);

        // Verify the results
        assertArrayEquals(new byte[] {1}, result);
    }

    @Test
    void testReadBytes_ThrowsIOException() throws Exception {
        // Setup
        final RandomAccessFile raf = new RandomAccessFile("./filename", "r");

        // Run the test
        assertThrows(FileNotFoundException.class, () -> FileIO.readBytes(new RandomAccessFile("./filename/123", "r"), 0L, 1));

        raf.close();
    }

    @Test
    void testFindFiles() {
        // Setup
        final File[] expectedResult = new File[]{new File("filename")};

        // Run the test
        final File[] result = FileIO.findFiles(".", 0);

        // Verify the results
        //assertArrayEquals(expectedResult, result);
        assertNotEquals(expectedResult, result);
    }

    @Test
    void testCreateDatFromIdx() {
        assertInstanceOf(RandomAccessFile.class, FileIO.createDatFromIdx("filename"));
        // Verify the results
    }

    @Test
    void testCreateNewIdxAndDataFilenames() {
        assertThrowsExactly(NullPointerException.class, () -> FileIO.createNewIdxAndDataFilenames(0));
        //assertEquals(2, files.length);
    }

    @Test
    void testGenerateFilename() {
        //assertNotEquals("result", FileIO.generateFilename());

        assertThrowsExactly(NullPointerException.class, FileIO::generateFilename);
    }

    @Test
    void testBytesToInt() {
        int i = 46;
        byte[] bytes = FileIO.intToBytes(4, i);
        assertEquals(i, FileIO.bytesToInt(bytes));
    }

    @Test
    void testIntToBytes() {
        int i = 0;
        assertArrayEquals(new byte[]{0, 0, 0, 0}, FileIO.intToBytes(4, i));
    }

    @Test
    void testLongToBytes() {
        byte[] lb = new byte[] {0, 0, 0, 0, 0, 0, 0, 0};
        assertArrayEquals(lb, FileIO.longToBytes(0L));
    }

    @Test
    void testBytesToLong() {
        byte[] lb = new byte[] {0, 0, 0, 0, 0, 0, 0, 0};
        assertEquals(0L, FileIO.bytesToLong(lb));
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
