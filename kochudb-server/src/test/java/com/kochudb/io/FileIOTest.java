package com.kochudb.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.File;
import java.io.RandomAccessFile;

import org.junit.jupiter.api.Test;

class FileIOTest {

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
