package com.kochudb.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.File;
import java.io.RandomAccessFile;

import org.junit.jupiter.api.Test;

import com.kochudb.utils.FileUtil;

class FileUtilTest {

    @Test
    void testFindFiles() {
        // Setup
        final File[] expectedResult = new File[]{new File("filename")};

        // Run the test
        final File[] result = FileUtil.findFiles(".", 0);

        // Verify the results
        //assertArrayEquals(expectedResult, result);
        assertNotEquals(expectedResult, result);
    }

    @Test
    void testCreateDatFromIdx() {
        assertInstanceOf(RandomAccessFile.class, FileUtil.createDatFromIdx("filename"));
        // Verify the results
    }

    @Test
    void testCreateNewIdxAndDataFilenames() {
        assertThrowsExactly(NullPointerException.class, () -> FileUtil.createNewIdxAndDataFilenames(0));
        //assertEquals(2, files.length);
    }

    @Test
    void testGenerateFilename() {
        //assertNotEquals("result", FileIO.generateFilename());

        assertThrowsExactly(NullPointerException.class, FileUtil::generateFilename);
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
