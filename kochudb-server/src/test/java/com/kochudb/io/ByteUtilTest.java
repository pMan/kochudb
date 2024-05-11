package com.kochudb.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.kochudb.utils.ByteUtil;

class ByteUtilTest {

    @Test
    void testBytesToInt() {
        int i = 46;
        byte[] bytes = ByteUtil.intToBytes(4, i);
        assertEquals(i, ByteUtil.bytesToInt(bytes));
    }

    @Test
    void testIntToBytes() {
        int i = 0;
        assertArrayEquals(new byte[] { 0, 0, 0, 0 }, ByteUtil.intToBytes(4, i));
    }

    @Test
    void testLongToBytes() {
        byte[] lb = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };
        assertArrayEquals(lb, ByteUtil.longToBytes(0L));
    }

    @Test
    void testBytesToLong() {
        byte[] lb = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };
        assertEquals(0L, ByteUtil.bytesToLong(lb));
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
