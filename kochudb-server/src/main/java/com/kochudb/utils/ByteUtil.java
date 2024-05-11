package com.kochudb.utils;

public final class ByteUtil {

    /**
     * convert a byte[] to int. This function does not expect a byte[] longer that 4
     * bytes
     * 
     * @param b byte[]
     * @return int
     */
    public static int bytesToInt(byte[] b) {
        int result = 0;
        for (int i = 0; i < b.length; i++) {
            result <<= 8;
            result |= (b[i] & 0xFF);
        }
        return result;
    }

    /**
     * Convert an int to byte[] of len 1 or 4
     * 
     * @param resultLen length of result array
     * @param in        integer
     * @return byte[]
     */
    public static byte[] intToBytes(int resultLen, int in) {
        byte[] bytes = new byte[resultLen];
        for (int i = 0; i < resultLen; i++) {
            int cur = resultLen - i - 1;
            bytes[i] = (byte) ((in & 0xFF) >> (cur * 8));
        }
        return bytes;
    }

    /**
     * Convert a byte[] of size 8 into a Long
     * 
     * @param bytes byte[]
     */
    public static long bytesToLong(final byte[] bytes) {
        long aLong = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            aLong = (aLong << 8) + (bytes[i] & 0xFF);
        }
        return aLong;
    }

    /**
     * Convert a Long object to a byte[] of size 8
     * 
     * @param aLong long
     * @return bytes
     */
    public static byte[] longToBytes(Long aLong) {
        byte[] bytes = new byte[Long.BYTES];
        int length = bytes.length;
        for (int i = 0; i < length; i++) {
            bytes[length - i - 1] = (byte) (aLong & 0xFF);
            aLong >>= 8;
        }
        return bytes;
    }

}
