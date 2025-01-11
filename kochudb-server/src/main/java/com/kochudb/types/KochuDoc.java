package com.kochudb.types;

import static com.kochudb.k.Record.KEY;
import static com.kochudb.k.Record.VALUE;
import static com.kochudb.utils.ByteUtil.bytesToInt;
import static com.kochudb.utils.ByteUtil.bytesToLong;
import static com.kochudb.utils.ByteUtil.intToBytes;
import static com.kochudb.utils.ByteUtil.longToBytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.kochudb.utils.ByteUtil;

public class KochuDoc implements Comparable<KochuDoc> {

    ByteArray key, value;
    long lastModified;

    public KochuDoc(ByteArray key, ByteArray value, long modifiedAt) {
        this.key = key;
        this.value = value;
        this.lastModified = modifiedAt;
    }

    public KochuDoc(byte[] key, byte[] value, long modifiedAt) {
        this.key = new ByteArray(key);
        this.value = new ByteArray(value);
        this.lastModified = modifiedAt;
    }

    public static KochuDoc deserialize(byte[] key, byte[] value, long timestamp) {
        return new KochuDoc(unzip(key), unzip(value), timestamp);
    }

    /**
     * create an instance of this class from bytes
     * 
     * @param bytes
     * @return
     */
    public static KochuDoc deserialize(byte[] bytes) {
        byte[] timeBytes = new byte[Long.BYTES];
        int curPos = 0;

        System.arraycopy(bytes, 0, timeBytes, 0, Long.BYTES);
        long timestamp = bytesToLong(timeBytes);
        curPos += Long.BYTES;

        byte[] keyPrefix = new byte[KEY.length];
        System.arraycopy(bytes, curPos, keyPrefix, 0, KEY.length);
        int keySize = ByteUtil.bytesToInt(keyPrefix);
        curPos += KEY.length;

        byte[] key = new byte[keySize];
        System.arraycopy(bytes, curPos, key, 0, key.length);
        curPos += key.length;

        byte[] valuePrefix = new byte[VALUE.length];
        System.arraycopy(bytes, curPos, valuePrefix, 0, VALUE.length);
        int valueSize = bytesToInt(valuePrefix);
        curPos += VALUE.length;

        byte[] value = new byte[valueSize];
        System.arraycopy(bytes, curPos, value, 0, value.length);

        return new KochuDoc(unzip(key), unzip(value), timestamp);
    }

    /**
     * convert key and value into a byte[] in the below format: [len(keybytes) in 1
     * byte, keybytes, len(valbytes) in 4 bytes, valbytes]
     * 
     * @return byte[]
     */
    public byte[] serialize() {
        byte[] keyData = zip(this.key.bytes());
        byte[] valData = zip(this.value.bytes());
        byte[] timeData = longToBytes(this.lastModified);

        byte[] bytes = new byte[KEY.length + VALUE.length + Long.BYTES + keyData.length + valData.length
                + timeData.length];

        int curPos = 0;

        // copy timestamp
        System.arraycopy(timeData, 0, bytes, curPos, Long.BYTES);
        curPos += Long.BYTES;

        // copy size of the key
        byte[] keyPrefix = intToBytes(KEY.length, keyData.length);
        System.arraycopy(keyPrefix, 0, bytes, curPos, KEY.length);
        curPos += KEY.length;

        // copy actual key
        System.arraycopy(keyData, 0, bytes, curPos, keyData.length);
        curPos += keyData.length;

        // copy size of the value
        byte[] valPrefix = intToBytes(VALUE.length, valData.length);
        System.arraycopy(valPrefix, 0, bytes, curPos, VALUE.length);
        curPos += VALUE.length;

        // copy actual value
        System.arraycopy(valData, 0, bytes, curPos, valData.length);
        curPos += valData.length;

        return bytes;
    }

    private static byte[] zip(byte[] bytes) {
        ByteArrayOutputStream bytesStream = new ByteArrayOutputStream();
        try (GZIPOutputStream zip = new GZIPOutputStream(bytesStream)) {
            zip.write(bytes);
            zip.close();
            return bytesStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return bytes;
    }

    private static byte[] unzip(byte[] zippedBytes) {
        ByteArrayInputStream bytesStream = new ByteArrayInputStream(zippedBytes);
        try (GZIPInputStream unzip = new GZIPInputStream(bytesStream)) {
            return unzip.readAllBytes();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return zippedBytes;
    }

    public ByteArray getKey() {
        return key;
    }

    public void setKey(ByteArray key) {
        this.key = key;
    }

    public ByteArray getValue() {
        return value;
    }

    public void setValue(ByteArray value) {
        this.value = value;
    }

    public long getLastModified() {
        return lastModified;
    }

    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    public int length() {
        return this.key.length() + this.value.length() + Long.BYTES;
    }

    @Override
    public int compareTo(KochuDoc o) {
        int res = this.getKey().compareTo(o.getKey());
        return res;
    }

    @Override
    public String toString() {
        return key + ", " + value + ", " + lastModified;
    }

}
