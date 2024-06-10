package com.kochudb.types;

import static com.kochudb.k.Record.KEY;
import static com.kochudb.k.Record.VALUE;
import static com.kochudb.utils.ByteUtil.intToBytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public record KeyValuePair(ByteArray key, ByteArray value) {

    public KeyValuePair(byte[] key, byte[] value) {
        this(new ByteArray(key), new ByteArray(value));
    }
    
    /**
     * create a new object from compressed key and value bytes
     * 
     * @param key compressed key
     * @param value compressed value
     * @return this
     */
    public static KeyValuePair fromCompressed(byte[] key, byte[] value) {
        return new KeyValuePair(new ByteArray(unzip(key)), new ByteArray(unzip(value)));
    }

    /**
     * convert key and value into a byte[] in the below format: [len(keybytes) in 1
     * byte, keybytes, len(valbytes) in 4 bytes, valbytes]
     * 
     * @return byte[]
     * @throws IOException 
     */
    public byte[] serialize() throws IOException {
        byte[] keyData = zip(this.key.serialize());
        byte[] valData = zip(this.value.serialize());

        byte[] bytes = new byte[KEY.length + VALUE.length + keyData.length + valData.length];
        int curPos = 0;
        
        byte[] keyPrefix = intToBytes(KEY.length, keyData.length);
        System.arraycopy(keyPrefix, 0, bytes, curPos, KEY.length);
        curPos += KEY.length;

        System.arraycopy(keyData, 0, bytes, curPos, keyData.length);
        curPos += keyData.length;

        byte[] valPrefix = intToBytes(VALUE.length, valData.length);
        System.arraycopy(valPrefix, 0, bytes, curPos, VALUE.length);
        curPos += VALUE.length;

        System.arraycopy(valData, 0, bytes, curPos, valData.length);

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
}
