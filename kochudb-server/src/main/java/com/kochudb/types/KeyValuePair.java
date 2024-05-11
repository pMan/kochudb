package com.kochudb.types;

import static com.kochudb.k.Record.KEY;
import static com.kochudb.k.Record.VALUE;
import static com.kochudb.utils.ByteUtil.intToBytes;

public record KeyValuePair(ByteArray key, ByteArray value) {

    public KeyValuePair(byte[] key, byte[] value) {
        this(new ByteArray(key), new ByteArray(value));
    }

    /**
     * convert key and value into a byte[] in the below format: [len(keybytes) in 1
     * byte, keybytes, len(valbytes) in 4 bytes, valbytes]
     * 
     * @return byte[]
     */
    public byte[] serialize() {
        byte[] bytes = new byte[KEY.length + VALUE.length + this.key.length() + this.value.length()];
        int curPos = 0;

        byte[] keyData = this.key.serialize();
        byte[] valData = this.value.serialize();

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
}
