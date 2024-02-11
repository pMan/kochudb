package com.kochudb.types;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.kochudb.io.FileOps;

public record KVPair<V extends Serializable>(String key, V value) implements Serializable {

	@SuppressWarnings("unchecked")
	public KVPair(String key, Object val) {
		this(key, (V) val);
	}

	public static KVPair<?> of(byte[] b) throws ClassNotFoundException, IOException {
		return KVPair.from(b);
	}

	private static KVPair<?> from(byte[] bytes) throws ClassNotFoundException, IOException {
		// read key
		byte[] magicByte = new byte[] {bytes[Record.KEY.length]};
		System.arraycopy(bytes, 0, magicByte, 0, Record.KEY.length);
		int lenOfKey = FileOps.bytesToInt(magicByte);
		byte[] keyBytes = new byte[lenOfKey];
		System.arraycopy(bytes, Record.KEY.length, keyBytes, 0, lenOfKey);
		String key = new String(keyBytes, "utf-8");
		
		//read value
		magicByte = new byte[4];
		System.arraycopy(bytes, lenOfKey + Record.KEY.length, magicByte, 0, Record.VALUE.length);
		int lenOfVal = FileOps.bytesToInt(magicByte);
		byte[] valBytes = new byte[lenOfVal];
		System.arraycopy(bytes, lenOfKey + Record.KEY.length + Record.VALUE.length, valBytes, 0, lenOfVal);
		
		var byteStream = new ByteArrayInputStream(valBytes);
		ObjectInputStream in = new ObjectInputStream(byteStream);
		return new KVPair<Serializable>(key, in.readObject());
	}

	public byte[] toBytesArray() throws IOException {
		byte[] keyBytes = key().getBytes();
		
		var byteStream = new ByteArrayOutputStream();
		var out = new ObjectOutputStream(byteStream);
		out.writeObject(value());
		byte[] valBytes = byteStream.toByteArray();
		
		byte[] bytes = new byte[Record.KEY.length + keyBytes.length + Record.VALUE.length + valBytes.length];

		byte[] keyMagicBytes = FileOps.intToBytes(Record.KEY.length, keyBytes.length);
		System.arraycopy(keyMagicBytes, 0, bytes, 0, keyMagicBytes.length);
		
		byte[] valMagicBytes = FileOps.intToBytes(Record.VALUE.length, valBytes.length);
		System.arraycopy(valMagicBytes, 0, bytes, keyBytes.length + Record.KEY.length, valMagicBytes.length);

		return bytes;
	}
}
