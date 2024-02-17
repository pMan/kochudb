package com.kochudb.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

import com.kochudb.k.Record;
import com.kochudb.types.ByteArray;
import com.kochudb.types.LSMTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileIO {

	private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

	/**
	 * Write the map of key to offset information to file. Generated index is not sparse.
	 *
	 * @param filename    name of Index file
	 * @param keyToOffset String key and corresponding offset in the data file
	 * @throws IOException IOException
	 */
	public static void writeIndexFile(String filename, Map<ByteArray, Long> keyToOffset) {
		logger.debug("Creating index file: {}", filename);

		try (RandomAccessFile indexFile = new RandomAccessFile(filename, "rw")) {

			for (Map.Entry<ByteArray, Long> entry: keyToOffset.entrySet()) {

				byte[] keyBytes = entry.getKey().getBytes();
				Long value = entry.getValue();

				byte[] offsetBytes = FileIO.longToBytes(value);
				byte[] recWithSize = new byte[keyBytes.length + 9];

				recWithSize[0] = (byte) keyBytes.length;

				System.arraycopy(keyBytes, 0, recWithSize, 1, keyBytes.length);
				System.arraycopy(offsetBytes, 0, recWithSize, 1 + keyBytes.length, Long.BYTES);

				indexFile.seek(indexFile.length());
				indexFile.write(recWithSize);
			}

			indexFile.getFD().sync();
		} catch (FileNotFoundException fnfe) {
			logger.error("File not found: {}", filename);
			fnfe.printStackTrace();
		} catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

	/**
	 * Reads a .index file to a TreeMap and return it. Index file contains a string
	 * key and its offset in the corresponding data file.
	 *
	 * @param filename path to index file
	 * @return TreeMap
	 */
	public static Map<ByteArray, Long> readIndexFile(String filename) {
		logger.debug("Reading index file: {}", filename);

		Map<ByteArray, Long> keyToOffset = new TreeMap<>();

		try (RandomAccessFile raf = new RandomAccessFile(filename, "r")) {
			raf.seek(0);
			while (raf.getFilePointer() < raf.length()) {
				// key
				byte[] key = new byte[raf.read()];
				raf.read(key, 0, key.length);

				// offset where the key resides
				byte[] nextEightBytes = new byte[8];
				raf.read(nextEightBytes, 0, 8);
				long offset = FileIO.bytesToLong(nextEightBytes);

				keyToOffset.put(new ByteArray(key), offset);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return keyToOffset;
	}

	/**
	 * Prepend size of data to the data and append it to the file.
	 *
	 * @param dataFile RAF object
	 * @param data     data as byte array
	 * @param recordType     represents whether the byte[] passed is key or value
	 * @return the offset at which the data was written
	 * @throws IOException IOException
	 */
	public static long appendData(RandomAccessFile dataFile, byte[] data, Record recordType) throws IOException {
		long offset = dataFile.length();
		int headerLen = recordType.length;
		byte[] bytes = new byte[headerLen + data.length];

		dataFile.seek(offset);

		byte[] headerArray = FileIO.intToBytes(headerLen, data.length);
		System.arraycopy(headerArray, 0, bytes, 0, headerLen);

		System.arraycopy(data, 0, bytes, headerLen, data.length);
		dataFile.write(bytes);

		return offset;
	}

	/**
	 * Read current object from the given offset. Object is either key or value.
	 *
	 * @param raf    file to read
	 * @param offset offset at which current data is stored
	 * @param objectType type of the object to read - key or value
	 * @return value in byte[]
	 * @throws IOException IOException
	 */
	public static byte[] readObject(RandomAccessFile raf, Long offset, Record objectType) throws IOException {

		raf.seek(offset);

		byte[] header = new byte[objectType.length];
		raf.read(header, 0, objectType.length);
		int lenOfData = FileIO.bytesToInt(header);

		byte[] data = new byte[lenOfData];
		raf.read(data, 0, lenOfData);

		return data;
	}

	/**
	 * Find all files from the given level.
	 * 
	 * @param dataDirectory parent directory
	 * @param level level of level com[action
	 * @return File[] files
	 */
	public static File[] findFiles(String dataDirectory, int level) {
		File dir = new File(dataDirectory);

        return dir.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir1, String name) {
				return name.matches("^l" + level + "_[0-9]+\\.[0-9]+\\.idx$");
			}
		});
	}
	
	public static RandomAccessFile createDatFromIdx(String idxFile) {
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(idxFile.replaceFirst(".(idx|idxtmp)$", ".dat"), "r");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return raf;
	}
	
	public static String[] createNewIdxAndDataFilenames(int level) {
		String newFilename = FileIO.generateFilename();
		String newIdxFilename = newFilename + ".idx";
		
		File newIdxFile = new File(newIdxFilename.replaceAll("([0-9]+.[0-9]+)(?=.idx)", "l" + level + "_$1"));
		newIdxFilename = newIdxFile.getAbsolutePath();
		String newDatFilename = newIdxFile.getAbsolutePath().replace(".idx", ".dat");
		
		return new String[] {newIdxFilename, newDatFilename};
	}
	
	/**
	 * Filename generator
	 *
	 * @return filename (canonical path) without extension
	 * @throws IOException
	 */
	public static String generateFilename() {
		Instant instant = Instant.now();
		return (LSMTree.dataDir.getAbsolutePath()) + "/"
				+ instant.getEpochSecond() + "." + instant.getNano();
	}


	/**
	 * convert a byte[] to int.
	 * This function does not expect a byte[] longer that 4 bytes
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
	 * @param in integer
	 * @return byte[]
	 */
	public static byte[] intToBytes(int resultLen, int in) {
		byte[] bytes = new byte[resultLen];
		for (int i = 0; i < resultLen; i++) {
			int cur = resultLen - i - 1;
			bytes[i] = (byte)((in & 0xFF) >> (cur * 8));
		}
		return bytes;
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
	 * Compress byte[]
	 * 
	 * @param bytes byte[] to compress
	 * @return byte[]
	 * @throws IOException
	 */
	public static byte[] compress(byte[] bytes) throws IOException {
		return bytes;
	}

	/**
	 * Decompress byte[]
	 * 
	 * @param bytes byte[] to decompress
	 * @return byte[]
	 * @throws IOException
	 */
	public static byte[] decompress(byte[] bytes) throws IOException {
		return bytes;
	}

}
