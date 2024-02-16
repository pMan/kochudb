package com.kochudb.types;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.io.FileIO;
import com.kochudb.k.K;
import com.kochudb.k.Record;

/**
 * MemTable and SSTable operations
 */

public class SSTable {
	
	private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

	// where data and index files are stored
	String basePath;

	// files marked for deletion during a compaction process
	public static Queue<File> markedForDeletion;
	
	/**
	 * Constructor
	 * @param dataDirectory data directory
	 */
	public SSTable(File dataDirectory) {
		
		try {
			basePath = dataDirectory.getCanonicalPath();
		} catch (IOException e) {
			logger.error("Write to directory failed: {}", dataDirectory);
			e.printStackTrace();
			System.exit(K.ERR_NO_DATA_DIR);
		}
		
		markedForDeletion = new ConcurrentLinkedQueue<File>();
	}

	/**
	 * Search data files for a given key, level by level, starting at level 0
	 * 
	 * @param key search key
	 * @return value for the key
	 */
	public byte[] search(ByteArray key) {
		int level = 0;
		
		// sorted newest first
		File[] indexFiles = FileIO.findFiles(basePath, level);
		Arrays.sort(indexFiles, Comparator.comparingLong(File::lastModified));
		
		while (indexFiles.length > 0 || level <= K.NUM_LEVELS) {
			logger.debug("Seraching key in level {}", level);
			
			for (File indexFile : indexFiles) {
				if (markedForDeletion.contains(indexFile))
					continue;
				
				try {
					Map<ByteArray, Long> curIndex = readIndexFile(indexFile.getAbsolutePath());

					if (curIndex.containsKey(key)) {
						Long offset = curIndex.get(key);
						
						String dataFilename = indexFile.getAbsolutePath().replace(".idx", ".dat");
						RandomAccessFile raf = new RandomAccessFile(dataFilename, "r");
						
						byte[] keyData = readObject(raf, offset, Record.KEY);
						byte[] value = readObject(raf, offset + keyData.length + Record.KEY.length, Record.VALUE);
						
						return FileIO.decompress(value);
					}
				} catch (IOException e) {
					logger.warn("Error reading data: {}", e.getMessage());
					e.printStackTrace();
				}
			}
			level++;
			indexFiles = FileIO.findFiles(basePath, level);
			Arrays.sort(indexFiles, Comparator.comparingLong(File::lastModified));
		}
		logger.debug("Key not found");
		return new byte[] {};
	}

	/**
	 * Write the map of key to offset information to file. Generated index is not sparse.
	 * 
	 * @param filename    name of Index file
	 * @param keyToOffset String key and corresponding offset in the data file
	 * @throws IOException
	 */
	public static void writeIndexFile(String filename, Map<ByteArray, Long> keyToOffset) throws IOException {
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
		}
	}

	/**
	 * Reads a .index file to a TreeMap and return it. Index file contains a string
	 * key and its offset in the corresponding data file.
	 * 
	 * @param filename path to index file
	 * @return TreeMap
	 * @throws IOException
	 * @throws ClassNotFoundException
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
		} catch (FileNotFoundException e) {
			e.printStackTrace();
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
	 * @throws IOException
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
	 * @throws IOException
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
}