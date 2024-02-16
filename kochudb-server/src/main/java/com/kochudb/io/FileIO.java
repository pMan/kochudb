package com.kochudb.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Instant;
import java.util.Arrays;

import com.kochudb.types.LSMTree;

public class FileIO {

	/**
	 * find all files identified by a matching file name pattern from the given dir
	 * 
	 * @param dir directory
	 * @param endsWith ends with
	 * @return File[]
	 */
	public static File[] getFiles(String dir, String pattern) {
		File[] files = new File(dir).listFiles(pathname -> {
			return pathname.getName().matches(pattern);
		});
		// newest first
		Arrays.sort(files, (a, b) -> b.compareTo(a));
		return files;
	}

	/**
	 * Find all files from the given level.
	 * 
	 * @param dataDir parent directory
	 * @param level level of level com[action
	 * @return File[] files
	 */
	public static File[] findFiles(String dataDirectory, int level) {
		File dir = new File(dataDirectory);
		File[] files = dir.listFiles(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				return name.matches("^l" + level + "_[0-9]+\\.[0-9]+\\.idx$");
			}
		});
		
		return files;
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
	public static final String generateFilename() {
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
	public static final int bytesToInt(byte[] b) {
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
	public static final byte[] intToBytes(int resultLen, int in) {
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
	 * @param long
	 * @return bytes
	 */
	public static final byte[] longToBytes(Long lo) {
		byte[] bytes = new byte[Long.BYTES];
		int length = bytes.length;
		for (int i = 0; i < length; i++) {
		    bytes[length - i - 1] = (byte) (lo & 0xFF);
		    lo >>= 8;
		}
		return bytes;
	}

	/**
	 * Convert a byte[] of size 8 into a Long
	 * 
	 * @param bytes byte[]
	 */
	public static final long bytesToLong(final byte[] bytes) {
		long value = 0;
		for (int i = 0; i < Long.BYTES; i++) {
		    value = (value << 8) + (bytes[i] & 0xFF);
		}
		return value;
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
	 * @param input byte[] to decompress
	 * @return byte[]
	 * @throws IOException
	 */
	public static byte[] decompress(byte[] input) throws IOException {
		return input;
	}

}
