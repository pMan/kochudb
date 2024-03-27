package com.kochudb.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.types.LSMTree;

public class FileIO {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Find all files from the given level, sorted oldest to newest.
     * 
     * @param dataDirectory parent directory
     * @param level level of LSM Tree
     * @return File[] files
     */
	public static File[] findFiles(String dataDirectory, int level) {
		return findFiles(dataDirectory, level, Comparator.comparingLong(File::lastModified));
	}
	
	/**
	 * Find all files from the given level, sorted by comparator
	 * 
	 * @param dataDirectory directory where files reside
	 * @param level level of LSM Tree
	 * @param comparator comparator to sort files
	 * @return File[] files
	 */
	public static File[] findFiles(String dataDirectory, int level, Comparator<File> comparator) {
		File dir = new File(dataDirectory);

		File[] files = dir.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir1, String name) {
				return name.matches("^l" + level + "_[0-9]+\\.[0-9]+\\.idx$");
			}
		});

		Arrays.sort(files, comparator);
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
    
    /**
     * create file names for the given level
     * @param level
     * @return
     */
    public static String[] createNewIdxAndDataFilenames(int level) {
        String newFilename = FileIO.generateFilename();
        String newIdxFilename = newFilename + ".idx";
        
        File newIdxFile = new File(newIdxFilename.replaceAll("([0-9]+.[0-9]+)(?=.idx)", "l" + level + "_$1"));
        newIdxFilename = newIdxFile.getAbsolutePath();
        String newDatFilename = newIdxFile.getAbsolutePath().replace(".idx", ".dat");
        
        return new String[] {newIdxFilename, newDatFilename};
    }
    
    /**
	 * rename an index file (.idx or .idxtmp) t0 data file
	 * 
	 * @param toRename absolute file path
	 * @return absolute file path
	 */
	public static String renameIndexFile(String toRename) {

		String newName = toRename.replaceFirst(".idxtmp$", ".idx");
		
		if (new File(toRename).renameTo(new File(newName)))
			logger.debug("Index file renamed to " + newName);
		else
			logger.error("Failed to rename inedx file");
		
		return newName;
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

}
