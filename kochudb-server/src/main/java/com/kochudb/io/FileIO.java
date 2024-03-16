package com.kochudb.io;

import static com.kochudb.k.Record.KEY;
import static com.kochudb.k.Record.VALUE;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.types.ByteArrayKey;
import com.kochudb.types.KVPair;
import com.kochudb.types.LSMTree;

public class FileIO {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Write the map of key to offset information to file. Generated index is not sparse.
     *
     * @param filename    name of Index file
     * @param keyToOffset String key and corresponding offset in the data file
     * @throws IOException IOException
     */
    public static void writeIndexFile(String filename, Map<ByteArrayKey, Long> keyToOffset) {
        logger.debug("Creating index file: {}", filename);

        try (RandomAccessFile indexFile = new RandomAccessFile(filename, "rw")) {

            for (Map.Entry<ByteArrayKey, Long> entry: keyToOffset.entrySet()) {

                byte[] keyBytes = entry.getKey().serialize();
                Long value = entry.getValue();

                byte[] offsetBytes = FileIO.longToBytes(value);
                byte[] recWithSize = new byte[keyBytes.length + KEY.length + Long.BYTES];

                recWithSize[0] = (byte) keyBytes.length;

                System.arraycopy(keyBytes, 0, recWithSize, KEY.length, keyBytes.length);
                System.arraycopy(offsetBytes, 0, recWithSize, KEY.length + keyBytes.length, Long.BYTES);

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
    public static Map<ByteArrayKey, Long> readIndexFile(String filename) {
        logger.debug("Reading index file: {}", filename);

        Map<ByteArrayKey, Long> keyToOffset = new TreeMap<>();

        try (RandomAccessFile raf = new RandomAccessFile(filename, "r")) {
            raf.seek(0);
            while (raf.getFilePointer() < raf.length()) {
                // key
                byte[] key = new byte[raf.read()];
                raf.read(key, 0, key.length);

                // offset where the value resides
                byte[] nextEightBytes = new byte[Long.BYTES];
                raf.read(nextEightBytes, 0, Long.BYTES);
                long offset = FileIO.bytesToLong(nextEightBytes);

                keyToOffset.put(new ByteArrayKey(key), offset);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return keyToOffset;
    }
    
    /**
     * Append data to file and return the offset where it was written to.
     * 
     * @param dataFile file
     * @param data byte[] of data
     * @return offset
     * @throws IOException
     */
    public static long appendData(RandomAccessFile dataFile, byte[] data) throws IOException {
    	long offset = dataFile.length();
    	dataFile.write(data);
    	return offset;
    }
    
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

	public static KVPair readKVPair(String dataFilename, Long offset) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(dataFilename, "r");

        int keyLen = bytesToInt(readBytes(raf, offset, KEY.length));
        offset += KEY.length;
        
        byte[] keyData = readBytes(raf, offset, keyLen);
        offset += keyLen;
        
        int valLen = bytesToInt(readBytes(raf, offset, VALUE.length));
        offset += VALUE.length;
        
        byte[] valueData = readBytes(raf, offset, valLen);
        
		return new KVPair(keyData, valueData);
	}

	public static byte[] readKVPairBytes(RandomAccessFile raf, Long offset) throws IOException {
        raf.seek(offset);

        byte[] keyHeader = new byte[KEY.length];
        raf.read(keyHeader, 0, KEY.length);
        int lenOfKey = FileIO.bytesToInt(keyHeader);

        byte[] key = new byte[lenOfKey];
        raf.read(key, 0, lenOfKey);

        byte[] valHeader = new byte[VALUE.length];
        raf.read(valHeader, 0, VALUE.length);
        int lenOfVal = FileIO.bytesToInt(valHeader);

        byte[] val = new byte[lenOfVal];
        raf.read(val, 0, lenOfVal);
        
        byte[] obj = new byte[KEY.length + VALUE.length + key.length + val.length];
        int curPos = 0;
        System.arraycopy(keyHeader, 0, obj, curPos, KEY.length);
        curPos += KEY.length;

        System.arraycopy(key, 0, obj, curPos, key.length);
        curPos += key.length;
        
        System.arraycopy(valHeader, 0, obj, curPos, VALUE.length);
        curPos += VALUE.length;

        System.arraycopy(val, 0, obj, curPos, val.length);
        
        return obj;
	}

	private static byte[] readBytes(RandomAccessFile raf, Long offset, int len) throws IOException {
		raf.seek(offset);
		byte[] b = new byte[len];
		raf.read(b, 0, len);
		return b;
	}
}
