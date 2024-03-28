package com.kochudb.storage;

import static com.kochudb.k.Record.KEY;
import static com.kochudb.k.Record.VALUE;
import static com.kochudb.utils.ByteUtil.bytesToInt;
import static com.kochudb.utils.ByteUtil.bytesToLong;
import static com.kochudb.utils.ByteUtil.longToBytes;
import static com.kochudb.utils.FileUtil.createDatFromIdx;
import static com.kochudb.utils.FileUtil.createNewIdxAndDataFilenames;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.types.ByteArray;
import com.kochudb.types.KeyValuePair;


public class SSTSegment {

	private final int level;
	private String indexFilePath;
	private final String datafilePath;
    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

	public SSTSegment(int level) {
		this.level = level;
		
		String[] filenames = createNewIdxAndDataFilenames(this.level);
		this.indexFilePath = filenames[0];
		this.datafilePath = filenames[1];
	}
	
	public String[] getFilesNames() {
		return new String[] {indexFilePath, datafilePath};
	}
	
	public void persist(SkipList skipList) throws FileNotFoundException, IOException {

		Map<ByteArray, Long> keyToOffsetMap = new TreeMap<>();

		try (RandomAccessFile dataFile = new RandomAccessFile(datafilePath, "rw")) {
			Iterator<SkipListNode> iterator = skipList.iterator();

			while (iterator.hasNext()) {
				SkipListNode node = iterator.next();

				KeyValuePair kvPair = new KeyValuePair(node.getKey().serialize(), node.getValue().serialize());
				long offset = appendData(dataFile, kvPair.serialize());

				keyToOffsetMap.put(node.getKey(), offset);
			}

			writeIndexFile(indexFilePath, keyToOffsetMap);
			logger.debug("Data file created: {}", datafilePath);
			logger.debug("Index file created: {}", indexFilePath);
			
		} catch (Exception e) {
			logger.error("Flush failed. {}", e.getMessage());
			e.printStackTrace();
		}
	}
	
	public String mergeTwoFiles(File file1, File file2) {

		Map<ByteArray, Object[]> mergedMap = new TreeMap<>();

		// order of files is important. file1 was created earlier than file2
		for (File file : new File[] { file1, file2 }) {
			Map<ByteArray, Long> indexMap;
			try {
				indexMap = parseIndexFile(file.getCanonicalPath());
				for (Map.Entry<ByteArray, Long> entry : indexMap.entrySet()) {
					mergedMap.put(entry.getKey(), new Object[] { entry.getValue(), file });
				}
			} catch (IOException e) {
				logger.warn("Invalid file path {}", file.getPath());
				e.printStackTrace();
			}
		}

		logger.debug("Merged index files {}, {}", file1.getName(), file2.getName());

		indexFilePath = this.indexFilePath.replaceFirst(".idx$", ".idxtmp");
		
		Map<ByteArray, Long> updatedIdxMap = new HashMap<>();
		Map<File, RandomAccessFile> openedFiles = new HashMap<>();

		openedFiles.put(file1, createDatFromIdx(file1.getAbsolutePath()));
		openedFiles.put(file2, createDatFromIdx(file2.getAbsolutePath()));

		try (RandomAccessFile newDataFile = new RandomAccessFile(datafilePath, "rw")) {

			for (Entry<ByteArray, Object[]> entry: mergedMap.entrySet()) {
				File file = (File) entry.getValue()[1];
				Long offset = (Long) entry.getValue()[0];

				byte[] serializedKVPair = readKVPairBytes(openedFiles.get(file), offset);
				Long newOffset = appendData(newDataFile, serializedKVPair);

				updatedIdxMap.put(entry.getKey(), newOffset);
				// f.setLastModified(Instant.now().getEpochSecond() * 1000);
			}
			newDataFile.getFD().sync();

			for (RandomAccessFile file : openedFiles.values())
				file.close();

			if (!updatedIdxMap.isEmpty())
				writeIndexFile(indexFilePath, updatedIdxMap);

			logger.debug("New data file created: {}", datafilePath);
			logger.debug("New index file created: {}", indexFilePath);

			SSTable.markedForDeletion.add(file1);
			SSTable.markedForDeletion.add(file2);

			return indexFilePath;
		} catch (IOException e) {
			logger.warn("Compaction failed: {}", e.getMessage());
			e.printStackTrace();
		} finally {
			for (RandomAccessFile file: openedFiles.values()) {
				try { file.close(); } catch (IOException e) { }
			}
		}
		return null;
	}
	

    /**
     * Write the map of key to offset information to file. Generated index is not sparse.
     *
     * @param filename    name of Index file
     * @param keyToOffset String key and corresponding offset in the data file
     * @throws IOException IOException
     */
    public void writeIndexFile(String filename, Map<ByteArray, Long> keyToOffset) {
        logger.debug("Creating index file: {}", filename);

        try (RandomAccessFile indexFile = new RandomAccessFile(filename, "rw")) {

            for (Map.Entry<ByteArray, Long> entry: keyToOffset.entrySet()) {

                byte[] keyBytes = entry.getKey().serialize();
                Long value = entry.getValue();

                byte[] offsetBytes = longToBytes(value);
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
     * Append data to file and return the offset where it was written to.
     * 
     * @param dataFile file
     * @param data byte[] of data
     * @return offset
     * @throws IOException
     */
    public long appendData(RandomAccessFile dataFile, byte[] data) throws IOException {
    	long offset = dataFile.length();
    	dataFile.write(data);
    	return offset;
    }

    /**
     * Reads a .index file to a TreeMap and return it. Index file contains a string
     * key and its offset in the corresponding data file.
     *
     * @param filename path to index file
     * @return TreeMap
     */
    public Map<ByteArray, Long> parseIndexFile(String filename) {
        logger.debug("Reading index file: {}", filename);

        Map<ByteArray, Long> keyToOffset = new TreeMap<>();

        try (RandomAccessFile raf = new RandomAccessFile(filename, "r")) {
            raf.seek(0);
            while (raf.getFilePointer() < raf.length()) {
                // key
                byte[] key = new byte[raf.read()];
                raf.read(key, 0, key.length);

                // offset where the value resides
                byte[] nextEightBytes = new byte[Long.BYTES];
                raf.read(nextEightBytes, 0, Long.BYTES);
                long offset = bytesToLong(nextEightBytes);

                keyToOffset.put(new ByteArray(key), offset);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return keyToOffset;
    }
    

	/**
	 * read len number of bytes from the given offset
	 * 
	 * @param raf file to read from
	 * @param offset offset position
	 * @param len number of bytes to read
	 * @return byte array
	 * @throws IOException
	 */
	public byte[] readBytes(RandomAccessFile raf, Long offset, int len) throws IOException {
		raf.seek(offset);
		byte[] b = new byte[len];
		raf.read(b, 0, len);
		return b;
	}
	
	public KeyValuePair readKVPair(String dataFilename, Long offset) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(dataFilename, "r");

        int keyLen = bytesToInt(readBytes(raf, offset, KEY.length));
        offset += KEY.length;
        
        byte[] keyData = readBytes(raf, offset, keyLen);
        offset += keyLen;
        
        int valLen = bytesToInt(readBytes(raf, offset, VALUE.length));
        offset += VALUE.length;
        
        byte[] valueData = readBytes(raf, offset, valLen);
        
		return new KeyValuePair(keyData, valueData);
	}


	/**
	 * read the bytes corresponding to one serialized KVPair object
	 * 
	 * @param raf data file to read from
	 * @param offset offset position
	 * @return byte[] a serialized KVPair
	 * @throws IOException
	 */
	public byte[] readKVPairBytes(RandomAccessFile raf, Long offset) throws IOException {
        raf.seek(offset);

        byte[] keyHeader = readBytes(raf, offset, KEY.length);
        offset += KEY.length;
        
        int keyLen = bytesToInt(keyHeader);
        byte[] key = readBytes(raf, offset, keyLen);
        offset += keyLen;

        byte[] valHeader = readBytes(raf, offset, VALUE.length);
        offset += VALUE.length;
        
        int valLen = bytesToInt(valHeader);
        byte[] val = readBytes(raf, offset, valLen);
        
        byte[] obj = new byte[KEY.length + VALUE.length + keyLen + valLen];
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

}
