package com.kochudb.types;

import com.kochudb.io.FileIO;
import com.kochudb.k.K;
import com.kochudb.k.Record;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
        
        markedForDeletion = new ConcurrentLinkedQueue<>();
    }

    /**
     * Search data files for a given key, level by level, starting at level 0
     * 
     * @param key search key
     * @return value for the key
     */
    public ByteArrayValue search(ByteArrayKey key) {
        int level = 0;
        
        // sorted newest first
        File[] indexFiles = FileIO.findFiles(basePath, level);
        Arrays.sort(indexFiles, Comparator.comparingLong(File::lastModified));
        
        while (indexFiles.length > 0 || level <= K.NUM_LEVELS) {
            logger.debug("Searching key in level {}", level);
            
            for (File indexFile : indexFiles) {
                if (markedForDeletion.contains(indexFile))
                    continue;
                
                try {
                    Map<ByteArrayKey, Long> curIndex = FileIO.readIndexFile(indexFile.getAbsolutePath());

                    if (curIndex.containsKey(key)) {
                        Long offset = curIndex.get(key);
                        
                        String dataFilename = indexFile.getAbsolutePath().replace(".idx", ".dat");
                        RandomAccessFile raf = new RandomAccessFile(dataFilename, "r");
                        
                        byte[] keyData = FileIO.readObject(raf, offset, Record.KEY);
                        byte[] value = FileIO.readObject(raf, offset + keyData.length + Record.KEY.length, Record.VALUE);
                        
                        return new ByteArrayValue(FileIO.decompress(value));
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
        return new ByteArrayValue();
    }
}