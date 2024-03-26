package com.kochudb.types;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.io.FileIO;
import com.kochudb.k.K;

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
            logger.error("Failed to read data dir: {}", dataDirectory);
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
    public ByteArray search(ByteArray key) {
        int level = 0;
        
        // sorted newest first
        File[] indexFiles = FileIO.findFiles(basePath, level);
        
        while (indexFiles.length > 0 || level <= K.NUM_LEVELS) {
            logger.debug("Searching key in level {}", level);
            
            for (File indexFile : indexFiles) {
                if (markedForDeletion.contains(indexFile))
                    continue;
                
                try {
                	String absFilePath = indexFile.getAbsolutePath();
                    Map<ByteArray, Long> curIndex = FileIO.readIndexFile(absFilePath);

                    if (curIndex.containsKey(key)) {
                        KVPair record = FileIO.readKVPair(absFilePath.replace(".idx", ".dat"), curIndex.get(key));
                        return record.val();
                    }
                } catch (IOException e) {
                    logger.warn("Error reading data: {}", e.getMessage());
                    e.printStackTrace();
                    return new ByteArray();
                }
            }
            level++;
            indexFiles = FileIO.findFiles(basePath, level);
        }
        logger.debug("Key not found");
        return new ByteArray();
    }
}