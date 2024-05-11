package com.kochudb.storage;

import static com.kochudb.k.K.LEVEL_MAX_SIZE_MULTIPLIER;
import static com.kochudb.k.K.LEVEL_ZERO_FILE_MAX_SIZE_KB;
import static com.kochudb.utils.FileUtil.createDatFromIdx;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.k.K;
import com.kochudb.types.ByteArray;
import com.kochudb.types.KeyValuePair;
import com.kochudb.utils.FileUtil;

/**
 * MemTable and SSTable operations
 */

public class SSTable {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    // where data and index files are stored
    private static String basePath;

    /**
     * Constructor
     * 
     * @param dataDirectory data directory
     */
    public SSTable(File dataDirectory) {
        try {
            basePath = dataDirectory.getCanonicalPath();
        } catch (IOException e) {
            logger.error("Failed to read data dir: {}", dataDirectory);
            System.exit(K.ERR_NO_DATA_DIR);
        }

        LSMTree.markedForDeletion = new ConcurrentLinkedQueue<>();

        LSMTree.filesToRename = new ArrayList<String>();
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
        File[] indexFiles = FileUtil.findFiles(basePath, level);

        while (indexFiles.length > 0 || level <= K.NUM_LEVELS) {
            logger.debug("Searching key in level {}", level);

            for (File indexFile : indexFiles) {
                if (LSMTree.markedForDeletion.contains(indexFile))
                    continue;

                try {
                    Segment segment = new Segment(level, indexFile.getAbsolutePath());
                    Map<ByteArray, Long> curIndex = segment.parseIndexFile();

                    if (curIndex.containsKey(key)) {
                        KeyValuePair record = segment.readKVPair(curIndex.get(key));
                        return record.value();
                    }
                } catch (IOException e) {
                    logger.warn("Error reading data: {}", e.getMessage());
                    e.printStackTrace();
                    return new ByteArray();
                }
            }
            level++;
            indexFiles = FileUtil.findFiles(basePath, level);
        }
        logger.debug("Key not found");
        return new ByteArray();
    }

    /**
     * compute max file size in the given level
     * 
     * @param level
     * @return
     */
    private long computeMaxFileSizeInLevel(int level) {
        if (level > 0)
            return computeMaxFileSizeInLevel(level - 1) * LEVEL_MAX_SIZE_MULTIPLIER;
        return 1024 * LEVEL_ZERO_FILE_MAX_SIZE_KB; // 4 kb;
    }

    /**
     * get the most recent segment at the given level
     * 
     * @param level
     * @return
     */
    public static Segment getMostRecentSegment(int level) {
        File[] files = FileUtil.findFiles(basePath, level);
        if (files.length == 0)
            return createNewSegment(level);

        File mostRecent = files[files.length - 1];
        return new Segment(level, mostRecent.getAbsolutePath());
    }

    /**
     * create a segment file at given level
     * 
     * @param level
     * @return
     */
    public static Segment createNewSegment(int level) {
        String[] newFileNames = FileUtil.createNewIdxAndDataFilenames(level);
        if (level > 0)
            newFileNames[0] = newFileNames[0].replaceFirst(".idx$", ".idxtmp");

        return new Segment(level, newFileNames[0], newFileNames[1]);

    }
}