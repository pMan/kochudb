package com.kochudb.tasks;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.io.FileIO;
import com.kochudb.k.K;
import com.kochudb.k.Record;
import com.kochudb.types.ByteArrayKey;
import com.kochudb.types.SSTable;

public class LevelCompactor implements Runnable {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());
    
    // Maximum size of file in level 0 in bytes
    long levelZeroFileSize;
    
    // Maximum number of files allowed in Level 0
    int levelZeroNumFiles;
    
    // Directory where data and index files are stored
    String dataDirectory;
    
    // Currently compaction is in progress?
    private static volatile AtomicBoolean isRunning;
    
    File dir;
    
    /**
     * Constructor
     * 
     * @param dir data directory
     */
    public LevelCompactor(File dir) {

        isRunning = new AtomicBoolean(false);

        levelZeroFileSize = 1024 * K.LEVEL_ZERO_FILE_MAX_SIZE_KB; // 4 kb
        
        levelZeroNumFiles = K.LEVEL_ZERO_NUM_FILES;

        this.dir = dir;
        
        try {
            dataDirectory = dir.getCanonicalPath();
        } catch (IOException e) {
            logger.error("Failed to access Data directory: {}", dataDirectory);
            System.exit(K.ERR_NO_DATA_DIR);
        }
        logger.info("Compaction thread initialized");
    }
    
    @Override
    public void run() {
        if (isRunning.get() || !isRunning.compareAndSet(false, true)) {
            logger.debug("Compaction thread is running");
            return;
        }
        
        logger.trace("Compaction thread started");
                
        compactLevel(0);
        
        if (isRunning.getAndSet(false))
            logger.trace("Compaction thread finished the job");
    }

    /**
     * Decide whether current level to be compacted. If yes, start compaction.
     *
     * @param level current level
     */
    void compactLevel(int level) {
        File[] files = FileIO.findFiles(dataDirectory, level);
        
        // oldest file first
        Arrays.sort(files, Comparator.comparingLong(File::lastModified));
        
        int allowedNumFilesInCurLevel = computeNumFilesInLevel(level);
        long maxFileSizeInLevel = computeMaxFileSizeInLevel(level);
        long maxTotalSizeInLevel = allowedNumFilesInCurLevel * maxFileSizeInLevel;
        long curTotalSizeInLevel = Arrays.stream(files).map(File::length).mapToLong(Long::longValue).sum();
        
        if (curTotalSizeInLevel > maxTotalSizeInLevel || files.length > allowedNumFilesInCurLevel) {
            logger.debug("Compaction started. current level: {}, number of files in level: {}", level, files.length);
            
            String bigTmpIdxFile = compactFiles(level, files, 0, files.length - 1);
            
            logger.trace("New compacted file created in level {}: {}", level, bigTmpIdxFile);
            
            // all files from cur level are moved to next level, safe to delete cur level files
            // delete all files marked for deletion
            deleteOutdatedFiles();
            
            // propagate to next level
            if (level < K.NUM_LEVELS)
                compactLevel(level + 1);
        } else {
            logger.trace("Did not meet all criteria to begin compaction in {}", level);
        }
    }
    
    /**
     * merge all SSTables - two adjacent files at a time
     * 
     * @param curLevel current level
     * @param files array of index files
     * @param start start position
     * @param end end position
     * @return absolute name of merged file
     */
    private String compactFiles(int curLevel, File[] files, int start, int end) {
        
        if (start == end)
            return files[start].getAbsolutePath();
        
        if (start == end - 1)
            return mergeTwoFiles(files[start], files[end], curLevel);
        
        int mid = start + (end - start)/2;
        
        String file1 = compactFiles(curLevel, files, start, mid);
        String file2 = compactFiles(curLevel, files, mid+1, end);
        
        return mergeTwoFiles(new File(file1), new File(file2), curLevel);
    }
    
    /**
     * merge tow SSTable into one
     * 
     * @param file1 first file
     * @param file2 second file
     * @param curLevel current level
     * @return absolute name of merged file
     */
    private String mergeTwoFiles(File file1, File file2, int curLevel) {

        Map<ByteArrayKey, Object[]> mergedMap = new TreeMap<>();
        
        // order of files is important. file1 was created earlier than file2
        for (File file: new File[] {file1, file2}) {
            Map<ByteArrayKey, Long> indexMap;
            try {
                indexMap = FileIO.readIndexFile(file.getCanonicalPath());
                for (Map.Entry<ByteArrayKey, Long> entry: indexMap.entrySet()) {
                    mergedMap.put(entry.getKey(), new Object[] {entry.getValue(), file});
                }
            } catch (IOException e) {
                logger.warn("Invalid file path {}", file.getPath());
                e.printStackTrace();
            }
        }
        
        logger.debug("Merged index files {}, {}", file1.getName(), file2.getName());
        
        String[] filenames = FileIO.createNewIdxAndDataFilenames(curLevel + 1);
        String newIdxFilename = filenames[0].replaceFirst(".idx$", ".idxtmp");
        String newDatFilename = filenames[1];
        
        try (RandomAccessFile newDatRaf = new RandomAccessFile(newDatFilename, "rw")) {
            
            logger.debug("New Temp file name: {}", newIdxFilename);

            Map<ByteArrayKey, Long> updatedIdxMap = new HashMap<>();
            Map<File, RandomAccessFile> openedFiles = new HashMap<>();
            
            openedFiles.put(file1, FileIO.createDatFromIdx(file1.getAbsolutePath()));
            openedFiles.put(file2, FileIO.createDatFromIdx(file2.getAbsolutePath()));
            
            logger.debug("Writing to merged data file: {}", newDatFilename);
            
            for (Entry<ByteArrayKey, Object[]> entry: mergedMap.entrySet()) {
                ByteArrayKey key = entry.getKey();
                File file =  (File) entry.getValue()[1];
                Long offset =  (Long) entry.getValue()[0];
    
                byte[] keyBytes = FileIO.readObject(openedFiles.get(file), offset, Record.KEY);
                byte[] valueBytes = FileIO.readObject(openedFiles.get(file), (offset + keyBytes.length + Record.KEY.length), Record.VALUE);
                
                Long newOffset = FileIO.appendData(newDatRaf, keyBytes, Record.KEY);
                FileIO.appendData(newDatRaf, valueBytes, Record.VALUE);
                
                updatedIdxMap.put(key, newOffset);
                //f.setLastModified(Instant.now().getEpochSecond() * 1000);
            }
            newDatRaf.getFD().sync();
            newDatRaf.close();
            
            for (RandomAccessFile file: openedFiles.values())
                file.close();
            
            if (!updatedIdxMap.isEmpty())
                FileIO.writeIndexFile(newIdxFilename, updatedIdxMap);
            
            logger.debug("New data file created: {}", newDatFilename);
            logger.debug("New index file created: {}", newIdxFilename);
            
            SSTable.markedForDeletion.add(file1);
            SSTable.markedForDeletion.add(file2);
            
            return newIdxFilename;
        } catch (IOException e) {
            logger.warn("Compaction failed: {}", e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    /**
     * files that are compacted into bigger files are periodically deleted 
     */
    private void deleteOutdatedFiles() {

        for (File file : SSTable.markedForDeletion) {
            
            String dataFilename = file.getAbsolutePath().replaceFirst(".(idx|idxtmp)$", ".dat");
            File datafile = new File(dataFilename);

            logger.debug(file.delete() ? "File deleted: {}" : "Failed to delete file: {}", file.getAbsolutePath());
            logger.debug(datafile.delete() ? "File deleted: {}" : "Failed to delete file: {}", dataFilename);
        }
        
        SSTable.markedForDeletion.clear();
    }
    
    /**
     * Compute the maximum size a file is allowed to have in the given level
     * This is computed from the configuration properties.
     * Number of files in the given level is equal to the number of files in prev level multiplied by
     * the multiplier. At level 0, it's configured as 10.
     * 
     * @param level current level
     * @return max file size in current level
     */
    private long computeMaxFileSizeInLevel(int level) {
        if (level > 0)
            return computeMaxFileSizeInLevel(level - 1) * K.LEVEL_MAX_SIZE_MULTIPLIER;
        return levelZeroFileSize;
    }
    
    /**
     * computer number of files in the given level
     * This is computed from the number of files in the prev level multiplied by a configured multiplier.
     * Multiplier is configured for level 0.
     * 
     * @param level - level of level compaction
     * @return number of files in level
     */
    private int computeNumFilesInLevel(int level) {
        if (level > 0)
            return computeNumFilesInLevel(level - 1) * K.LEVEL_MAX_FILES_MULTIPLIER;
        return levelZeroNumFiles;
    }
}
