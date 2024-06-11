package com.kochudb.storage;

import static com.kochudb.k.K.LEVEL_MAX_SIZE_MULTIPLIER;
import static com.kochudb.k.K.LEVEL_ZERO_FILE_MAX_SIZE_KB;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.types.ByteArray;
import com.kochudb.utils.FileUtil;

public class Level {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private int level;
    private List<SSTable> sSTables;
    private PriorityQueue<Object[]> keyValueHeap;

    public Level(int level) {
        this.level = level;
        sSTables = new LinkedList<SSTable>();
        keyValueHeap = new PriorityQueue<Object[]>((first, second) -> {
            ByteArray keyFirst = (ByteArray) first[0];
            ByteArray keySecond = (ByteArray) second[0];

            if (keyFirst.compareTo(keySecond) == 0) {
                SSTable segFirst = (SSTable) first[2];
                SSTable segSecond = (SSTable) second[2];
                return segFirst.getIndexFile().compareTo(segSecond.getIndexFile());
            }
            return keyFirst.compareTo(keySecond);
        });

        List<File> files = Arrays.asList(FileUtil.findFiles(LSMTree.dataDir.getAbsolutePath(), level));
        for (File file : files)
            sSTables.add(new SSTable(level, file.getAbsolutePath()));
    }

    public int getLevel() {
        return this.level;
    }

    public List<SSTable> getSegments() {
        return sSTables;
    }

    public ByteArray search(ByteArray key) {
        for (SSTable sSTable : sSTables) {
            // introduce iterator
            Map<ByteArray, Long> map = sSTable.parseIndex();

            if (map.containsKey(key)) {
                Long offset = map.get(key);
                return sSTable.readRecord(offset).value();
            }
        }
        return null;
    }

    /**
     * compact all segments in given level to a merged segment in next higher level
     * 
     * @param level
     * @throws IOException
     */
    public void compactLevel() throws IOException {
        // Map<Segment, RandomAccessFile> openedFiles = new HashMap<>();
        for (SSTable sSTable : sSTables) {
            for (Map.Entry<ByteArray, Long> indexData : sSTable.parseIndex().entrySet()) {
                Object[] objArray = new Object[] { indexData.getKey(), indexData.getValue(), sSTable };
                keyValueHeap.offer(objArray);
            }
            // openedFiles.put(segment, createDatFromIdx(segment.getIndexFile()));
        }

        SSTable newSegment = new SSTable(level + 1);
        Map<ByteArray, Long> updatedIdxMap = newSegment.parseIndex();
        RandomAccessFile curDataFile = newSegment.openDataFileForWrite();

        long curSize = 0, maxFileSizeInLevel = computeMaxFileSizeInLevel(level + 1);
        while (!keyValueHeap.isEmpty()) {
            Object[] objArray = keyValueHeap.poll();
            ByteArray key = (ByteArray) objArray[0];
            
            while (!keyValueHeap.isEmpty() && (key).compareTo((ByteArray) keyValueHeap.peek()[0]) == 0) {
                objArray = keyValueHeap.poll();
                key = (ByteArray) objArray[0];
            }

            Long offset = (Long) objArray[1];
            SSTable sSTable = (SSTable) objArray[2];

            //byte[] serializedKVPair = sSTable.readKVPairBytes(offset);
            byte[] serializedRecord = sSTable.readRecord(offset).serialize();
            Long curOffset = newSegment.appendData(curDataFile, serializedRecord);
            updatedIdxMap.put(key, curOffset);
            curSize += serializedRecord.length;

            if (curSize >= maxFileSizeInLevel) {
                newSegment.saveIndex(updatedIdxMap);
                curDataFile.close();
                logger.debug("New data file created: {}", newSegment.getDataFile());

                LSMTree.filesToRename.add(newSegment.getIndexFile());
                newSegment = new SSTable(level + 1);
                curDataFile = newSegment.openDataFileForWrite();

                updatedIdxMap.clear();
                curSize = 0;
            }
            // f.setLastModified(Instant.now().getEpochSecond() * 1000);
        }
        curDataFile.getFD().sync();
        curDataFile.close();

        if (!updatedIdxMap.isEmpty()) {
            newSegment.saveIndex(updatedIdxMap);
            LSMTree.filesToRename.add(newSegment.getIndexFile());

            logger.debug("New data file created: {}", newSegment.getIndexFile());
            logger.debug("New index file created: {}", newSegment.getDataFile());
        }

        renameIndexFiles();
        deleteCompactedFiles();
    }

    public void insert(SSTable sSTable) {
        this.sSTables.add(0, sSTable);
    }

    private void renameIndexFiles() {
        for (String tempName : LSMTree.filesToRename) {
            FileUtil.renameIndexFile(tempName);
        }
        LSMTree.filesToRename.clear();
    }

    /**
     * files that are compacted into bigger files are periodically deleted
     */
    private void deleteCompactedFiles() {
        for (SSTable s : sSTables) {
            File datafile = new File(s.getDataFile());
            File indexfile = new File(s.getIndexFile());

            logger.debug(indexfile.delete() ? "File deleted: {}" : "Failed to delete file: {}",
                    indexfile.getAbsolutePath());
            logger.debug(datafile.delete() ? "File deleted: {}" : "Failed to delete file: {}",
                    datafile.getAbsolutePath());
        }
    }

    private static long computeMaxFileSizeInLevel(int level) {
        if (level > 0)
            return computeMaxFileSizeInLevel(level - 1) * LEVEL_MAX_SIZE_MULTIPLIER;
        return 1024 * LEVEL_ZERO_FILE_MAX_SIZE_KB; // 4 kb;
    }

    public SSTable getLatestSegment(int level) {
        if (sSTables.size() == 0)
            return new SSTable(level);

        return sSTables.get(sSTables.size() - 1);
    }
}
