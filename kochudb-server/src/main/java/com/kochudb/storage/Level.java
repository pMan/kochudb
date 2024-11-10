package com.kochudb.storage;

import static com.kochudb.k.K.LEVEL_MAX_SIZE_MULTIPLIER;
import static com.kochudb.k.K.LEVEL_ZERO_FILE_MAX_SIZE_KB;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.types.ByteArray;
import com.kochudb.types.Record;
import com.kochudb.utils.ByteUtil;
import com.kochudb.utils.FileUtil;

public class Level {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private int level;
    private final LSMTree<?, ?> lsmTree;
    private List<SSTable> sSTables;
    private PriorityQueue<Object[]> keyValueHeap;

    public Level(int level, LSMTree<?, ?> tree) {
        this.level = level;
        this.lsmTree = tree;
        sSTables = new LinkedList<SSTable>();
        keyValueHeap = new PriorityQueue<Object[]>((first, second) -> {
            Record keyFirst = (Record) first[0];
            Record keySecond = (Record) second[0];

            if (keyFirst.compareTo(keySecond) == 0) {
                SSTable segFirst = (SSTable) first[1];
                SSTable segSecond = (SSTable) second[1];
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

    /**
     * search all SSTables in this level
     * 
     * @param key
     * @return ByteArray value
     */
    public ByteArray search(ByteArray key) {
        for (SSTable sSTable : sSTables) {
            Record indexRecord = sSTable.search(key);
            if (indexRecord != null) {

                long offset = ByteUtil.bytesToLong(indexRecord.value().bytes());
                Record data = sSTable.readRecord(offset);
                return data.value();
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
        for (SSTable sSTable : sSTables) {
            SkipList<Record> skiplist = sSTable.parseIndex();
            Iterator<SkipListNode<Record>> iter = skiplist.iterator();
            while (iter.hasNext()) {
                SkipListNode<Record> node = iter.next();
                Object[] objArray = new Object[] { node.data, sSTable };
                keyValueHeap.offer(objArray);
            }
        }

        SkipList<Record> skipList = new SkipList<Record>();

        long maxFileSizeInLevel = computeMaxFileSizeInLevel(level + 1);
        while (!keyValueHeap.isEmpty()) {
            Object[] objArray = keyValueHeap.poll();
            Record record = (Record) objArray[0];

            while (!keyValueHeap.isEmpty() && record.compareTo((Record) keyValueHeap.peek()[0]) == 0) {
                objArray = keyValueHeap.poll();
                record = (Record) objArray[0];
            }

            Long offset = ByteUtil.bytesToLong(((ByteArray) record.value()).bytes());
            SSTable sSTable = (SSTable) objArray[1];

            Record record2 = sSTable.readRecord(offset);
            skipList.put(record2);

            if (skipList.size() >= maxFileSizeInLevel) {
                SSTable newSegment = new SSTable(level + 1);
                newSegment.persist(skipList);
                LSMTree.filesToRename.add(newSegment.getIndexFile());

                logger.debug("New data file created: {}", newSegment.getDataFile());
                skipList = new SkipList<Record>();
            }
        }

        if (skipList.length() > 0) {
            SSTable ssTable = new SSTable(level + 1);
            ssTable.persist(skipList);
            LSMTree.filesToRename.add(ssTable.getIndexFile());

            logger.debug("New data file created: {}", ssTable.getIndexFile());
            logger.debug("New index file created: {}", ssTable.getDataFile());
        }

        renameIndexFiles();
        deleteCompactedFiles();
        lsmTree.updateLevels();
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
        return 1024 * LEVEL_ZERO_FILE_MAX_SIZE_KB; // 4 kb
    }
}
