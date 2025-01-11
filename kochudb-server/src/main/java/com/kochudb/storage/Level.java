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
import com.kochudb.types.KochuDoc;
import com.kochudb.utils.ByteUtil;
import com.kochudb.utils.FileUtil;

public class Level {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private int level;
    private final LSMTree lsmTree;
    private List<SSTable> sSTables;
    private PriorityQueue<Object[]> keyValueHeap;

    public Level(int level, LSMTree tree) {
        this.level = level;
        this.lsmTree = tree;
        sSTables = new LinkedList<SSTable>();
        keyValueHeap = new PriorityQueue<Object[]>((first, second) -> {
            KochuDoc keyFirst = (KochuDoc) first[0];
            KochuDoc keySecond = (KochuDoc) second[0];

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
    public KochuDoc search(KochuDoc doc) {
        for (SSTable sSTable : sSTables) {
            KochuDoc indexDoc = sSTable.search(doc);
            if (indexDoc != null) {

                long offset = ByteUtil.bytesToLong(indexDoc.getValue().bytes());
                KochuDoc data = sSTable.readKochuDoc(offset);
                return data;
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
            SkipList skiplist = sSTable.parseIndex();
            Iterator<SkipListNode> iter = skiplist.iterator();
            while (iter.hasNext()) {
                SkipListNode node = iter.next();
                Object[] objArray = new Object[] { node.data, sSTable };
                keyValueHeap.offer(objArray);
            }
        }

        SkipList skipList = new SkipList();

        long maxFileSizeInLevel = computeMaxFileSizeInLevel(level + 1);
        while (!keyValueHeap.isEmpty()) {
            Object[] objArray = keyValueHeap.poll();
            KochuDoc doc = (KochuDoc) objArray[0];

            while (!keyValueHeap.isEmpty() && doc.compareTo((KochuDoc) keyValueHeap.peek()[0]) == 0) {
                objArray = keyValueHeap.poll();
                doc = (KochuDoc) objArray[0];
            }

            Long offset = ByteUtil.bytesToLong(((ByteArray) doc.getValue()).bytes());
            SSTable sSTable = (SSTable) objArray[1];

            KochuDoc dataDoc = sSTable.readKochuDoc(offset);
            skipList.put(dataDoc);

            if (skipList.size() >= maxFileSizeInLevel) {
                SSTable newSegment = new SSTable(level + 1);
                newSegment.persist(skipList);
                LSMTree.filesToRename.add(newSegment.getIndexFile());

                logger.debug("New data file created: {}", newSegment.getDataFile());
                skipList = new SkipList();
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
