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

    private List<Segment> segments = new LinkedList<Segment>();

    public Level(int level) {
        this.level = level;

        List<File> files = Arrays.asList(FileUtil.findFiles(LSMTree.dataDir.getAbsolutePath(), level));
        for (File file : files)
            segments.add(new Segment(level, file.getAbsolutePath()));
    }

    public int getLevel() {
        return this.level;
    }

    public List<Segment> getSegments() {
        return segments;
    }

    public ByteArray search(ByteArray key) throws IOException {
        for (Segment segment : segments) {
            // introduce iterator
            Map<ByteArray, Long> map = segment.parseIndexFile();

            if (map.containsKey(key)) {
                Long offset = map.get(key);
                return segment.readKVPair(offset).value();
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
        PriorityQueue<Object[]> keyValueHeap = new PriorityQueue<Object[]>((first, second) -> {
            ByteArray keyFirst = (ByteArray) first[0];
            ByteArray keySecond = (ByteArray) second[0];

            if (keyFirst.compareTo(keySecond) == 0) {
                Segment segFirst = (Segment) first[2];
                Segment segSecond = (Segment) second[2];
                return segFirst.getIndexFile().compareTo(segSecond.getIndexFile());
            }
            return keyFirst.compareTo(keySecond);
        });

        // Map<Segment, RandomAccessFile> openedFiles = new HashMap<>();

        for (Segment segment : segments) {
            for (Map.Entry<ByteArray, Long> indexData : segment.parseIndexFile().entrySet()) {
                Object[] objArray = new Object[] { indexData.getKey(), indexData.getValue(), segment };
                keyValueHeap.offer(objArray);
            }
            // openedFiles.put(segment, createDatFromIdx(segment.getIndexFile()));
        }

        // Segment newSegment = SSTable.createNewSegment(level + 1);
        /* Segment newSegment = SSTable.getMostRecentSegment(level + 1); */
        Level nextLevel = new Level(level + 1);
        List<Segment> segs = nextLevel.segments;
        Segment newSegment = segs.isEmpty() ? new Segment(level + 1) : segs.get(segs.size() - 1);
        Map<ByteArray, Long> updatedIdxMap = newSegment.parseIndexFile();
        RandomAccessFile curDataFile = newSegment.openDataFileForWrite();

        long curSize = 0, maxFileSizeInLevel = computeMaxFileSizeInLevel(level + 1);

        while (!keyValueHeap.isEmpty()) {
            Object[] objArray = keyValueHeap.poll();
            while (!keyValueHeap.isEmpty()
                    && ((ByteArray) objArray[0]).compareTo(((ByteArray) keyValueHeap.peek()[0])) == 0)
                objArray = keyValueHeap.poll();

            ByteArray key = (ByteArray) objArray[0];
            Long offset = (Long) objArray[1];
            Segment segment = (Segment) objArray[2];

            byte[] serializedKVPair = segment.readKVPairBytes(offset);
            Long curOffset = newSegment.appendData(curDataFile, serializedKVPair);

            updatedIdxMap.put(key, curOffset);
            curSize += serializedKVPair.length;

            if (curSize >= maxFileSizeInLevel) {
                newSegment.saveIndexFile(updatedIdxMap);
                curDataFile.close();
                logger.debug("New data file created: {}", newSegment.getDataFile());

                LSMTree.filesToRename.add(newSegment.getIndexFile());

                newSegment = new Segment(level + 1);
                curDataFile = newSegment.openDataFileForWrite();

                updatedIdxMap.clear();
                curSize = 0;
            }
            // f.setLastModified(Instant.now().getEpochSecond() * 1000);
        }
        curDataFile.getFD().sync();
        curDataFile.close();

        // for (RandomAccessFile file : openedFiles.values())
        // file.close();

        if (!updatedIdxMap.isEmpty()) {
            newSegment.saveIndexFile(updatedIdxMap);
            LSMTree.filesToRename.add(newSegment.getIndexFile());

            logger.debug("New data file created: {}", newSegment.getIndexFile());
            logger.debug("New index file created: {}", newSegment.getDataFile());
        }

        renameIndexFiles();
        deleteCompactedFiles();
    }

    public void insert(Segment segment) {
        this.segments.add(0, segment);
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
        for (Segment s : segments) {
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

    public Segment getLatestSegment(int level) {
        if (segments.size() == 0)
            return new Segment(level);

        return segments.get(segments.size() - 1);
        // File mo6stRecent = files[files.length - 1];
        // return new Segment(level, mostRecent.getAbsolutePath());
    }
}
