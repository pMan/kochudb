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
import java.util.Queue;
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
	String basePath;

	// files marked for deletion during a compaction process
	public static Queue<File> markedForDeletion;

	public static List<String> filesToRename;
	
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

		markedForDeletion = new ConcurrentLinkedQueue<>();
		
		filesToRename = new ArrayList<String>();
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
				if (markedForDeletion.contains(indexFile))
					continue;

				try {
					Segment segment = new Segment(level, indexFile.getAbsolutePath());
					Map<ByteArray, Long> curIndex = segment.parseIndexFile();

					if (curIndex.containsKey(key)) {
						KeyValuePair record = segment.readKVPair(curIndex.get(key));
						return record.val();
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
	 * compact all segments in given level to a merged segment in next higher level
	 * 
	 * @param level
	 * @throws IOException
	 */
	public void compactAndPromoteSegments(int level) throws IOException {
		List<File> files = Arrays.asList(FileUtil.findFiles(basePath, level));
		for (File toDelete: markedForDeletion)
			files.remove(toDelete);
		
		List<Segment> segments = new LinkedList<Segment>();
		for (File file: files)
			segments.add(new Segment(level, file.getAbsolutePath()));
		
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

		Map<Segment, RandomAccessFile> openedFiles = new HashMap<>();
		
    	for (Segment seg: segments) {
    		for (Map.Entry<ByteArray, Long> indexData: seg.parseIndexFile().entrySet()) {
    			Object[] objArray = new Object[] {indexData.getKey(), indexData.getValue(), seg};
    			keyValueHeap.offer(objArray);
    		}
    		openedFiles.put(seg, createDatFromIdx(seg.getIndexFile()));
    	}
    	
    	Segment curSeg = this.getMostRecentSegment(level + 1);
    	Map<ByteArray, Long> updatedIdxMap = new TreeMap<>();
    	RandomAccessFile curDataFile = curSeg.openDataFileForWrite();

    	long curSize = 0, maxFileSizeInLevel = computeMaxFileSizeInLevel(level + 1);
    	
		while (! keyValueHeap.isEmpty()) {
			Object[] objArray = keyValueHeap.poll();
			while (! keyValueHeap.isEmpty() && ((ByteArray) objArray[0]).compareTo(((ByteArray) keyValueHeap.peek()[0])) == 0)
				objArray = keyValueHeap.poll();
			
			ByteArray key = (ByteArray) objArray[0];
			Long offset = (Long) objArray[1];
			Segment segment = (Segment) objArray[2];

			byte[] serializedKVPair = segment.readKVPairBytes(offset);
			Long curOffset = curSeg.appendData(curDataFile, serializedKVPair);

			updatedIdxMap.put((ByteArray) objArray[0], curOffset);
			curSize += serializedKVPair.length;
			
			if (curSize >= maxFileSizeInLevel) {
				curSeg.saveIndexFile(updatedIdxMap);
				curDataFile.close();

				logger.debug("New data file created: {}", curSeg.getIndexFile());
				logger.debug("New index file created: {}", curSeg.getDataFile());
				
				curSeg = createNewSegment(level + 1);
				curDataFile = curSeg.openDataFileForWrite();

				filesToRename.add(curSeg.getIndexFile());
				updatedIdxMap.clear();
				curSize = 0;
			}
			// f.setLastModified(Instant.now().getEpochSecond() * 1000);
		}
		curDataFile.getFD().sync();
		curDataFile.close();

		for (RandomAccessFile file : openedFiles.values())
			file.close();

		if (! updatedIdxMap.isEmpty()) {
			curSeg.saveIndexFile(updatedIdxMap);
			filesToRename.add(curSeg.getIndexFile());
			
			logger.debug("New data file created: {}", curSeg.getIndexFile());
			logger.debug("New index file created: {}", curSeg.getDataFile());
		}

		for (String tempName: filesToRename)
			FileUtil.renameIndexFile(tempName);
		
		for (Segment seg: segments)
			SSTable.markedForDeletion.add(new File(seg.getIndexFile()));
    }

	/**
	 * compute max file size in the given level
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
	public Segment getMostRecentSegment(int level) {
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
    	newFileNames[0] = newFileNames[0].replaceFirst(".idx$", ".idxtmp");

    	return new Segment(level, newFileNames[0], newFileNames[1]);

	}
}