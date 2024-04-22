package com.kochudb.storage;

import static com.kochudb.k.K.DATA_FILE_EXT;
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
	public void mergeSegments(int level) throws IOException {
		List<File> files = Arrays.asList(FileUtil.findFiles(basePath, level));
		for (File f: markedForDeletion) {
			files.remove(f);
		}
		List<Segment> segments = new LinkedList<Segment>();
		for (File file: files) {
			Segment seg = new Segment(level, file.getAbsolutePath(), file.getAbsolutePath().replaceFirst(".idx$", DATA_FILE_EXT));
			segments.add(seg);
		}
    	long maxFileSizeInLevel = computeMaxFileSizeInLevel(level + 1);
    	
    	PriorityQueue<Object[]> keyValueHeap = new PriorityQueue<Object[]>((a, b) -> {
    		ByteArray keya = (ByteArray) a[0];
    		ByteArray keyb = (ByteArray) b[0];
    		
    		if (keya.compareTo(keyb) == 0) {
    			Segment sega = (Segment) a[2];
    			Segment segb = (Segment) b[2];
    			return sega.getIndexFile().compareTo(segb.getIndexFile());
    		}
    		return keya.compareTo(keyb);
    	});

		Map<Segment, RandomAccessFile> openedFiles = new HashMap<>();
		
    	for (Segment seg: segments) {
    		for (Map.Entry<ByteArray, Long> indexData: seg.parseIndexFile().entrySet()) {
    			Object[] objArray = new Object[] {indexData.getKey(), indexData.getValue(), seg};
    			keyValueHeap.add(objArray);
    		}
    		openedFiles.put(seg, createDatFromIdx(seg.getIndexFile()));
    	}
    	
    	Segment segNew = this.getMostRecentSegment(level + 1);
    	Map<ByteArray, Long> updatedIdxMap = new HashMap<>();
    	RandomAccessFile newDataFile = segNew.openDataFileForWrite();

    	long curSize = 0;
		while (! keyValueHeap.isEmpty()) {
			Object[] objArray = keyValueHeap.poll();
			while (! keyValueHeap.isEmpty() && ((ByteArray)objArray[0]).compareTo(((ByteArray)keyValueHeap.peek()[0])) == 0)
				objArray = keyValueHeap.poll();
			
			ByteArray key = (ByteArray) objArray[0];
			Long offset = (Long) objArray[1];
			Segment segment = (Segment) objArray[2];				

			byte[] serializedKVPair = segment.readKVPairBytes(offset);
			Long newOffset = segNew.appendData(newDataFile, serializedKVPair);

			updatedIdxMap.put((ByteArray)objArray[0], newOffset);
			
			curSize += key.length() + Long.BYTES + serializedKVPair.length;
			
			if (curSize >= maxFileSizeInLevel) {
				segNew.saveIndexFile(updatedIdxMap);
				newDataFile.close();
				
				filesToRename.add(segNew.getIndexFile());

				logger.debug("New data file created: {}", segNew.getIndexFile());
				logger.debug("New index file created: {}", segNew.getDataFile());
				
				segNew = createNewSegment(level + 1);
				
				newDataFile = segNew.openDataFileForWrite();
				updatedIdxMap = new HashMap<>();
				curSize = 0;
			}
			// f.setLastModified(Instant.now().getEpochSecond() * 1000);
		}
		newDataFile.getFD().sync();
		newDataFile.close();

		for (RandomAccessFile file : openedFiles.values())
			file.close();

		if (!updatedIdxMap.isEmpty()) {
			segNew.saveIndexFile(updatedIdxMap);
			filesToRename.add(segNew.getIndexFile());
			
			logger.debug("New data file created: {}", segNew.getIndexFile());
			logger.debug("New index file created: {}", segNew.getDataFile());
		}

		for (String path: filesToRename)
			FileUtil.renameIndexFile(path);
		
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
		File[] files = FileUtil.findFiles(basePath, level + 1);
		if (files.length == 0) {
			return createNewSegment(level);
		}
		File f = files[files.length - 1];
		return new Segment(level, f.getAbsolutePath() , f.getAbsolutePath().replaceFirst(".idx$", DATA_FILE_EXT));
	}
	
	/**
	 * create a segment file at given level
	 * 
	 * @param level
	 * @return
	 */
	public static Segment createNewSegment(int level) {
    	String[] newFileNames = FileUtil.createNewIdxAndDataFilenames(level);
    	newFileNames[0] = newFileNames[0].replaceFirst(".idx$", DATA_FILE_EXT);

    	return new Segment(level, newFileNames[0], newFileNames[1]);

	}
}