package com.kochudb.tasks;

import static com.kochudb.k.K.DATA_FILE_EXT;
import static com.kochudb.k.K.ERR_NO_DATA_DIR;
import static com.kochudb.k.K.LEVEL_MAX_FILES_MULTIPLIER;
import static com.kochudb.k.K.LEVEL_MAX_SIZE_MULTIPLIER;
import static com.kochudb.k.K.LEVEL_ZERO_FILE_MAX_SIZE_KB;
import static com.kochudb.k.K.LEVEL_ZERO_NUM_FILES;
import static com.kochudb.k.K.NUM_LEVELS;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.storage.LSMTree;
import com.kochudb.storage.Level;
import com.kochudb.storage.SSTable;
import com.kochudb.utils.FileUtil;

public class LevelCompactor implements Runnable {

	private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

	// Maximum size of file in level 0 in bytes
	long levelZeroFileSize;

	// Directory where data and index files are stored
	String dataDirectory;

	// Currently compaction is in progress?
	private static volatile AtomicBoolean isRunning;

	File dir;
	// SSTable ssTable;

	LSMTree tree;
	
	/**
	 * Constructor
	 * 
	 * @param dir data directory
	 */
	public LevelCompactor(File dir, LSMTree lsmTree) {

		//this.ssTable = ssTable;
		this.tree = lsmTree;
		
		isRunning = new AtomicBoolean(false);

		levelZeroFileSize = 1024 * LEVEL_ZERO_FILE_MAX_SIZE_KB; // 4 kb

		this.dir = dir;

		try {
			dataDirectory = dir.getCanonicalPath();
		} catch (IOException e) {
			logger.error("Failed to access Data directory: {}", dataDirectory);
			System.exit(ERR_NO_DATA_DIR);
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
		try {
			compactLevel(0);
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (isRunning.getAndSet(false))
			logger.trace("Compaction thread finished the job");
	}

	/**
	 * check if compaction criteria is met
	 * 
	 * @param files files in the level
	 * @param level current level
	 * @return boolean
	 */
	boolean shouldStartCompactionNow(int level) {
		File[] files = FileUtil.findFiles(dataDirectory, level);
		if (files.length == 0)
			return false;
		
		int allowedNumFilesInCurLevel = computeNumFilesInLevel(level);
		if (files.length > allowedNumFilesInCurLevel)
			return true;
		
		long maxFileSizeInLevel = computeMaxFileSizeInLevel(level);
		long maxTotalSizeInLevel = allowedNumFilesInCurLevel * maxFileSizeInLevel;
		long curTotalSizeInLevel = Arrays.stream(files).map(File::length).mapToLong(Long::longValue).sum();

		return curTotalSizeInLevel > maxTotalSizeInLevel;
	}

	/**
	 * Decide whether current level to be compacted. If yes, start compaction.
	 *
	 * @param level current level
	 * @throws IOException 
	 */
	void compactLevel(int level) throws IOException {
		if (shouldStartCompactionNow(level)) {
			logger.debug("Compaction started. current level: {}", level);

			Level curLevel = new Level(level, this.tree);
			curLevel.compactLevel();
			
			logger.trace("Deleting compacted files at level: {}", level);
			deleteOutdatedFiles();

			if (level < NUM_LEVELS)
				compactLevel(level + 1);
		} else {
			logger.trace("Did not meet all criteria to begin compaction in {}", level);
		}
	}

	/**
	 * files that are compacted into bigger files are periodically deleted
	 */
	private void deleteOutdatedFiles() {
		for (File file : LSMTree.markedForDeletion) {
			String dataFilename = file.getAbsolutePath().replaceFirst(".(idx|idxtmp)$", DATA_FILE_EXT);
			File datafile = new File(dataFilename);

			logger.debug(file.delete() ? "File deleted: {}" : "Failed to delete file: {}", file.getAbsolutePath());
			logger.debug(datafile.delete() ? "File deleted: {}" : "Failed to delete file: {}", dataFilename);
		}

		LSMTree.markedForDeletion.clear();
	}

	/**
	 * Compute the maximum size a file is allowed to have in the given level This is
	 * computed from the configuration properties. Number of files in the given
	 * level is equal to the number of files in prev level multiplied by the
	 * multiplier. At level 0, it's configured as 10.
	 * 
	 * @param level current level
	 * @return max file size in current level
	 */
	private long computeMaxFileSizeInLevel(int level) {
		if (level > 0)
			return computeMaxFileSizeInLevel(level - 1) * LEVEL_MAX_SIZE_MULTIPLIER;
		return levelZeroFileSize;
	}

	/**
	 * computer number of files in the given level This is computed from the number
	 * of files in the prev level multiplied by a configured multiplier. Multiplier
	 * is configured for level 0.
	 * 
	 * @param level - level of level compaction
	 * @return number of files in level
	 */
	private int computeNumFilesInLevel(int level) {
		if (level > 0)
			return computeNumFilesInLevel(level - 1) * LEVEL_MAX_FILES_MULTIPLIER;
		return LEVEL_ZERO_NUM_FILES;
	}
}
