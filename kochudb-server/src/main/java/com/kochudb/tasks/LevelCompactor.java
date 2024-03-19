package com.kochudb.tasks;

import static com.kochudb.k.K.ERR_NO_DATA_DIR;
import static com.kochudb.k.K.LEVEL_MAX_FILES_MULTIPLIER;
import static com.kochudb.k.K.LEVEL_MAX_SIZE_MULTIPLIER;
import static com.kochudb.k.K.LEVEL_ZERO_FILE_MAX_SIZE_KB;
import static com.kochudb.k.K.LEVEL_ZERO_NUM_FILES;
import static com.kochudb.k.K.NUM_LEVELS;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.io.FileIO;
import com.kochudb.types.ByteArrayKey;
import com.kochudb.types.SSTable;

public class LevelCompactor implements Runnable {

	private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

	// Maximum size of file in level 0 in bytes
	long levelZeroFileSize;

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

		compactLevel(0);

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
	boolean shouldStartCompactionNow(File[] files, int level) {
		int allowedNumFilesInCurLevel = computeNumFilesInLevel(level);
		long maxFileSizeInLevel = computeMaxFileSizeInLevel(level);
		long maxTotalSizeInLevel = allowedNumFilesInCurLevel * maxFileSizeInLevel;
		long curTotalSizeInLevel = Arrays.stream(files).map(File::length).mapToLong(Long::longValue).sum();

		return curTotalSizeInLevel > maxTotalSizeInLevel || files.length > allowedNumFilesInCurLevel;
	}

	/**
	 * Decide whether current level to be compacted. If yes, start compaction.
	 *
	 * @param level current level
	 */
	void compactLevel(int level) {
		File[] files = FileIO.findFiles(dataDirectory, level);

		if (shouldStartCompactionNow(files, level)) {
			logger.debug("Compaction started. current level: {}, number of files in level: {}", level, files.length);

			String bigTmpIdxFile = compactFiles(level, files, 0, files.length - 1);

			logger.trace("New compacted file created in level {}: {}", level, bigTmpIdxFile);

			FileIO.renameIndexFile(bigTmpIdxFile);

			// all files from cur level are compacted and moved to next level
			// delete all files marked for deletion
			deleteOutdatedFiles();

			// propagate to next level
			if (level < NUM_LEVELS)
				compactLevel(level + 1);
		} else {
			logger.trace("Did not meet all criteria to begin compaction in {}", level);
		}
	}

	/**
	 * merge all SSTables - two adjacent files at a time
	 * 
	 * @param curLevel current level
	 * @param files    array of index files
	 * @param start    start position
	 * @param end      end position
	 * @return absolute name of merged file
	 */
	private String compactFiles(int curLevel, File[] files, int start, int end) {

		if (start == end)
			return files[start].getAbsolutePath();

		if (start == end - 1)
			return mergeTwoFiles(files[start], files[end], curLevel);

		int mid = start + (end - start) / 2;

		String file1 = compactFiles(curLevel, files, start, mid);
		String file2 = compactFiles(curLevel, files, mid + 1, end);

		return mergeTwoFiles(new File(file1), new File(file2), curLevel);
	}

	/**
	 * merge tow SSTable into one
	 * 
	 * @param file1    first file
	 * @param file2    second file
	 * @param curLevel current level
	 * @return absolute name of merged file
	 */
	private String mergeTwoFiles(File file1, File file2, int curLevel) {

		Map<ByteArrayKey, Object[]> mergedMap = new TreeMap<>();

		// order of files is important. file1 was created earlier than file2
		for (File file : new File[] { file1, file2 }) {
			Map<ByteArrayKey, Long> indexMap;
			try {
				indexMap = FileIO.readIndexFile(file.getCanonicalPath());
				for (Map.Entry<ByteArrayKey, Long> entry : indexMap.entrySet()) {
					mergedMap.put(entry.getKey(), new Object[] { entry.getValue(), file });
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
		logger.debug("New Temp file name: {}", newIdxFilename);

		Map<ByteArrayKey, Long> updatedIdxMap = new HashMap<>();
		Map<File, RandomAccessFile> openedFiles = new HashMap<>();

		openedFiles.put(file1, FileIO.createDatFromIdx(file1.getAbsolutePath()));
		openedFiles.put(file2, FileIO.createDatFromIdx(file2.getAbsolutePath()));

		try (RandomAccessFile newDataFile = new RandomAccessFile(newDatFilename, "rw")) {

			for (Entry<ByteArrayKey, Object[]> entry : mergedMap.entrySet()) {
				File file = (File) entry.getValue()[1];
				Long offset = (Long) entry.getValue()[0];

				byte[] serializedKVPair = FileIO.readKVPairBytes(openedFiles.get(file), offset);
				Long newOffset = FileIO.appendData(newDataFile, serializedKVPair);

				updatedIdxMap.put(entry.getKey(), newOffset);
				// f.setLastModified(Instant.now().getEpochSecond() * 1000);
			}
			newDataFile.getFD().sync();

			for (RandomAccessFile file : openedFiles.values())
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
		} finally {
			for (RandomAccessFile file: openedFiles.values()) {
				try { file.close(); } catch (IOException e) { }
			}
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
