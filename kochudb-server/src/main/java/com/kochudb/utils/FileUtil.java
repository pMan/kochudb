package com.kochudb.utils;

import static com.kochudb.k.K.DATA_FILE_EXT;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.storage.LSMTree;
import com.kochudb.storage.SSTable;

public class FileUtil {

	private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

	/**
	 * Find all files from the given level, sorted oldest to newest.
	 * 
	 * @param dataDirectory parent directory
	 * @param level         level of LSM Tree
	 * @return File[] files
	 */
	public static File[] findFiles(String dataDirectory, int level) {
		String pattern = "^l" + level + "_[0-9]+\\.[0-9]+\\.idx$";
		return findFiles(dataDirectory, pattern, Comparator.comparingLong(File::lastModified));
	}

	public static List<SSTable> findSegments(String dataDirectory, int level) {
		String pattern = "^l" + level + "_[0-9]+\\.[0-9]+\\.idx$";
		File[] files = findFiles(dataDirectory, pattern, Comparator.comparingLong(File::lastModified));
		List<SSTable> sSTables = new LinkedList<SSTable>();
		for (File file : files) {
			SSTable seg = new SSTable(level, file.getAbsolutePath());
			sSTables.add(seg);
		}
		return sSTables;
	}

	/**
	 * Find all files from the given level, sorted by comparator
	 * 
	 * @param dataDirectory directory where files reside
	 * @param level         level of LSM Tree
	 * @param comparator    comparator to sort files
	 * @return File[] files
	 */
	public static File[] findFiles(String dataDirectory, String pattern, Comparator<File> comparator) {
		File dir = new File(dataDirectory);

		File[] files = dir.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir1, String name) {
				return name.matches(pattern);
			}
		});

		Arrays.sort(files, comparator);
		return files;
	}

	public static RandomAccessFile createDatFromIdx(String idxFile) {
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(idxFile.replaceFirst(".(idx|idxtmp)$", DATA_FILE_EXT), "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return raf;
	}

	/**
	 * read len bytes from the raf from the offset
	 * 
	 * @param raf    file to read from
	 * @param offset where the data reside
	 * @param len    number of bytes to read
	 * @return bytes
	 * @throws IOException
	 */
	public static byte[] readBytes(RandomAccessFile raf, Long offset, int len) throws IOException {
		raf.seek(offset);
		byte[] b = new byte[len];
		raf.read(b, 0, len);
		return b;
	}

	/**
	 * create file names for the given level
	 * 
	 * @param level
	 * @return
	 */
	public static String[] createNewIdxAndDataFilenames(int level) {
		String newFilename = FileUtil.generateFilename();
		String newIdxFilename = newFilename + ".idx";

		File newIdxFile = new File(newIdxFilename.replaceAll("([0-9]+.[0-9]+)(?=.idx)", "l" + level + "_$1"));
		newIdxFilename = newIdxFile.getAbsolutePath();
		String newDatFilename = newIdxFile.getAbsolutePath().replace(".idx", DATA_FILE_EXT);

		return new String[] { newIdxFilename, newDatFilename };
	}

	/**
	 * rename a temp index file (.idxtmp) to .idx file
	 * 
	 * @param toRename absolute file path
	 * @return absolute file path
	 */
	public static String renameIndexFile(String toRename) {

		String newName = toRename.replaceFirst(".idxtmp$", ".idx");

		if (new File(toRename).renameTo(new File(newName)))
			logger.debug("Index file renamed to " + newName);
		else
			logger.error("Failed to rename inedx file");

		return newName;
	}

	/**
	 * Filename generator
	 *
	 * @return filename (canonical path) without extension
	 * @throws IOException
	 */
	public static String generateFilename() {
		Instant instant = Instant.now();
		int nanos = instant.getNano();
		return (LSMTree.dataDir.getAbsolutePath()) + "/" + instant.getEpochSecond() + "." + nanos;
	}
}
