package com.kochudb.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.storage.LSMTree;

public class FileUtil {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Find all files from the given level, sorted oldest to newest.
     * 
     * @param dataDirectory parent directory
     * @param level level of LSM Tree
     * @return File[] files
     */
	public static File[] findFiles(String dataDirectory, int level) {
		return findFiles(dataDirectory, level, Comparator.comparingLong(File::lastModified));
	}
	
	/**
	 * Find all files from the given level, sorted by comparator
	 * 
	 * @param dataDirectory directory where files reside
	 * @param level level of LSM Tree
	 * @param comparator comparator to sort files
	 * @return File[] files
	 */
	public static File[] findFiles(String dataDirectory, int level, Comparator<File> comparator) {
		File dir = new File(dataDirectory);

		File[] files = dir.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File dir1, String name) {
				return name.matches("^l" + level + "_[0-9]+\\.[0-9]+\\.idx$");
			}
		});

		Arrays.sort(files, comparator);
		return files;
	}
	
    public static RandomAccessFile createDatFromIdx(String idxFile) {
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(idxFile.replaceFirst(".(idx|idxtmp)$", ".dat"), "r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return raf;
    }
    
    /**
     * create file names for the given level
     * @param level
     * @return
     */
    public static String[] createNewIdxAndDataFilenames(int level) {
        String newFilename = FileUtil.generateFilename();
        String newIdxFilename = newFilename + ".idx";
        
        File newIdxFile = new File(newIdxFilename.replaceAll("([0-9]+.[0-9]+)(?=.idx)", "l" + level + "_$1"));
        newIdxFilename = newIdxFile.getAbsolutePath();
        String newDatFilename = newIdxFile.getAbsolutePath().replace(".idx", ".dat");
        
        return new String[] {newIdxFilename, newDatFilename};
    }
    
    /**
	 * rename an index file (.idx or .idxtmp) t0 data file
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
        return (LSMTree.dataDir.getAbsolutePath()) + "/"
                + instant.getEpochSecond() + "." + instant.getNano();
    }
}
