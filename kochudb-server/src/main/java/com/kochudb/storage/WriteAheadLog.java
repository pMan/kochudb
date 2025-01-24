package com.kochudb.storage;

import static com.kochudb.utils.ByteUtil.intToBytes;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WriteAheadLog {
	private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

	private File walDir;
	private File wal;
	private FileOutputStream walStream;

	public WriteAheadLog(String dir) {
		walDir = new File(dir);
		if (!walDir.exists() || !walDir.isDirectory())
			walDir.mkdirs();

		File recent = getMostRecentFile();

		wal = recent == null ? createNewWal() : recent;

		try {
			walStream = new FileOutputStream(wal, true);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private File createNewWal() {
		Instant instant = Instant.now();
		int nanos = instant.getNano();
		String filename = "" + instant.getEpochSecond() + "_" + nanos;
		File newFile = new File(walDir.getAbsolutePath() + "/binlog_" + filename);
		return newFile;
	}

	public File getMostRecentFile() {
		File[] files = walDir.listFiles(File::isFile);
		long lastModifiedTime = 0L;
		File recentFile = null;

		if (files != null) {
			for (File file : files) {
				if (file.lastModified() > lastModifiedTime) {
					recentFile = file;
					lastModifiedTime = file.lastModified();
				}
			}
		}
		return recentFile;
	}

	public void append(byte[] data) {
		byte[] bytesToWrite = new byte[data.length + Integer.BYTES];
		byte[] leng = intToBytes(Integer.BYTES, data.length);

		System.arraycopy(leng, 0, bytesToWrite, 0, Integer.BYTES);
		System.arraycopy(data, 0, bytesToWrite, Integer.BYTES, data.length);

		try {
			walStream.write(bytesToWrite, 0, bytesToWrite.length);
		} catch (IOException e) {
			logger.info(e.getMessage());
			e.printStackTrace();
		}
	}

	public void updateFile() {
		wal = createNewWal();
	}

}
