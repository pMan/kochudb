package com.kochudb.tasks;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Comparator;
import java.util.Deque;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//import com.kochudb.storage.SSTable;
import com.kochudb.storage.SSTable;
import com.kochudb.storage.SkipList;
import com.kochudb.types.ByteArray;
import com.kochudb.utils.FileUtil;

public class MemTableFlusher implements Runnable {

	private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

	Deque<SkipList> memTableQueue;

	// key and the offset of the value where the record is stored in file
	Map<ByteArray, Long> keyToOffsetMap;

	public MemTableFlusher(Deque<SkipList> memTableQueue) {
		this.memTableQueue = memTableQueue;
	}

	@Override
	public void run() {
		long currentTime = Instant.now().toEpochMilli();

		flush();

		// delete all WALs created before
		File[] walsTooDelete = FileUtil.findFiles("WAL", "^binlog[0-9_]+$",
				Comparator.comparingLong(File::lastModified));

		for (File file : walsTooDelete)
			if (file.lastModified() < currentTime)
				file.delete();
	}

	// flush current state of the queue
	private void flush() {
		while (!memTableQueue.isEmpty()) {
			SkipList skipList = memTableQueue.peekFirst();

			SSTable sSTable = new SSTable(0);
			try {
				sSTable.persist(skipList);
				memTableQueue.remove(skipList);
			} catch (IOException e) {
				logger.error("memtable flush operation failed");
				e.printStackTrace();
			}
		}
	}
}
