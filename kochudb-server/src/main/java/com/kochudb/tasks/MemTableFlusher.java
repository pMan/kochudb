package com.kochudb.tasks;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.io.FileIO;
import com.kochudb.k.Record;
import com.kochudb.types.ByteArray;
import com.kochudb.types.SSTable;
import com.kochudb.types.Skiplist;
import com.kochudb.types.SkiplistNode;

public class MemTableFlusher implements Runnable {

	private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

	Deque<Skiplist> memTableQueue;
	
	// key and the offset where the record is stored in file
	Map<ByteArray, Long> keyToOffsetMap;

	public MemTableFlusher(Deque<Skiplist> memTableQueue) {
		this.memTableQueue = memTableQueue;
	}

	@Override
	public void run() {
		while (!memTableQueue.isEmpty()) {
			try {
				Skiplist skipList = memTableQueue.peekFirst();
				
				keyToOffsetMap = new TreeMap<ByteArray, Long>();
				
				String[] filenames = FileIO.createNewIdxAndDataFilenames(0);
				String indexfilePath = filenames[0];
				String datafilePath = filenames[1];

				logger.info("Flushing to file: {}", datafilePath);
				
				try (RandomAccessFile dataFile = new RandomAccessFile(datafilePath, "rw")) {
					Iterator<SkiplistNode> iter = skipList.iterator();
					
					while (iter.hasNext()) {
						SkiplistNode node = iter.next();
						ByteArray key = node.getKey();
						
						byte[] keyBytes = FileIO.compress(key.getBytes());
						long keyOffset = SSTable.appendData(dataFile, keyBytes, Record.KEY);

						byte[] valBytes = FileIO.compress(node.getValue());
						SSTable.appendData(dataFile, valBytes, Record.VALUE);
						keyToOffsetMap.put(key, keyOffset);
					}
					
					logger.debug("Flush complete");
				} catch (Exception e) {
					logger.error("Flush failed. {}", e.getMessage());
					e.printStackTrace();
				}
				
				SSTable.writeIndexFile(indexfilePath, keyToOffsetMap);
				logger.debug("Index file created: {}", indexfilePath);
				memTableQueue.remove(skipList);
				
			} catch (IOException e) {
				logger.error("Writing file to disk failed");
				e.printStackTrace();
			}
		}
	}
}
