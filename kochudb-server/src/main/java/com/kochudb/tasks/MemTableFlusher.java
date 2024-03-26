package com.kochudb.tasks;

import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.io.FileIO;
import com.kochudb.types.ByteArray;
import com.kochudb.types.KVPair;
import com.kochudb.types.SkipList;
import com.kochudb.types.SkipListNode;

public class MemTableFlusher implements Runnable {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    Deque<SkipList> memTableQueue;
    
    // key and the offset where the record is stored in file
    Map<ByteArray, Long> keyToOffsetMap;

    public MemTableFlusher(Deque<SkipList> memTableQueue) {
        this.memTableQueue = memTableQueue;
    }

    @Override
    public void run() {
        flush();
    }

    // flush current state of the queue
	private void flush() {
		while (!memTableQueue.isEmpty()) {
			SkipList skipList = memTableQueue.peekFirst();

			keyToOffsetMap = new TreeMap<>();

			String[] filenames = FileIO.createNewIdxAndDataFilenames(0);
			String indexFilePath = filenames[0];
			String datafilePath = filenames[1];

			try (RandomAccessFile dataFile = new RandomAccessFile(datafilePath, "rw")) {
				Iterator<SkipListNode> iterator = skipList.iterator();

				while (iterator.hasNext()) {
					SkipListNode node = iterator.next();

					//byte[] keyBytes = FileIO.compress(node.getKey().serialize());
					//byte[] valBytes = FileIO.compress(node.getValue().serialize());

					KVPair kvPair = new KVPair(node.getKey().serialize(), node.getValue().serialize());
					long offset = FileIO.appendData(dataFile, kvPair.serialize());

					keyToOffsetMap.put(node.getKey(), offset);
				}

				FileIO.writeIndexFile(indexFilePath, keyToOffsetMap);
				logger.debug("Data file created: {}", datafilePath);
				logger.debug("Index file created: {}", indexFilePath);
				
				memTableQueue.remove(skipList);

				logger.debug("Flush complete");
			} catch (Exception e) {
				logger.error("Flush failed. {}", e.getMessage());
				e.printStackTrace();
			}

		}
	}
}
