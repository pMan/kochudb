package com.kochudb.tasks;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Deque;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.types.ByteArray;
import com.kochudb.types.SSTSegment;
import com.kochudb.types.SkipList;

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

			SSTSegment sSTSegment = new SSTSegment(0);
			try {
				sSTSegment.persist(skipList);
				memTableQueue.remove(skipList);
			} catch (IOException e) {
				logger.error("memtable flush operation failed");
				e.printStackTrace();
			}
		}
	}
}
