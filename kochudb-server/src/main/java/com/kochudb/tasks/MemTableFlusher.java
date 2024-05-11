package com.kochudb.tasks;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Deque;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//import com.kochudb.storage.SSTable;
import com.kochudb.storage.Segment;
import com.kochudb.storage.SkipList;
import com.kochudb.types.ByteArray;

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

            Segment segment = new Segment(0);
            try {
                segment.persist(skipList);
                memTableQueue.remove(skipList);
            } catch (IOException e) {
                logger.error("memtable flush operation failed");
                e.printStackTrace();
            }
        }
    }
}
