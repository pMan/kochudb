package com.kochudb.tasks;

import com.kochudb.io.FileIO;
import com.kochudb.k.Record;
import com.kochudb.types.ByteArrayKey;
import com.kochudb.types.SkipList;
import com.kochudb.types.SkipListNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class MemTableFlusher implements Runnable {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    Deque<SkipList> memTableQueue;
    
    // key and the offset where the record is stored in file
    Map<ByteArrayKey, Long> keyToOffsetMap;

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
            try {
                SkipList skipList = memTableQueue.peekFirst();

                keyToOffsetMap = new TreeMap<>();

                String[] filenames = FileIO.createNewIdxAndDataFilenames(0);
                String indexFilePath = filenames[0];
                String datafilePath = filenames[1];

                logger.info("Flushing to file: {}", datafilePath);

                try (RandomAccessFile dataFile = new RandomAccessFile(datafilePath, "rw")) {
                    Iterator<SkipListNode> iterator = skipList.iterator();

                    while (iterator.hasNext()) {
                        SkipListNode node = iterator.next();
                        ByteArrayKey key = node.getKey();

                        byte[] keyBytes = FileIO.compress(key.getBytes());
                        long keyOffset = FileIO.appendData(dataFile, keyBytes, Record.KEY);

                        byte[] valBytes = FileIO.compress(node.getValue().serialize());
                        FileIO.appendData(dataFile, valBytes, Record.VALUE);
                        keyToOffsetMap.put(key, keyOffset);
                    }

                    logger.debug("Flush complete");
                } catch (Exception e) {
                    logger.error("Flush failed. {}", e.getMessage());
                    e.printStackTrace();
                }

                FileIO.writeIndexFile(indexFilePath, keyToOffsetMap);
                logger.debug("Index file created: {}", indexFilePath);
                memTableQueue.remove(skipList);

            } catch (Exception e) {
                logger.error("Writing file to disk failed");
                e.printStackTrace();
            }
        }
    }
}
