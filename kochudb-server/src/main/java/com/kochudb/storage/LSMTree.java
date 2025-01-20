package com.kochudb.storage;

import static com.kochudb.k.K.LEVEL_ZERO_FILE_MAX_SIZE_KB;
import static com.kochudb.k.K.NUM_LEVELS;
import static com.kochudb.k.K.VALUE_MAX_SIZE;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.server.KVStorage;
import com.kochudb.tasks.LevelCompactor;
import com.kochudb.tasks.MemTableFlusher;
import com.kochudb.types.KochuDoc;

/**
 * LSM Tree implementing basic operation on data store
 */
public class LSMTree implements KVStorage {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * SkipList for in-memory data
     */
    SkipList memTable;

    /**
     * Deque of memTables is for handling failures by flushing thread
     */
    Deque<SkipList> memTableQueue;

    /**
     * Size in bytes Threshold for triggering flushing of currently active skiplist
     */
    Integer maxSkipListSize = LEVEL_ZERO_FILE_MAX_SIZE_KB * 1024; // 4 kb

    Integer curSkipListSize;

    /**
     * single threaded executor for periodic memTable flushing
     */
    ExecutorService memTableExecutor;

    /**
     * single threaded, scheduled executor for compaction thread
     */
    ScheduledThreadPoolExecutor compactorExecutor;

    public static File dataDir;

    // files marked for deletion during a compaction process
    public static Queue<File> markedForDeletion;

    public static List<String> filesToRename;

    public static List<Level> levels;

    /**
     * Base class that takes care of all LSM Tree operations, including compaction
     * and indexing.
     * 
     * @throws IOException IOException
     */
    public LSMTree(Properties props) {
        dataDir = new File(props.getProperty("data.dir", "data"));
        curSkipListSize = 0;
        filesToRename = new ArrayList<String>();

        /*
         * search thread and memTable-flush thread can perform one or both of the below
         * operations concurrently: iterate the queue holding skiplists, or iterate the
         * skiplist
         */
        memTableQueue = new ConcurrentLinkedDeque<SkipList>();
        memTable = new SkipList();

        if (!dataDir.exists() || !dataDir.isDirectory()) {
            dataDir.mkdirs();
            logger.info("Data Directory created");
        }

        levels = new ArrayList<Level>();
        updateLevels();

        memTableExecutor = Executors.newSingleThreadExecutor((runnable) -> {
            return new Thread(runnable, "memtable-flusher");
        });

        compactorExecutor = new ScheduledThreadPoolExecutor(1, (runnable) -> {
            return new Thread(runnable, "compactor");
        });

        compactorExecutor.scheduleWithFixedDelay(new LevelCompactor(dataDir), 3, 1, TimeUnit.SECONDS);
    }

    /**
     * update levels
     */
    public void updateLevels() {
        List<Level> updated = new ArrayList<Level>();
        for (int i = 0; i <= NUM_LEVELS; i++) {
            Level level = new Level(i, this);
            updated.add(level);
        }
        levels = updated;
    }

    /**
     * Search key in the data store and return the most recent value of the key
     * Search in-memory first, then disk
     */
    @Override
    public KochuDoc get(byte[] key) {
        var search = new KochuDoc(key, null, 0L);
        if (memTable.containsKey(search))
            return memTable.get(search).data;

        Iterator<SkipList> iter = memTableQueue.descendingIterator();
        while (iter.hasNext()) {
            SkipList skiplist = iter.next();
            if (skiplist.containsKey(search))
                return skiplist.get(search).data;
        }

        for (Level level : levels) {
            KochuDoc doc = level.search(search);
            if (doc != null)
                return doc;
        }
        return new KochuDoc(null, new byte[] {}, 0);
    }

    /**
     * Add/overwrite the value for the given key in the data store Max size of key
     * is restricted to 256 bytes, refer - SSTable.writeIndexFile Max size of value
     * is restricted to 4MB
     */
    @Override
    public KochuDoc set(KochuDoc doc) {
        if (curSkipListSize >= maxSkipListSize) {
            memTable.writeLock.lock();

            try {
                memTableQueue.add(memTable);
                memTable = new SkipList();
                if (memTableQueue.size() > 5) {
                	// create new WAL
                }
            } finally {
                memTableQueue.peekLast().writeLock.unlock();
            }

            memTableExecutor.submit(new MemTableFlusher(memTableQueue));
            curSkipListSize = 0;
        }

        if (doc.getKey().length() > 255)
            return new KochuDoc(null, "Error: Key too long. Max allowed size is 256".getBytes(), 0L);
        ;

        if (doc.getValue().length() > VALUE_MAX_SIZE)
            return new KochuDoc(null, "Error: Value too long. Max allowed size is 4MB".getBytes(), 0L);

        memTable.put(doc);
        // append WAL
        curSkipListSize += doc.length();

        return doc;
    }

    /**
     * Delete key from data store
     */
    @Override
    public KochuDoc del(byte[] key) {
        KochuDoc doc = new KochuDoc(key, null, Instant.now().toEpochMilli());
        memTable.put(doc);
        // append WAL
        return doc;
    }
}
