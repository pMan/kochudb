package com.kochudb.storage;

import static com.kochudb.k.K.LEVEL_ZERO_FILE_MAX_SIZE_KB;
import static com.kochudb.k.K.NUM_LEVELS;
import static com.kochudb.k.K.VALUE_MAX_SIZE;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
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
import com.kochudb.types.ByteArray;

/**
 * LSM Tree implementing basic operation on data store
 */
public class LSMTree implements KVStorage<ByteArray, ByteArray> {

    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * SkipList for in-memory data
     */
    SkipList memTable;

    /**
     * Deque of memTables Deque is for handling failures by flushing thread
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
    public LSMTree(Properties props) throws IOException {
        dataDir = new File(props.getProperty("data.dir", "data"));
        if (!dataDir.exists() || !dataDir.isDirectory()) {
            dataDir.mkdirs();
            logger.info("Data Directory created");
        }

        curSkipListSize = 0;
        filesToRename = new ArrayList<String>();

        /*
         * search thread and memTable-flush thread can perform one or both of the below
         * operations concurrently: iterate the queue holding skiplists, or iterate the
         * skiplist
         */
        memTableQueue = new ConcurrentLinkedDeque<>();
        memTable = new SkipList();

        levels = new ArrayList<Level>();
        for (int level = 0; level < NUM_LEVELS; level++) {
            levels.add(new Level(level));
        }

        memTableExecutor = Executors.newSingleThreadExecutor((runnable) -> {
            return new Thread(runnable, "memtable-flusher");
        });

        compactorExecutor = new ScheduledThreadPoolExecutor(1, (runnable) -> {
            return new Thread(runnable, "compactor");
        });

        compactorExecutor.scheduleWithFixedDelay(new LevelCompactor(dataDir), 3, 1, TimeUnit.SECONDS);
    }

    /**
     * Search key in the data store and return the most recent value of the key
     * Search in-memory first, then disk
     */
    @Override
    public ByteArray get(ByteArray key) {
        if (memTable.containsKey(key))
            return memTable.get(key).val;

        logger.trace("Key {} not in memTable", key);

        Iterator<SkipList> iter = memTableQueue.descendingIterator();

        while (iter.hasNext()) {
            SkipList skiplist = iter.next();

            if (skiplist.containsKey(key)) {
                logger.trace("Key found in older memTable");
                return skiplist.get(key).val;
            }
        }

        logger.debug("Searching key in data files");

        for (Level level : levels) {
            ByteArray val;
            try {
                val = level.search(key);
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("Search failed with error: {}", e.getMessage());
                val = new ByteArray();
                break;
            }
            if (val != null)
                return val;
        }
        return new ByteArray();
    }

    /**
     * Add/overwrite the value for the given key in the data store Max size of key
     * is restricted to 256 bytes, refer - SSTable.writeIndexFile Max size of value
     * is restricted to 4MB
     */
    @Override
    public byte[] set(ByteArray key, ByteArray val) {
        if (curSkipListSize >= maxSkipListSize) {
            memTable.writeLock.lock();

            try {
                memTableQueue.add(memTable);
                memTable = new SkipList();
            } finally {
                memTableQueue.peekLast().writeLock.unlock();
            }

            memTableExecutor.submit(new MemTableFlusher(memTableQueue));
            curSkipListSize = 0;
        }

        if (key.length() > 255)
            return "Error: Key too long. Max allowed size is 256".getBytes();

        if (val.length() > VALUE_MAX_SIZE)
            return "Error: Value too long. Max allowed size is 4MB".getBytes();

        memTable.put(key, val);
        curSkipListSize += key.length() + val.length();

        return "ok".getBytes();
    }

    /**
     * Delete key from data store
     */
    @Override
    public byte[] del(ByteArray key) {
        memTable.put(key, new ByteArray());
        return "ok".getBytes();
    }
}
