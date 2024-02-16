package com.kochudb.types;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Deque;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.k.K;
import com.kochudb.server.KVStorage;
import com.kochudb.tasks.LevelCompactor;
import com.kochudb.tasks.MemTableFlusher;

/**
 * LSM Tree implementing basic operation on data store
 */
public class LSMTree implements KVStorage<ByteArray, byte[]> {

	private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

	/**
	 * SkipList for in-memory data
	 */
	Skiplist memtable;

	/**
	 * Deque of memtables Deque is for handling failures by flushing thread
	 */
	Deque<Skiplist> memtableQueue;

	/**
	 * Reference to SSTable instance
	 */
	SSTable ssTable;

	/**
	 * Size in bytes
	 * 
	 * Threshold for triggering flushing of currently active skiplist
	 */
	Integer maxSkipListSize = K.LEVEL_ZERO_FILE_MAX_SIZE_KB * 1024; // 4 kb

	Integer curSkipListSize = 0;

	/**
	 * single threaded executor for periodic memtable flushing
	 */
	ExecutorService memtableExecutor;

	/**
	 * single threaded, scheduled executor for compaction thread
	 */
	ScheduledThreadPoolExecutor compactorExecutor;

	public static File dataDir;
	
	/**
	 * Base class that takes care of all LSM Tree operations, including compaction
	 * and indexing.
	 * 
	 * @throws IOException
	 */
	public LSMTree(Properties props) throws IOException {
		dataDir = new File(props.getProperty("data.dir", "data"));
		if (!dataDir.exists() || !dataDir.isDirectory()) {
			dataDir.mkdirs();
			logger.info("Data Directory created");
		}

		curSkipListSize = 0;

		/**
		 * search thread and memtable-flush thread can perform one or both of the below
		 * operations concurrently: iterate the queue holding skiplists, or iterate the skiplist
		 */
		memtableQueue = new ConcurrentLinkedDeque<>();
		
		//memtable = new ConcurrentSkipListMap<ByteArray, byte[]>(new ByteArrayComparator());
		memtable = new Skiplist();
		
		ssTable = new SSTable(dataDir);

		memtableExecutor = Executors.newSingleThreadExecutor((runnable) -> {
			return new Thread(runnable, "memtable-flusher");
		});
		
		compactorExecutor = new ScheduledThreadPoolExecutor(1, (runnable) -> {
			return new Thread(runnable, "compactor");
		});
		compactorExecutor.scheduleWithFixedDelay(new LevelCompactor(dataDir), 5, 5, TimeUnit.SECONDS);
	}

	/**
	 * Search key in the data store and return the most recent value of the key
	 * Search in-memory first, then disk
	 */
	@Override
	public byte[] get(ByteArray key) {
		if (memtable.containsKey(key))
			return (byte[]) memtable.get(key).val;

		logger.debug("Key not in memtable");

		Iterator<Skiplist> iter = memtableQueue.descendingIterator();

		while (iter.hasNext()) {
			Skiplist skiplist = iter.next();

			if (skiplist.containsKey(key)) {
				logger.debug("Key found in older memtable");
				return (byte[]) skiplist.get(key).val;
			}
		}

		logger.debug("Searching key in data files");
		return ssTable.search(key);
	}

	/**
	 * Add/overwrite the value for the given key in the data store Max size of key
	 * is restricted to 256 bytes, refer - SSTable.writeIndexFile Max size of value
	 * is restricted to 4MB
	 */
	@Override
	public byte[] set(ByteArray key, byte[] val) {
		// start memtable flusher thread
		if (curSkipListSize >= maxSkipListSize) {
			memtableQueue.add(memtable);
			memtable = new Skiplist();
			memtableExecutor.submit(new MemTableFlusher(memtableQueue));
			curSkipListSize = 0;
		}

		if (key.length() > 255)
			return "Error: Key too long. Max allowed size is 256".getBytes();

		if (val.length > K.VALUE_MAX_SIZE)
			return "Error: Value too long. Max allowed size is 4MB".getBytes();

		memtable.put(key, val);
		curSkipListSize += key.length() + val.length;

		return "ok".getBytes();
	}

	/**
	 * Delete key from data store
	 */
	@Override
	public byte[] del(ByteArray key) {
		memtable.put(key, new byte[] {});
		return "ok".getBytes();
	}
}
