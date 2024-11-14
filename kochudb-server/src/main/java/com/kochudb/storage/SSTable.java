package com.kochudb.storage;

import static com.kochudb.k.K.DATA_FILE_EXT;
import static com.kochudb.k.K.INDEX_FILE_EXT;
import static com.kochudb.k.Record.KEY;
import static com.kochudb.k.Record.VALUE;
import static com.kochudb.utils.ByteUtil.bytesToInt;
import static com.kochudb.utils.ByteUtil.longToBytes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.types.ByteArray;
import com.kochudb.types.Record;
import com.kochudb.utils.FileUtil;

public class SSTable {
    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private String indexFile, dataFile;

    public SSTable(int level) {
        String[] newFileNames = FileUtil.createNewIdxAndDataFilenames(level);
        if (level > 0)
            newFileNames[0] = newFileNames[0].replaceFirst(".idx$", ".idxtmp");

        this.indexFile = newFileNames[0];
        this.dataFile = newFileNames[1];
    }

    public SSTable(int level, String index) {
        this.indexFile = index;
        this.dataFile = index.replaceFirst(INDEX_FILE_EXT, DATA_FILE_EXT);
    }

    public String getIndexFile() {
        return this.indexFile;
    }

    public String getDataFile() {
        return this.dataFile;
    }

    /**
     * parse index file of this segment into a Map
     * 
     * @return map of key and its corresponding offset
     */
    public SkipList<Record> parseIndex() {
        logger.debug("Reading index file: {}", this.indexFile);

        // Map<ByteArray, Long> keyToOffset = new TreeMap<>();
        SkipList<Record> skipList = new SkipList<>();

        try (RandomAccessFile raf = new RandomAccessFile(this.indexFile, "r")) {
            raf.seek(0);
            while (raf.getFilePointer() < raf.length()) {
                // key
                byte[] key = new byte[raf.read()];
                raf.read(key, 0, key.length);

                // offset where the value resides
                byte[] nextEightBytes = new byte[Long.BYTES];
                raf.read(nextEightBytes, 0, Long.BYTES);
                // long offset = ByteUtil.bytesToLong(nextEightBytes);

                // keyToOffset.put(new ByteArray(key), offset);
                Record record = new Record(new ByteArray(key), new ByteArray(nextEightBytes),
                        Instant.now().getEpochSecond());
                skipList.put(record);
            }
        } catch (FileNotFoundException e) {
            return new SkipList<>();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return skipList;
    }

    /**
     * search this SSTable segment for a given key
     * 
     * @param searchKey
     * @return KeyValueRecord
     */
    public Record search(ByteArray searchKey) {
        SkipList<Record> skiplist = this.parseIndex();
        SkipListNode<Record> result = skiplist.find(new Record(searchKey, null, 0L));

        if (result.data != null && result.getKey().compareTo(searchKey) == 0)
            return (Record) result.data;

        return null;
    }

    /**
     * write the given map into index file
     * 
     * @param keyToOffset map
     */
    public void saveIndex(Map<ByteArray, Long> keyToOffset) {
        logger.debug("Creating index file: {}", this.indexFile);

        try (RandomAccessFile indexFile = new RandomAccessFile(this.indexFile, "rw")) {
            indexFile.setLength(0);

            for (Map.Entry<ByteArray, Long> entry : keyToOffset.entrySet()) {

                byte[] keyBytes = entry.getKey().bytes();
                Long value = entry.getValue();

                byte[] offsetBytes = longToBytes(value);
                byte[] recWithSize = new byte[keyBytes.length + KEY.length + Long.BYTES];

                recWithSize[0] = (byte) keyBytes.length;

                System.arraycopy(keyBytes, 0, recWithSize, KEY.length, keyBytes.length);
                System.arraycopy(offsetBytes, 0, recWithSize, KEY.length + keyBytes.length, Long.BYTES);

                indexFile.seek(indexFile.length());
                indexFile.write(recWithSize);
            }

            indexFile.getFD().sync();
            logger.debug("New index file created: {}", this.indexFile);
        } catch (FileNotFoundException fnfe) {
            logger.error("File not found: {}", this.indexFile);
            fnfe.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * open data file for writing
     * 
     * @return opened file
     * @throws FileNotFoundException
     */
    public RandomAccessFile openDataFileForWrite() throws FileNotFoundException {
        return new RandomAccessFile(this.dataFile, "rw");
    }

    /**
     * append byte[] to data file
     * 
     * @param raf  data file
     * @param data bytes to write
     * @return offset where data was written at
     * @throws IOException
     */
    public long appendData(RandomAccessFile raf, byte[] data) throws IOException {
        long offset = raf.length();
        raf.write(data);
        return offset;
    }

    /**
     * read KVPair from the given offset
     * 
     * @param offset offset
     * @return KeyValuePair object
     * @throws IOException
     */
    public Record readRecord(Long offset) {
        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "r")) {

            byte[] timeBytes = readBytes(raf, offset, Long.BYTES);
            offset += Long.BYTES;

            int keyLen = bytesToInt(readBytes(raf, offset, KEY.length));
            offset += KEY.length;

            byte[] keyData = readBytes(raf, offset, keyLen);
            offset += keyLen;

            int valLen = bytesToInt(readBytes(raf, offset, VALUE.length));
            offset += VALUE.length;

            byte[] valueData = readBytes(raf, offset, valLen);

            return Record.fromCompressed(keyData, valueData, timeBytes);
        } catch (FileNotFoundException e) {
            return new Record(new byte[] {}, "File could not be found on the disk".getBytes(), 0);
        } catch (IOException e) {
            return new Record(new byte[] {}, "Error encountered whie reading file".getBytes(), 0);
        }
    }

    /**
     * read len bytes from the raf from the offset
     * 
     * @param raf    file to read from
     * @param offset where the data reside
     * @param len    number of bytes to read
     * @return bytes
     * @throws IOException
     */
    public byte[] readBytes(RandomAccessFile raf, Long offset, int len) throws IOException {
        raf.seek(offset);
        byte[] b = new byte[len];
        raf.read(b, 0, len);
        return b;
    }

    /**
     * save a skiplist to disk
     * 
     * @param skipList skiplist to save
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void persist(SkipList<Record> skipList) throws FileNotFoundException, IOException {
        Map<ByteArray, Long> keyToOffsetMap = new TreeMap<>();
        try (RandomAccessFile dataFileObj = new RandomAccessFile(dataFile, "rw")) {
            Iterator<SkipListNode<Record>> iterator = skipList.iterator();

            while (iterator.hasNext()) {
                SkipListNode<Record> listNode = iterator.next();

                long offset = appendData(dataFileObj, listNode.data.serialize());

                keyToOffsetMap.put(listNode.getKey(), offset);
            }

            saveIndex(keyToOffsetMap);
            logger.debug("Data file created: {}", dataFile);
            logger.debug("Index file created: {}", indexFile);

        } catch (Exception e) {
            logger.error("Flush failed. {}", e.getMessage());
            e.printStackTrace();
        }
    }
}
