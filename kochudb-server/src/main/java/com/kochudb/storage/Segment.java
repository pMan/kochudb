package com.kochudb.storage;

import static com.kochudb.k.K.DATA_FILE_EXT;
import static com.kochudb.k.K.INDEX_FILE_EXT;
import static com.kochudb.k.Record.KEY;
import static com.kochudb.k.Record.VALUE;
import static com.kochudb.utils.ByteUtil.bytesToInt;
import static com.kochudb.utils.ByteUtil.bytesToLong;
import static com.kochudb.utils.ByteUtil.longToBytes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.kochudb.types.ByteArray;
import com.kochudb.types.KeyValuePair;
import com.kochudb.utils.FileUtil;

public class Segment {
    private static final Logger logger = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private int level;
    private String indexFile, dataFile;

    public Segment(int level, String index, String data) {
        this.level = level;
        this.indexFile = index;
        this.dataFile = data;
    }

    public Segment(int level, String index) {
        this.level = level;
        this.indexFile = index;
        this.dataFile = index.replaceFirst(INDEX_FILE_EXT, DATA_FILE_EXT);
    }

    public Segment(int level) {
        String[] newFileNames = FileUtil.createNewIdxAndDataFilenames(level);
        if (level > 0)
            newFileNames[0] = newFileNames[0].replaceFirst(".idx$", ".idxtmp");

        this.level = level;
        this.indexFile = newFileNames[0];
        this.dataFile = newFileNames[1];

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
    public Map<ByteArray, Long> parseIndexFile() {
        logger.debug("Reading index file: {}", this.indexFile);

        Map<ByteArray, Long> keyToOffset = new TreeMap<>();

        try (RandomAccessFile raf = new RandomAccessFile(this.indexFile, "r")) {
            raf.seek(0);
            while (raf.getFilePointer() < raf.length()) {
                // key
                byte[] key = new byte[raf.read()];
                raf.read(key, 0, key.length);

                // offset where the value resides
                byte[] nextEightBytes = new byte[Long.BYTES];
                raf.read(nextEightBytes, 0, Long.BYTES);
                long offset = bytesToLong(nextEightBytes);

                keyToOffset.put(new ByteArray(key), offset);
            }
        } catch (FileNotFoundException e) {
            return new TreeMap<>();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return keyToOffset;
    }

    /**
     * write the given map into index file
     * 
     * @param keyToOffset map
     */
    public void saveIndexFile(Map<ByteArray, Long> keyToOffset) {
        logger.debug("Creating index file: {}", this.indexFile);

        try (RandomAccessFile indexFile = new RandomAccessFile(this.indexFile, "rw")) {
            indexFile.setLength(0);

            for (Map.Entry<ByteArray, Long> entry : keyToOffset.entrySet()) {

                byte[] keyBytes = entry.getKey().serialize();
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
     * read KVPair at the given offest in bytes
     * 
     * @param offset offset
     * @return bytes
     * @throws IOException
     */
    public byte[] readKVPairBytes(Long offset) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(dataFile, "r");
        raf.seek(offset);

        byte[] keyHeader = readBytes(raf, offset, KEY.length);
        offset += KEY.length;

        int keyLen = bytesToInt(keyHeader);
        byte[] key = readBytes(raf, offset, keyLen);
        offset += keyLen;

        byte[] valHeader = readBytes(raf, offset, VALUE.length);
        offset += VALUE.length;

        int valLen = bytesToInt(valHeader);
        byte[] val = readBytes(raf, offset, valLen);
        raf.close();

        byte[] bytes = new byte[KEY.length + VALUE.length + keyLen + valLen];
        int curPos = 0;
        System.arraycopy(keyHeader, 0, bytes, curPos, KEY.length);
        curPos += KEY.length;

        System.arraycopy(key, 0, bytes, curPos, key.length);
        curPos += key.length;

        System.arraycopy(valHeader, 0, bytes, curPos, VALUE.length);
        curPos += VALUE.length;

        System.arraycopy(val, 0, bytes, curPos, val.length);

        return bytes;
    }

    /**
     * read KVPair from the given offset
     * 
     * @param offset offset
     * @return KeyValuePair object
     * @throws IOException
     */
    public KeyValuePair readKVPair(Long offset) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(dataFile, "r");

        int keyLen = bytesToInt(readBytes(raf, offset, KEY.length));
        offset += KEY.length;

        byte[] keyData = readBytes(raf, offset, keyLen);
        offset += keyLen;

        int valLen = bytesToInt(readBytes(raf, offset, VALUE.length));
        offset += VALUE.length;

        byte[] valueData = readBytes(raf, offset, valLen);

        return new KeyValuePair(keyData, valueData);
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
    public void persist(SkipList skipList) throws FileNotFoundException, IOException {

        Map<ByteArray, Long> keyToOffsetMap = new TreeMap<>();

        try (RandomAccessFile dataFileObj = new RandomAccessFile(dataFile, "rw")) {
            Iterator<SkipListNode> iterator = skipList.iterator();

            while (iterator.hasNext()) {
                SkipListNode node = iterator.next();

                KeyValuePair kvPair = new KeyValuePair(node.getKey().serialize(), node.getValue().serialize());
                long offset = appendData(dataFileObj, kvPair.serialize());

                keyToOffsetMap.put(node.getKey(), offset);
            }

            saveIndexFile(keyToOffsetMap);
            logger.debug("Data file created: {}", dataFile);
            logger.debug("Index file created: {}", indexFile);

        } catch (Exception e) {
            logger.error("Flush failed. {}", e.getMessage());
            e.printStackTrace();
        }
    }
}
