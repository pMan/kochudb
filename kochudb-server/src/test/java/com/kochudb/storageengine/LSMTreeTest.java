package com.kochudb.storageengine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.kochudb.storage.LSMTree;
import com.kochudb.types.ByteArray;

class LSMTreeTest {

    static LSMTree lsmt;
    static Properties props = new Properties();

    @BeforeAll
    static void testLSMTree() {
        props.put("data.dir", "data-test");
        LSMTreeTest.lsmt = new LSMTree(props);
    }

    @AfterAll
    static void tearDownLSMTree() {
        new File((String) props.get("data.dir")).delete();
    }

    @Test
    void testSetAndGet() throws UnsupportedEncodingException {
        ByteArray key = new ByteArray("Key".getBytes());
        ByteArray val = new ByteArray("Val".getBytes());
        byte[] result = LSMTreeTest.lsmt.set(key, val);

        assertEquals("ok", new String(result, "utf-8"));
        assertEquals("Val", new String(LSMTreeTest.lsmt.get(key).bytes(), "utf-8"));
    }

    @Test
    void testGetNonExistingKey() throws UnsupportedEncodingException {
        // assertEquals("", new String(LSMTreeTest.lsmt.get(new
        // ByteArray("non-existing-key")), "utf-8"));
    }
}
