package com.kochudb.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

class KochuDBServerTest {

    @Mock
    private static KochuDBServer kochuDBServerUnderTest;

    @Captor
    private ArgumentCaptor<String> captor;

    static Properties p = new Properties();

    @BeforeAll
    public static void setup() throws Exception {
        // Setup
        p.load(new FileInputStream("src/test/resources/application-test.properties"));
        kochuDBServerUnderTest = mock(KochuDBServer.class, withSettings().useConstructor(p));

        kochuDBServerUnderTest.start();
    }
    
    @Test
    void testRun() {
        verify(kochuDBServerUnderTest, timeout(100).atLeastOnce()).start();
        // Verify the results
    }

    @AfterAll
    static void testTerminate() throws IOException {
        // Run the test
        kochuDBServerUnderTest.terminate();
        File dir = new File(p.getProperty("data.dir"));
        if (dir.exists() && dir.isDirectory())
        	dir.delete();
        // Verify the results
    }
}
