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
        p.load(new FileInputStream("src/main/resources/application.properties"));

        p.setProperty("server.port", "2223");
        p.setProperty("query.pool.size", "25");
        p.setProperty("data.dir", "data-test-123");

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
        File dir = new File(p.getProperty("data.dir"));
        if (dir.exists() && dir.isDirectory())
            dir.delete();
        kochuDBServerUnderTest.terminate();
        // Verify the results
    }
}
