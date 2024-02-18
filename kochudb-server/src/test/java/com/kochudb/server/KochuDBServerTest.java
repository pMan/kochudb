package com.kochudb.server;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.mockito.Mockito.*;

class KochuDBServerTest {

    @Mock
    private KochuDBServer kochuDBServerUnderTest;

    @Captor
    private ArgumentCaptor<String> captor;

    Properties p = new Properties();

    @Test
    void testRun() throws Exception {
        // Setup
        // Run the test
        p.load(new FileInputStream("src/test/resources/application-test.properties"));
        kochuDBServerUnderTest = mock(KochuDBServer.class, withSettings().useConstructor(p));

        kochuDBServerUnderTest.start();

        verify(kochuDBServerUnderTest, timeout(100).atLeastOnce()).start();
        // Verify the results
    }


    @Test
    void testListen() throws IOException {
        // Setup
        Properties props = new Properties();
        props.setProperty("server.port", "2023");
        props.setProperty("data.dir", "data-test");
        KochuDBServer kochuDBServerUnderTest1 = new KochuDBServer(props);
        kochuDBServerUnderTest1.start();

        // Run the test
        kochuDBServerUnderTest1.terminate();

        // Verify the results
    }

    @Test
    void testTerminate() throws IOException {
        // Setup
        Properties props = new Properties();
        props.setProperty("server.port", "2023");
        props.setProperty("data.dir", "data-test");
        KochuDBServer kochuDBServerUnderTest1 = new KochuDBServer(props);
        kochuDBServerUnderTest1.start();

        // Run the test
        kochuDBServerUnderTest1.terminate();

        // Verify the results
    }
}
