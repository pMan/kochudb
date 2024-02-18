package com.kochudb.server;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;

class MainTest {

    @Test
    void testMain() throws Exception {
        // Verify the results
        assertThrows(NumberFormatException.class, () -> Main.main(new String[]{"src/test/resources/testfile"}));
    }

    @Test
    void testMain_ThrowsNumberFormatException() {
        // Setup
        // Run the test
        assertThrows(NumberFormatException.class, () -> Main.main(new String[]{"src/test/resources/testfile"}));
    }

    @Test
    void testMain_ThrowsIOException() {
        // Setup
        // Run the test
        assertThrows(NumberFormatException.class, () -> Main.main(new String[]{"src/test/resources/testfile"}));
    }
}
