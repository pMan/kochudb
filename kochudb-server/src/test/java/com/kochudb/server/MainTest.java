package com.kochudb.server;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.FileNotFoundException;

import org.junit.jupiter.api.Test;

class MainTest {

    @Test
    void testMain() throws Exception {
        // Verify the results
        assertThrows(FileNotFoundException.class, () -> Main.main(new String[] { "non-existing-file" }));
    }

}
