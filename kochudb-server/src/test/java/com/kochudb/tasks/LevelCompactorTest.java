package com.kochudb.tasks;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kochudb.storage.SSTable;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertThrows;

class LevelCompactorTest {

    private LevelCompactor levelCompactorUnderTest;

    @BeforeEach
    void setUp() {
        File f = new File("test-dir");
        levelCompactorUnderTest = new LevelCompactor(f);
    }

    @Test
    void testRun() {
        // Setup
        // Run the test
        assertThrows(NullPointerException.class, () -> levelCompactorUnderTest.run());

        // Verify the results
    }

    @Test
    void testCompactLevel() {
        // Setup
        // Run the test
        assertThrows(NullPointerException.class, () -> levelCompactorUnderTest.compactLevel(0));

        // Verify the results
    }
}
