package com.kochudb.tasks;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kochudb.storage.SkipList;

import java.util.LinkedList;
import java.util.List;

class MemTableFlusherTest {

    private MemTableFlusher memTableFlusherUnderTest;

    @BeforeEach
    void setUp() {
        memTableFlusherUnderTest = new MemTableFlusher(new LinkedList<>());
    }

    @Test
    void testRun() {
        // Setup
        // Run the test
        memTableFlusherUnderTest.run();

        // Verify the results
    }
}
