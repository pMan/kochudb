package com.kochudb.shared;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RequestTest {

    private Request requestUnderTest;

    @BeforeEach
    void setUp() {
        requestUnderTest = new Request("com", "key", "val");
    }

    @Test
    void testCommandGetterAndSetter() {
        final String command = "com";
        requestUnderTest.setCommand(command);
        assertEquals(command, requestUnderTest.getCommand());
    }

    @Test
    void testKeyGetterAndSetter() {
        final String key = "key";
        requestUnderTest.setKey(key);
        assertEquals(key, requestUnderTest.getKey());
    }

    @Test
    void testValueGetterAndSetter() {
        final String value = "val";
        requestUnderTest.setValue(value);
        assertEquals(value, requestUnderTest.getValue());
    }

    @Test
    void testToString() {
        assertEquals("//com key val", requestUnderTest.toString());
    }
}
