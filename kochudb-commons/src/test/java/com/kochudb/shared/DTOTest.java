package com.kochudb.shared;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;

import javax.xml.stream.events.StartDocument;

class DTOTest {

    private DTO requestUnderTest;

    @BeforeEach
    void setUp() {
        requestUnderTest = new DTO("com".getBytes(), "key".getBytes(), "val".getBytes());
    }

    @Test
    void testCommandGetterAndSetter() {
        final String command = "com";
        assertEquals(command, new String(requestUnderTest.command(), StandardCharsets.UTF_8));
    }

    @Test
    void testKeyGetterAndSetter() {
        final String key = "key";
        assertEquals(key, new String(requestUnderTest.key(), StandardCharsets.UTF_8));
    }

    @Test
    void testValueGetterAndSetter() {
        final String value = "val";
        assertEquals(value, new String(requestUnderTest.value(), StandardCharsets.UTF_8));
    }
}
