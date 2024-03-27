package com.kochudb.shared;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ResponseTest {

    private Response responseUnderTest;

    @BeforeEach
    void setUp() {
        responseUnderTest = new Response("com", "key", "val", "data");
    }

    @Test
    void testDataGetterAndSetter() {
        final String data = "data";
        responseUnderTest.setData(data);
        assertEquals(data, responseUnderTest.getData());
    }

    @Test
    void testToString() {
        assertEquals("[key=key, value=val, command=com, data=data]", responseUnderTest.toString());
    }
}
