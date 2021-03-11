package it.luca.lgd.utils;

import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

class Java8UtilsTest {

    private final String NOT_NULL_STRING = "abc";
    private final String NULL_STRING = null;
    private final Function<String, String> toUpperCase = String::toUpperCase;
    private final String EXPECTED = toUpperCase.apply(NOT_NULL_STRING);

    @Test
    public void orElse() {

        String OR_ELSE_VALUE = "def";
        String actual = Java8Utils.orElse(NOT_NULL_STRING, toUpperCase, OR_ELSE_VALUE);
        assertNotNull(actual);
        assertEquals(EXPECTED, actual);

        //noinspection ConstantConditions
        assertEquals(OR_ELSE_VALUE, Java8Utils.orElse(NULL_STRING, toUpperCase, OR_ELSE_VALUE));
    }

    @Test
    public void orNull() {

        String actual = Java8Utils.orNull(NOT_NULL_STRING, toUpperCase);
        assertNotNull(actual);
        assertEquals(EXPECTED, actual);

        //noinspection ConstantConditions
        assertNull(Java8Utils.orNull(NULL_STRING, toUpperCase));
    }
}