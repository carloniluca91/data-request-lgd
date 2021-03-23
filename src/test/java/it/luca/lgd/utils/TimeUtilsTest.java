package it.luca.lgd.utils;

import org.junit.jupiter.api.Test;

import java.util.function.BiPredicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TimeUtilsTest {

    private final String START_DATE = "2021-01-01";
    private final String END_DATE = "2021-02-01";
    private final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

    @Test
    public void isValidDate() {

        assertTrue(TimeUtils.isValidDate(START_DATE, DEFAULT_DATE_FORMAT));
        String OTHER_FORMAT = "yyyyMMdd";
        assertFalse(TimeUtils.isValidDate(START_DATE, OTHER_FORMAT));
    }

    @Test
    public void isBeforeOrEqual() {

        assertTrue(TimeUtils.isBeforeOrEqual(START_DATE, END_DATE, DEFAULT_DATE_FORMAT));
        assertTrue(TimeUtils.isBeforeOrEqual(START_DATE, START_DATE, DEFAULT_DATE_FORMAT));
        assertFalse(TimeUtils.isBeforeOrEqual(END_DATE, START_DATE, DEFAULT_DATE_FORMAT));
    }

    @Test
    public void isBothStartDateAndEndDateValid() {

        BiPredicate<String, String> biPredicate = (s, e) ->
                TimeUtils.isBothStartDateAndEndDateValid(s, e, DEFAULT_DATE_FORMAT).getT1();

        assertTrue(biPredicate.test(START_DATE, END_DATE));
        assertTrue(biPredicate.test(START_DATE, START_DATE));
        assertFalse(biPredicate.test(END_DATE, START_DATE));
        assertFalse(biPredicate.test("2020-0101", END_DATE));
        assertFalse(biPredicate.test(START_DATE, "2020-0201"));
    }
}