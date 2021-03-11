package it.luca.lgd.utils;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.*;

class TimeUtilsTest {

    private final String startDate = "2021-01-01";
    private final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

    @Test
    public void isValidDate() {

        assertTrue(TimeUtils.isValidDate(startDate, DEFAULT_DATE_FORMAT));
        String OTHER_FORMAT = "yyyyMMdd";
        assertFalse(TimeUtils.isValidDate(startDate, OTHER_FORMAT));
    }

    @Test
    public void isBeforeOrEqual() {

        String endDate = "2021-02-01";
        assertTrue(TimeUtils.isBeforeOrEqual(startDate, endDate, DEFAULT_DATE_FORMAT));
        assertTrue(TimeUtils.isBeforeOrEqual(startDate, startDate, DEFAULT_DATE_FORMAT));
        assertFalse(TimeUtils.isBeforeOrEqual(endDate, startDate, DEFAULT_DATE_FORMAT));
    }
}