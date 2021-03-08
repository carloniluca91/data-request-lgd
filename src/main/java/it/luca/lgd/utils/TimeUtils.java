package it.luca.lgd.utils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class TimeUtils {

    public static String changeDateFormat(String date, String oldFormat, String newFormat) {

        return toString(toLocalDate(date, oldFormat), newFormat);
    }

    public static java.sql.Date fromUtilDateToSqlDate(java.util.Date date) {

        return Optional.ofNullable(date)
                .map(d -> new java.sql.Date(d.getTime()))
                .orElse(null);
    }

    public static java.sql.Timestamp fromUtilDateToSqlTimestamp(java.util.Date date) {

        return Optional.ofNullable(date)
                .map(d -> new java.sql.Timestamp(d.getTime()))
                .orElse(null);
    }

    public static Boolean isValidDate(String date, String format) {

        try { toLocalDate(date, format);
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    public static Boolean isBeforeOrEqual(String firstDate, String seconDate, String commonFormat) {

        return toLocalDate(firstDate, commonFormat)
                .compareTo(toLocalDate(seconDate, commonFormat)) <= 0;
    }

    public static LocalDate toLocalDate(String date, String format) {

        return LocalDate.parse(date, DateTimeFormatter.ofPattern(format));
    }

    public static String toString(LocalDate localDate, String format) {

        return localDate.format(DateTimeFormatter.ofPattern(format));
    }
}
