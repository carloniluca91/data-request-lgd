package it.luca.lgd.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static it.luca.lgd.utils.Java8Utils.orNull;

public class TimeUtils {

    public static java.sql.Date toSqlDate(LocalDate localDate) {

        return orNull(localDate, java.sql.Date::valueOf);
    }

    public static java.sql.Timestamp toSqlTimestamp(LocalDateTime localDateTime) {

        return orNull(localDateTime, java.sql.Timestamp::valueOf);
    }

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

    public static LocalDate toLocalDate(java.sql.Date date) {

        return orNull(date, java.sql.Date::toLocalDate);
    }

    public static LocalDate toLocalDate(String date, String format) {

        return orNull(date, s -> LocalDate.parse(s, DateTimeFormatter.ofPattern(format)));
    }

    public static LocalDateTime toLocalDateTime(java.sql.Timestamp timestamp) {

        return orNull(timestamp, java.sql.Timestamp::toLocalDateTime);
    }

    public static String toString(LocalDate localDate, String format) {

        return orNull(localDate, l -> l.format(DateTimeFormatter.ofPattern(format)));
    }
}
