package it.luca.lgd.utils;

import it.luca.lgd.oozie.WorkflowJobParameter;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static it.luca.lgd.utils.Java8Utils.orNull;

/**
 * Time utilities class
 */

public class TimeUtils {

    public static Tuple2<Boolean, String> isBothStartDateAndEndDateValid(String startDate, String endDate, String format) {

        return isValidDate(startDate, format) ?
                isValidDate(endDate, format) ?
                        isBeforeOrEqual(startDate, endDate, format) ?
                                new Tuple2<>(true, null) :
                                // StartDate greater than endDate
                                new Tuple2<>(false, String.format("%s (%s) is greater than %s (%s)",
                                        WorkflowJobParameter.START_DATE.getName(), startDate,
                                        WorkflowJobParameter.END_DATE.getName(), endDate)) :

                        // Invalid endDate
                        new Tuple2<>(false, String.format("Invalid %s (%s). It should follow format '%s'",
                                WorkflowJobParameter.END_DATE.getName(), endDate, format)) :

                // Invalid startDate
                new Tuple2<>(false, String.format("Invalid %s (%s). It should follow format '%s'",
                        WorkflowJobParameter.START_DATE.getName(), startDate, format));
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

    public static LocalDate toLocalDate(java.util.Date date) {

        return orNull(date, x -> x.toInstant().atZone(ZoneId.systemDefault()).toLocalDate());
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

    public static LocalDateTime toLocalDateTime(java.util.Date date) {

        return orNull(date, x -> x.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
    }

    public static java.sql.Date toSqlDate(java.util.Date date) {

        return orNull(date, d -> new java.sql.Date(d.getTime()));
    }

    public static java.sql.Date toSqlDate(LocalDate localDate) {

        return orNull(localDate, java.sql.Date::valueOf);
    }

    public static java.sql.Timestamp toSqlTimestamp(LocalDateTime localDateTime) {

        return orNull(localDateTime, java.sql.Timestamp::valueOf);
    }

    public static String localDateToString(LocalDate localDate, String pattern) {

        return orNull(localDate, l -> l.format(DateTimeFormatter.ofPattern(pattern)));
    }
}
