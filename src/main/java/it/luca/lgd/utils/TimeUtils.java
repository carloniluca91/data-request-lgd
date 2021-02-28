package it.luca.lgd.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class TimeUtils {

    public static String changeDateFormat(String date, String oldFormat, String newFormat) {

        return toString(toLocalDate(date, oldFormat), newFormat);
    }

    public static LocalDate fromUtilDateToLocalDate(java.util.Date date) {

        return Optional.ofNullable(date)
                .map(d -> d.toInstant().atZone(ZoneId.systemDefault()).toLocalDate())
                .orElse(null);
    }

    public static LocalDateTime fromUtilDateToLocalDateTime(java.util.Date date) {

        return Optional.ofNullable(date)
                .map(d -> LocalDateTime.ofInstant(d.toInstant(), ZoneId.systemDefault()))
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
