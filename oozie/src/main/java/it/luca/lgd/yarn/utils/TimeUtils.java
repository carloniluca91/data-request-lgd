package it.luca.lgd.yarn.utils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimeUtils {

    public static ZoneId romeZoneId() {
        return ZoneId.of("Europe/Rome");
    }

    public static String epochMillsToZonedTimestamp(long epochMillis, ZoneId zoneId) {
        return ZonedDateTime
                .ofInstant(Instant.ofEpochMilli(epochMillis), zoneId)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
