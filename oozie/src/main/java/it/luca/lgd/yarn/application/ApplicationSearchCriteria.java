package it.luca.lgd.yarn.application;

import it.luca.lgd.yarn.utils.TimeUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.ZoneId;
import java.util.function.Predicate;

@Getter
@AllArgsConstructor
public abstract class ApplicationSearchCriteria {

    private final long startTime = System.currentTimeMillis();
    private final String user;
    private final YarnApplicationType applicationType;
    private final Predicate<String> applicationNamePredicate;
    private final String queue;

    @Override
    public String toString() {

        ZoneId zoneId = TimeUtils.romeZoneId();
        return String.format("appType = %s, user = '%s', queue = '%s', startTime > %s (%s @ %s)",
                applicationType.getType(),
                user,
                queue,
                startTime,
                TimeUtils.epochMillsToZonedTimestamp(startTime, zoneId),
                zoneId.getId());
    }
}
