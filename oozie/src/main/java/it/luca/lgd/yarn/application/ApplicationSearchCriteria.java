package it.luca.lgd.yarn.application;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.function.Predicate;

@Getter
@AllArgsConstructor
public class ApplicationSearchCriteria {

    private final long startTime = System.currentTimeMillis();
    private final String userName;
    private final ApplicationType applicationType;
    private final Predicate<String> applicationNamePredicate;
    private final String applicationQueue;
}
