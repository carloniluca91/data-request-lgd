package it.luca.lgd.model;

import it.luca.lgd.oozie.WorkflowJobParameter;
import it.luca.lgd.utils.Tuple2;

import java.util.Map;
import java.util.stream.Collectors;

public abstract class JobParameters {

    protected static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

    public abstract Tuple2<Boolean, String> validate();

    public abstract Map<WorkflowJobParameter, String> toMap();

    public String asString() {

        return toMap().entrySet().stream()
                .map(e -> String.format("%s = %s", e.getKey(), e.getValue()))
                .collect(Collectors.joining(", "));
    }
}
