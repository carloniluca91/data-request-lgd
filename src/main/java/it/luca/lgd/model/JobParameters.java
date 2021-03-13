package it.luca.lgd.model;

import it.luca.lgd.oozie.WorkflowJobParameter;
import it.luca.lgd.utils.Tuple2;

import java.util.Map;
import java.util.stream.Collectors;

public interface JobParameters {

    Tuple2<Boolean, String> validate();

    Map<WorkflowJobParameter, String> toMap();

    default String asString() {

        return toMap().entrySet().stream()
                .map(e -> String.format("%s = %s", e.getKey(), e.getValue()))
                .collect(Collectors.joining(", "));
    }
}
