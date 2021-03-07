package it.luca.lgd.oozie;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum WorkflowJobId {

    CANCELLED_FLIGHTS("CANCELLED_FLIGHTS"),
    FPASPERD("FPASPERD");

    private final String id;
}
