package it.luca.lgd.oozie;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum WorkflowJobId {

    CICLILAV_STEP1("CICLILAV_STEP1"),
    FPASPERD("FPASPERD");

    private final String id;
}