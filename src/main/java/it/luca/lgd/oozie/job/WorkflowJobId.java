package it.luca.lgd.oozie.job;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum WorkflowJobId {

    CICLILAV_STEP1("CICLILAV_STEP1", "Job # 1"),
    FPASPERD("FPASPERD", "Job # 2");

    private final String id;
    private final String description;
}
