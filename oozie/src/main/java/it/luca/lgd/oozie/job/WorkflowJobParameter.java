package it.luca.lgd.oozie.job;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum WorkflowJobParameter {

    // COMMON PARAMETERS
    END_DATE("end_date"),
    PIG_SCRIPT("pigScript"),
    WORKFLOW_NAME("workflowName"),
    START_DATE("start_date"),

    // CICLILAV_STEP1
    CICLILAV_STEP1_WORKFLOW_PATH("ciclilavStep1_workflow"),
    CICLILAV_STEP1_PIG_SCRIPT_PATH("ciclilavStep1_pigScript");

    private final String name;
}
