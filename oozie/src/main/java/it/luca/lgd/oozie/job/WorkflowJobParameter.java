package it.luca.lgd.oozie.job;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.oozie.client.OozieClient;

@Getter
@AllArgsConstructor
public enum WorkflowJobParameter {

    // COMMON PARAMETERS
    END_DATE("end_date"),
    PIG_SCRIPT_PATH("pigScript"),
    START_DATE("start_date"),
    WORKFLOW_NAME("workflowName"),
    WORKFLOW_PATH(OozieClient.APP_PATH),

    // CICLILAV_STEP1
    CICLILAV_STEP1_WORKFLOW("ciclilavStep1_workflow"),
    CICLILAV_STEP1_PIG("ciclilavStep1_pig"),

    // FPASPERD
    FPASPERD_WORKFLOW("fpasperd_workflow"),
    FPASPERD_PIG("fpasperd_pig");

    private final String name;
}
