package it.luca.lgd.oozie.job;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.oozie.client.OozieClient;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
public enum WorkflowJobParameter {

    // common parameters
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

    @Getter
    private final String name;

    private static final Map<String, WorkflowJobParameter> map = new HashMap<>();
    static {
        Arrays.stream(WorkflowJobParameter.values())
                .forEach(workflowJobParameter ->
                        map.put(workflowJobParameter.getName(), workflowJobParameter));
    }

    public static WorkflowJobParameter withName(String name) {
        return map.get(name);
    }
}
