package it.luca.lgd.oozie;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.oozie.client.OozieClient;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Getter
@AllArgsConstructor
public enum WorkflowJobParameter {

    // common parameters
    END_DATE("end_date"),
    START_DATE("start_date"),
    IATA_CODE("iata_code"),
    WORKFLOW_NAME("workflowName"),
    WORKFLOW_PATH(OozieClient.APP_PATH),

    // CANCELLED_FLIGHTS
    CANCELLED_FLIGHTS_APP_PATH("ciclilavStep1_workflow"),

    // FPASPERD
    FPASPERD_WORKFLOW("fpasperd_workflow"),
    FPASPERD_PIG("fpasperd_pig");

    private final String name;
}
