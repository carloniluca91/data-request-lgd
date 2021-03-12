package it.luca.lgd.oozie;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.oozie.client.OozieClient;

@Getter
@AllArgsConstructor
public enum WorkflowJobParameter {

    // common parameters
    END_DATE("endDate"),
    START_DATE("startDate"),
    IATA_CODE("iataCode"),
    WORKFLOW_NAME("workflowName"),
    WORKFLOW_PATH(OozieClient.APP_PATH),

    // CANCELLED_FLIGHTS
    CANCELLED_FLIGHTS_APP_PATH("cancelledFlights_appPath"),

    // FPASPERD
    FPASPERD_WORKFLOW("fpasperd_workflow"),
    FPASPERD_PIG("fpasperd_pig");

    private final String name;
}
