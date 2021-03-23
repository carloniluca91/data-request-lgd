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

    // FLIGHT_DETAILS
    FLIGHT_DETAILS_APP_PATH("flightDetails_appPath"),

    // CANCELLED_FLIGHTS
    CANCELLED_FLIGHTS_APP_PATH("cancelledFlights_appPath"),

    // MONTHLY_GROUPED_DELAYS
    MONTHLY_GROUPED_DELAYS_APP_PATH("monthlyGroupedDelays_appPath"),
    AIRLINE_IATAS("airlineIatas");

    private final String name;
}
