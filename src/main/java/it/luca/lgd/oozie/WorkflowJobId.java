package it.luca.lgd.oozie;

import it.luca.lgd.model.parameters.CancelledFlightsParameters;
import it.luca.lgd.model.parameters.JobParameters;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Getter
@AllArgsConstructor
public enum WorkflowJobId {

    CANCELLED_FLIGHTS("CANCELLED_FLIGHTS", CancelledFlightsParameters.class),
    FPASPERD("FPASPERD", CancelledFlightsParameters.class);

    private final String id;
    private final Class<? extends JobParameters> parameterClass;

    private static final Map<String, WorkflowJobId> map = new HashMap<>();
    static { Arrays.stream(WorkflowJobId.values()).forEach(v -> map.put(v.getId(), v)); }

    public static WorkflowJobId withId(String id) {
        return map.get(id);
    }
}
