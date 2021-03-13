package it.luca.lgd.oozie;

import it.luca.lgd.model.CancelledFlightsParameters;
import it.luca.lgd.model.JobParameters;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Getter
@AllArgsConstructor
public enum WorkflowJobLabel {

    CANCELLED_FLIGHTS("CANCELLED_FLIGHTS", CancelledFlightsParameters.class),
    FPASPERD("FPASPERD", CancelledFlightsParameters.class);

    private final String id;
    private final Class<? extends JobParameters> parameterClass;

    private static final Map<String, WorkflowJobLabel> map = new HashMap<>();
    static { Arrays.stream(WorkflowJobLabel.values()).forEach(v -> map.put(v.getId(), v)); }

    public static WorkflowJobLabel withId(String id) {
        return map.get(id);
    }
}
