package it.luca.lgd.oozie;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Getter
@AllArgsConstructor
public enum WorkflowJobId {

    CANCELLED_FLIGHTS("CANCELLED_FLIGHTS"),
    FPASPERD("FPASPERD");

    private final String id;

    private static final Map<String, WorkflowJobId> map = new HashMap<>();
    static { Arrays.stream(WorkflowJobId.values()).forEach(v -> map.put(v.getId(), v)); }

    public static WorkflowJobId withId(String id) {
        return map.get(id);
    }
}
