package it.luca.lgd.model.parameters;

import it.luca.lgd.oozie.WorkflowJobId;
import it.luca.lgd.oozie.WorkflowJobParameter;
import it.luca.lgd.utils.Tuple2;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;
import java.util.stream.Collectors;

@Getter
@AllArgsConstructor
public abstract class JobParameters {

    protected final WorkflowJobId workflowJobId;

    public abstract Tuple2<Boolean, String> validate();

    public abstract Map<WorkflowJobParameter, String> toMap();

    protected String asString() {

        return toMap().entrySet().stream()
                .map(e -> String.format("%s = %s", e.getKey(), e.getValue()))
                .collect(Collectors.joining(", "));
    }

    @Override
    public String toString() {
        return asString();
    }
}
