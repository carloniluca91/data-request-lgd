package it.luca.lgd.model.parameters;

import it.luca.lgd.oozie.WorkflowJobId;
import it.luca.lgd.oozie.WorkflowJobParameter;
import it.luca.lgd.utils.Tuple2;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;

@Getter
@AllArgsConstructor
public abstract class JobParameters {

    protected final WorkflowJobId workflowJobId;

    public abstract Tuple2<Boolean, String> areValid();

    public abstract Map<WorkflowJobParameter, String> toMap();

    protected abstract String asString();

    @Override
    public String toString() {
        return asString();
    }
}
