package it.luca.lgd.model.input;

import it.luca.lgd.oozie.job.WorkflowJobId;
import it.luca.lgd.utils.Tuple2;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public abstract class AbstractJobInput {

    protected final WorkflowJobId workflowJobId;

    public abstract Tuple2<Boolean, String> isValid();

    protected abstract String asString();

    @Override
    public String toString() {
        return asString();
    }
}
