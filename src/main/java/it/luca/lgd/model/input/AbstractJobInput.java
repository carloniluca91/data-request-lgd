package it.luca.lgd.model.input;

import it.luca.lgd.oozie.job.WorkflowJobId;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public abstract class AbstractJobInput {

    protected final WorkflowJobId workflowJobId;

    public abstract boolean isValid();

    protected abstract String asString();

    @Override
    public String toString() {
        return asString();
    }
}
