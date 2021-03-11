package it.luca.lgd.exception;

import it.luca.lgd.oozie.WorkflowJobId;

public class IllegalWorkflowIdException extends RuntimeException {

    public IllegalWorkflowIdException(WorkflowJobId workflowJobId) {

        super(String.format("Illegal %s (%s)", WorkflowJobId.class.getSimpleName(), workflowJobId.getId()));
    }
}
