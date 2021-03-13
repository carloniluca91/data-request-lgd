package it.luca.lgd.exception;

import it.luca.lgd.oozie.WorkflowJobLabel;

public class IllegalWorkflowIdException extends RuntimeException {

    public IllegalWorkflowIdException(WorkflowJobLabel workflowJobLabel) {

        super(String.format("Illegal %s (%s)", WorkflowJobLabel.class.getSimpleName(), workflowJobLabel.getId()));
    }
}
