package it.luca.lgd.exception;

import it.luca.lgd.oozie.WorkflowJobParameter;

public class IllegalWorkflowParameterException extends RuntimeException {

    public IllegalWorkflowParameterException(WorkflowJobParameter workflowJobParameter) {

        super(String.format("Illegal %s ('%s'). Check content of original job.properties file",
                WorkflowJobParameter.class.getSimpleName(),
                workflowJobParameter.getName()));
    }
}
