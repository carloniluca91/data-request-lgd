package it.luca.lgd.exception;

import it.luca.lgd.oozie.WorkflowJobParameter;

public class IllegalWorkflowParameterException extends Exception {

    public IllegalWorkflowParameterException(WorkflowJobParameter workflowJobParameter) {

        super(String.format("Undefined %s ('%s'). Check content of original job.properties file",
                WorkflowJobParameter.class.getSimpleName(),
                workflowJobParameter.getName()));
    }
}
