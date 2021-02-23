package it.luca.lgd.oozie.exception;

import it.luca.lgd.oozie.job.WorkflowJobParameter;

public class IllegalWorkflowParameterException extends Exception {

    public IllegalWorkflowParameterException(WorkflowJobParameter workflowJobParameter) {

        super(String.format("Undefined %s ('%s'). Check content of original job.properties file",
                WorkflowJobParameter.class.getSimpleName(),
                workflowJobParameter.getName()));
    }
}
