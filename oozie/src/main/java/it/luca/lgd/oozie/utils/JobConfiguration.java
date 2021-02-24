package it.luca.lgd.oozie.utils;

import it.luca.lgd.oozie.exception.IllegalWorkflowParameterException;
import it.luca.lgd.oozie.job.WorkflowJobParameter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.Optional;

public class JobConfiguration extends PropertiesConfiguration {

    public JobConfiguration(String fileName) throws ConfigurationException {
        super(fileName);
    }

    public String getParameter(WorkflowJobParameter workflowJobParameter) throws IllegalWorkflowParameterException {
        return Optional.ofNullable(super.getString(workflowJobParameter.getName()))
                .orElseThrow(() -> new IllegalWorkflowParameterException(workflowJobParameter));
    }
}
