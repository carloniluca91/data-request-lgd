package it.luca.lgd.oozie.utils;

import it.luca.lgd.oozie.job.WorkflowJobParameter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class JobConfiguration extends PropertiesConfiguration {

    public JobConfiguration(String fileName) throws ConfigurationException {
        super(fileName);
    }

    public String getString(WorkflowJobParameter workflowJobParameter) {
        return super.getString(workflowJobParameter.getName());
    }
}
