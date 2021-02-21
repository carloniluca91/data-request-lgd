package it.luca.lgd.oozie.utils;

import it.luca.lgd.oozie.job.WorkflowJobParameter;
import org.apache.commons.configuration.PropertiesConfiguration;

public class JobConfiguration extends PropertiesConfiguration {

    public String getString(WorkflowJobParameter workflowJobParameter) {
        return super.getString(workflowJobParameter.getName());
    }
}
