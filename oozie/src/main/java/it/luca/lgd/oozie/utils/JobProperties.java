package it.luca.lgd.oozie.utils;

import it.luca.lgd.oozie.job.WorkflowJobParameter;

import java.util.Properties;

public class JobProperties extends Properties {

    public void setProperty(WorkflowJobParameter workflowJobParameter, String value) {
        super.setProperty(workflowJobParameter.getName(), value);
    }

    public String printProperties() {

        StringBuilder stringBuilder = new StringBuilder("\n\n");
        super.keySet().forEach(o -> {
            String key = (String) o;
            stringBuilder.append(String.format("      %s = %s\n", key, super.getProperty(key)));
        });

        return stringBuilder.toString();
    }
}
