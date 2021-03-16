package it.luca.lgd.utils;

import it.luca.lgd.oozie.WorkflowJobParameter;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.Map;
import java.util.Properties;

public class JobProperties extends Properties {

    /**
     * Creates JobProperties object containing all key-value pairs of input object
     * @param propertiesConfiguration: input properties
     * @return object containing all key-value pairs within input object
     */

    public static JobProperties copyOf(PropertiesConfiguration propertiesConfiguration) {

        JobProperties jobProperties = new JobProperties();
        //noinspection unchecked
        propertiesConfiguration.getKeys().forEachRemaining(o -> {
            String key = (String) o;
            jobProperties.setProperty(key, propertiesConfiguration.getString(key));
        });

        return jobProperties;
    }

    /**
     * Adds each key-value pair of given map
     * @param parameterMap: input map
     */

    public void setParameters(Map<WorkflowJobParameter, String> parameterMap) {
        parameterMap.forEach(((workflowJobParameter, s) -> super.setProperty(workflowJobParameter.getName(), s)));
    }

    /**
     * Adds new key-value pair
     * @param workflowJobParameter: parameter (key)
     * @param value: value
     */

    public void setParameter(WorkflowJobParameter workflowJobParameter, String value) {
        super.setProperty(workflowJobParameter.getName(), value);
    }

    /**
     * Returns a string representation of all stored key-value pairs
     * @return all key-value pairs
     */

    public String getPropertiesReport() {

        StringBuilder stringBuilder = new StringBuilder("\n\n");
        super.keySet().forEach(o -> {
            String key = (String) o;
            stringBuilder.append(String.format("      %s = %s\n", key, super.getProperty(key)));
        });

        return stringBuilder.toString();
    }
}
