package it.luca.lgd.oozie.utils;

import it.luca.lgd.oozie.job.WorkflowJobParameter;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JobPropertiesTest {

    @Test
    public void testCopyOf() {

        PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
        propertiesConfiguration.setProperty("a.b", "valueOfa.b");
        propertiesConfiguration.setProperty("c.d", "valueOfc.d");

        JobProperties jobProperties = JobProperties.copyOf(propertiesConfiguration);
        //noinspection unchecked
        propertiesConfiguration.getKeys().forEachRemaining(keyObject -> {
            String keyString = (String) keyObject;
            assertTrue(jobProperties.containsKey(keyString));
            assertEquals(jobProperties.getProperty(keyString), propertiesConfiguration.getString(keyString));
        });
    }

    @Test
    public void testSetParameters() {

        Map<WorkflowJobParameter, String> parameterMap = new HashMap<WorkflowJobParameter, String>(){{
            put(WorkflowJobParameter.START_DATE, "startDate");
            put(WorkflowJobParameter.END_DATE, "endDate");
        }};

        JobProperties jobProperties = new JobProperties();
        jobProperties.setParameters(parameterMap);
        parameterMap.forEach(((workflowJobParameter, s) -> {
            assertTrue(jobProperties.containsKey(workflowJobParameter.getName()));
            assertEquals(jobProperties.getProperty(workflowJobParameter.getName()), s);
        }));
    }

    @Test
    public void testSetParameter() {

        WorkflowJobParameter workflowJobParameter = WorkflowJobParameter.WORKFLOW_PATH;
        String value = "workflowPath";
        JobProperties jobProperties = new JobProperties();
        jobProperties.setParameter(workflowJobParameter, value);
        assertTrue(jobProperties.containsKey(workflowJobParameter.getName()));
        assertEquals(jobProperties.getProperty(workflowJobParameter.getName()), value);
    }
}