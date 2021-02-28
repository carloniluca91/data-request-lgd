package it.luca.lgd.utils;

import it.luca.lgd.exception.IllegalWorkflowParameterException;
import it.luca.lgd.oozie.WorkflowJobParameter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobConfigurationTest {

    private final JobConfiguration jobConfiguration = new JobConfiguration();

    @Test
    public void testGetParameterException() {

        WorkflowJobParameter workflowJobParameter = WorkflowJobParameter.WORKFLOW_PATH;
        assertFalse(jobConfiguration.containsKey(workflowJobParameter.getName()));
        assertThrows(IllegalWorkflowParameterException.class, () -> jobConfiguration.getParameter(workflowJobParameter));
    }
}