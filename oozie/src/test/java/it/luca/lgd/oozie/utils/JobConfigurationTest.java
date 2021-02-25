package it.luca.lgd.oozie.utils;

import it.luca.lgd.oozie.exception.IllegalWorkflowParameterException;
import it.luca.lgd.oozie.job.WorkflowJobParameter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JobConfigurationTest {

    private final JobConfiguration jobConfiguration = new JobConfiguration();

    @Test
    public void testGetParameterException() {

        WorkflowJobParameter workflowJobParameter = WorkflowJobParameter.WORKFLOW_PATH;
        assertFalse(jobConfiguration.containsKey(workflowJobParameter.getName()));
        assertThrows(IllegalWorkflowParameterException.class, () -> jobConfiguration.getParameter(workflowJobParameter));
    }
}