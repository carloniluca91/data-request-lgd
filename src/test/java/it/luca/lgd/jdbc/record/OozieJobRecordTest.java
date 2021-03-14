package it.luca.lgd.jdbc.record;

import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

class OozieJobRecordTest extends OozieRecordTest {

    private static final String WORKFLOW_APP_NAME = "workflowAppName";
    private static final String WORKFLOW_APP_PATH = "workflowAppPath";
    private static final List<WorkflowAction> WORKFLOW_ACTIONS = Collections.emptyList();
    private static final WorkflowJob.Status WORKFLOW_STATUS = WorkflowJob.Status.SUCCEEDED;
    private static final LocalDateTime WORKFLOW_START_TIME = LocalDateTime.now().minusMinutes(10);
    private static final LocalDateTime WORKFLOW_END_TIME = LocalDateTime.now();
    private static final String WORKFLOW_TRACKING_URL = "workflowTrackingUrl";

    @BeforeAll
    public static void init() {

        when(workflowJob.getId()).thenReturn(WORKFLOW_ID);
        when(workflowJob.getAppName()).thenReturn(WORKFLOW_APP_NAME);
        when(workflowJob.getAppPath()).thenReturn(WORKFLOW_APP_PATH);
        when(workflowJob.getActions()).thenReturn(WORKFLOW_ACTIONS);
        when(workflowJob.getStatus()).thenReturn(WORKFLOW_STATUS);
        when(workflowJob.getStartTime()).thenReturn(toDate.apply(WORKFLOW_START_TIME));
        when(workflowJob.getEndTime()).thenReturn(toDate.apply(WORKFLOW_END_TIME));
        when(workflowJob.getConsoleUrl()).thenReturn(WORKFLOW_TRACKING_URL);
    }

    @Test
    public void fromWorkflowJob() {

        OozieJobRecord oozieJobRecord = OozieJobRecord.from(workflowJob);
        assertEquals(WORKFLOW_ID, oozieJobRecord.getJobLauncherId());
        assertEquals(WORKFLOW_APP_NAME, oozieJobRecord.getJobName());
        assertEquals(WORKFLOW_APP_PATH, oozieJobRecord.getJobAppPath());
        assertEquals(WORKFLOW_ACTIONS.size(), oozieJobRecord.getJobTotalActions());
        assertEquals(WORKFLOW_STATUS.toString(), oozieJobRecord.getJobFinishStatus());
        assertEquals(WORKFLOW_START_TIME, oozieJobRecord.getJobStartTime());
        assertEquals(WORKFLOW_START_TIME.toLocalDate(), oozieJobRecord.getJobStartDate());
        assertEquals(WORKFLOW_END_TIME, oozieJobRecord.getJobEndTime());
        assertEquals(WORKFLOW_END_TIME.toLocalDate(), oozieJobRecord.getJobEndDate());
        assertEquals(WORKFLOW_TRACKING_URL, oozieJobRecord.getJobTrackingUrl());
        assertNotNull(oozieJobRecord.getTsInsert());
        assertNotNull(oozieJobRecord.getDtInsert());
    }
}