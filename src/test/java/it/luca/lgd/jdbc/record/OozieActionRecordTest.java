package it.luca.lgd.jdbc.record;

import org.apache.oozie.client.WorkflowAction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OozieActionRecordTest extends OozieRecordTest {

    private static final WorkflowAction firstAction = mock(WorkflowAction.class);
    private static final WorkflowAction secondAction = mock(WorkflowAction.class);

    private static final String ACTION_TYPE = "PIG";

    private static final String FIRST_ACTION_ID = "a1";
    private static final String FIRST_ACTION_NAME = "a1 - PIG";
    private static final WorkflowAction.Status FIRST_ACTION_STATUS = WorkflowAction.Status.OK;
    private static final String FIRST_ACTION_CHILD_ID = "job_1,job_2";
    private static final LocalDateTime FIRST_ACTION_START_TIME = LocalDateTime.now().minusMinutes(5);
    private static final LocalDateTime FIRST_ACTION_END_TIME = LocalDateTime.now();

    private static final String SECOND_ACTION_ID = "a2";
    private static final String SECOND_ACTION_NAME = "a2 - PIG";
    private static final WorkflowAction.Status SECOND_ACTION_STATUS = WorkflowAction.Status.FAILED;
    private static final String SECOND_ACTION_ERROR_CODE = "ohiOhi";
    private static final String SECOND_ACTION_ERROR_MESSAGE = "ilBudello!";

    @BeforeAll
    public static void init() {

        when(workflowJob.getId()).thenReturn(WORKFLOW_ID);

        // Expected first action
        when(firstAction.getId()).thenReturn(FIRST_ACTION_ID);
        when(firstAction.getType()).thenReturn(ACTION_TYPE);
        when(firstAction.getName()).thenReturn(FIRST_ACTION_NAME);
        when(firstAction.getStatus()).thenReturn(FIRST_ACTION_STATUS);
        when(firstAction.getExternalChildIDs()).thenReturn(FIRST_ACTION_CHILD_ID);
        when(firstAction.getStartTime()).thenReturn(toDate.apply(FIRST_ACTION_START_TIME));
        when(firstAction.getEndTime()).thenReturn(toDate.apply(FIRST_ACTION_END_TIME));
        when(firstAction.getErrorCode()).thenReturn(null);
        when(firstAction.getErrorMessage()).thenReturn(null);

        // Expected second action (as its startDate is null)
        when(secondAction.getType()).thenReturn(ACTION_TYPE);
        when(secondAction.getId()).thenReturn(SECOND_ACTION_ID);
        when(secondAction.getName()).thenReturn(SECOND_ACTION_NAME);
        when(secondAction.getStatus()).thenReturn(SECOND_ACTION_STATUS);
        when(secondAction.getExternalChildIDs()).thenReturn(null);
        when(secondAction.getStartTime()).thenReturn(null);
        when(secondAction.getEndTime()).thenReturn(null);
        when(secondAction.getErrorCode()).thenReturn(SECOND_ACTION_ERROR_CODE);
        when(secondAction.getErrorMessage()).thenReturn(SECOND_ACTION_ERROR_MESSAGE);

        when(workflowJob.getActions()).thenReturn(Arrays.asList(secondAction, firstAction));
    }

    @Test
    public void batchFrom() {


        List<OozieActionRecord> oozieActionRecords = OozieActionRecord.batchFrom(workflowJob);
        assertEquals(2, oozieActionRecords.size());
        OozieActionRecord firstAction = oozieActionRecords.get(0);

        assertEquals(WORKFLOW_ID, firstAction.getJobLauncherId());
        assertEquals(FIRST_ACTION_ID, firstAction.getActionId());
        assertEquals(ACTION_TYPE, firstAction.getActionType());
        assertEquals(FIRST_ACTION_NAME, firstAction.getActionName());
        assertNotNull(firstAction.getActionNumber());
        assertEquals(FIRST_ACTION_STATUS.toString(), firstAction.getActionFinishStatus());
        assertEquals(FIRST_ACTION_CHILD_ID.split(",").length, firstAction.getActionChildId().size());
        assertTrue(firstAction.getActionChildId()
                .stream().allMatch(s -> s.startsWith("job_")));
        assertEquals(FIRST_ACTION_CHILD_ID.split(",").length, firstAction.getActionChildYarnApplicationId().size());
        assertTrue(firstAction.getActionChildYarnApplicationId()
                .stream().allMatch(s -> s.startsWith("application_")));

        assertEquals(FIRST_ACTION_START_TIME, firstAction.getActionStartTime());
        assertEquals(FIRST_ACTION_START_TIME.toLocalDate(), firstAction.getActionStartDate());
        assertEquals(FIRST_ACTION_END_TIME, firstAction.getActionEndTime());
        assertEquals(FIRST_ACTION_END_TIME.toLocalDate(), firstAction.getActionEndDate());
        assertNull(firstAction.getActionErrorCode());
        assertNull(firstAction.getActionErrorMessage());
        assertNotNull(firstAction.getTsInsert());
        assertNotNull(firstAction.getDtInsert());

        OozieActionRecord secondAction = oozieActionRecords.get(1);
        assertEquals(WORKFLOW_ID, secondAction.getJobLauncherId());
        assertEquals(SECOND_ACTION_ID, secondAction.getActionId());
        assertEquals(ACTION_TYPE, secondAction.getActionType());
        assertEquals(SECOND_ACTION_NAME, secondAction.getActionName());
        assertNotNull(secondAction.getActionNumber());
        assertEquals(SECOND_ACTION_STATUS.toString(), secondAction.getActionFinishStatus());

        assertNull(secondAction.getActionChildId());
        assertNull(secondAction.getActionChildYarnApplicationId());
        assertNull(secondAction.getActionStartTime());
        assertNull(secondAction.getActionStartDate());
        assertNull(secondAction.getActionEndTime());
        assertNull(secondAction.getActionEndDate());
        assertEquals(SECOND_ACTION_ERROR_CODE, secondAction.getActionErrorCode());
        assertEquals(SECOND_ACTION_ERROR_MESSAGE, secondAction.getActionErrorMessage());
        assertNotNull(firstAction.getTsInsert());
        assertNotNull(firstAction.getDtInsert());
    }
}