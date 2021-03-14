package it.luca.lgd.jdbc.record;

import org.apache.oozie.client.WorkflowJob;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.function.Function;

import static org.mockito.Mockito.mock;

public abstract class OozieRecordTest {

    protected static final WorkflowJob workflowJob = mock(WorkflowJob.class);
    protected static final String WORKFLOW_ID = "workflowId";
    protected static final Function<LocalDateTime, Date> toDate = localDateTime -> java.util.Date
            .from(localDateTime.atZone(ZoneOffset.systemDefault()).toInstant());
}
