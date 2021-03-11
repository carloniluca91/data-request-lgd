package it.luca.lgd.oozie;

import lombok.Getter;
import org.apache.oozie.client.WorkflowJob;

import java.util.Arrays;
import java.util.List;

@Getter
public enum WorkflowJobStatuses {

    COMPLETED(WorkflowJob.Status.SUCCEEDED, WorkflowJob.Status.FAILED, WorkflowJob.Status.KILLED),
    NOT_COMPLETED(WorkflowJob.Status.PREP, WorkflowJob.Status.RUNNING, WorkflowJob.Status.SUSPENDED);

    private final List<WorkflowJob.Status> statuses;

    WorkflowJobStatuses(WorkflowJob.Status... statuses) {
        this.statuses = Arrays.asList(statuses);
    }

    public boolean contains(WorkflowJob.Status status) {
        return statuses.contains(status);
    }
}
