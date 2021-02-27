package it.luca.lgd.model;

import it.luca.lgd.oozie.job.WorkflowJobType;
import lombok.Builder;
import lombok.Getter;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@Builder
@Getter
public class OozieJobRecord {

    private final String jobLauncherId;
    private final String jobType;
    private final String jobName;
    private final String jobStatus;
    private final LocalDate jobStartDate;
    private final LocalDateTime jobStartTime;
    private final LocalDate jobEndDate;
    private final LocalDateTime jobEndTime;
    private final Long jobActions;
    private final Long jobCompletedActions;
    private final String jobTrackingUrl;
    private final String jobUser;
    private final LocalDateTime recordInsertTime;
    private final LocalDateTime lastRecordUpdateTime;

    public static OozieJobRecord fromWorkflowJob(WorkflowJob workflowJob) {

        Function<java.util.Date, LocalDate> toLocalDate = date -> Optional.ofNullable(date)
                .map(d -> d.toInstant().atZone(ZoneId.systemDefault()).toLocalDate())
                .orElse(null);

        Function<java.util.Date, LocalDateTime> toLocalDateTime = date -> Optional.ofNullable(date)
                .map(d -> LocalDateTime.ofInstant(d.toInstant(), ZoneId.systemDefault()))
                .orElse(null);

        List<WorkflowAction.Status> completedStatuses = Arrays.asList(WorkflowAction.Status.DONE,
                WorkflowAction.Status.FAILED,
                WorkflowAction.Status.KILLED,
                WorkflowAction.Status.OK);

        return OozieJobRecord.builder()
                .jobLauncherId(workflowJob.getId())
                .jobType(WorkflowJobType.WORKFLOW_JOB.getType())
                .jobName(workflowJob.getAppName())
                .jobStatus(workflowJob.getStatus().toString())
                .jobStartDate(toLocalDate.apply(workflowJob.getStartTime()))
                .jobStartTime(toLocalDateTime.apply(workflowJob.getStartTime()))
                .jobEndDate(toLocalDate.apply(workflowJob.getEndTime()))
                .jobEndTime(toLocalDateTime.apply(workflowJob.getEndTime()))
                .jobActions((long) workflowJob.getActions().size())
                .jobCompletedActions(workflowJob.getActions().stream()
                        .filter(workflowAction -> completedStatuses.contains(workflowAction.getStatus()))
                        .count())
                .jobTrackingUrl(workflowJob.getConsoleUrl())
                .jobUser(workflowJob.getUser())
                .recordInsertTime(LocalDateTime.now())
                .lastRecordUpdateTime(LocalDateTime.now())
                .build();
    }
}
