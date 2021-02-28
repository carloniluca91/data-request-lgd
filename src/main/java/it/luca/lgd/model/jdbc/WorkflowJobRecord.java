package it.luca.lgd.model.jdbc;

import it.luca.lgd.oozie.WorkflowJobType;
import lombok.*;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@Entity
@Table
@Data
@NoArgsConstructor
public class WorkflowJobRecord {

    @Id private String jobLauncherId;
    private String jobType;
    private String jobName;
    private String jobUser;
    private String jobStatus;
    private LocalDate jobStartDate;
    private LocalDateTime jobStartTime;
    private LocalDate jobEndDate;
    private LocalDateTime jobEndTime;
    private Long jobTotalActions;
    private Long jobCompletedActions;
    private String jobTrackingUrl;
    private LocalDateTime recordInsertTime;
    private LocalDateTime lastRecordUpdateTime;

    public static WorkflowJobRecord fromWorkflowJob(WorkflowJob workflowJob) {

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

        WorkflowJobRecord workflowJobRecord = new WorkflowJobRecord();
        workflowJobRecord.setJobLauncherId(workflowJob.getId());
        workflowJobRecord.setJobType(WorkflowJobType.WORKFLOW_JOB.getType());
        workflowJobRecord.setJobName(workflowJob.getAppName());
        workflowJobRecord.setJobUser(workflowJob.getUser());
        workflowJobRecord.setJobStatus(workflowJob.getStatus().toString());
        workflowJobRecord.setJobStartDate(toLocalDate.apply(workflowJob.getStartTime()));
        workflowJobRecord.setJobStartTime(toLocalDateTime.apply(workflowJob.getStartTime()));
        workflowJobRecord.setJobEndDate(toLocalDate.apply(workflowJob.getEndTime()));
        workflowJobRecord.setJobEndTime(toLocalDateTime.apply(workflowJob.getEndTime()));
        workflowJobRecord.setJobTotalActions((long) workflowJob.getActions().size());
        workflowJobRecord.setJobCompletedActions(workflowJob.getActions().stream()
                .filter(workflowAction -> completedStatuses.contains(workflowAction.getStatus()))
                .count());

        workflowJobRecord.setJobTrackingUrl(workflowJob.getConsoleUrl());
        workflowJobRecord.setRecordInsertTime(LocalDateTime.now());
        workflowJobRecord.setLastRecordUpdateTime(LocalDateTime.now());
        return workflowJobRecord;
    }
}
