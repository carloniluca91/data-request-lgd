package it.luca.lgd.model.jdbc;

import it.luca.lgd.oozie.OozieJobType;
import lombok.Data;
import lombok.NoArgsConstructor;
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

@Data
@Entity
@Table(schema = "oozie", name = "oozie_job")
@NoArgsConstructor
public class OozieJobRecord {

    @Id private String jobLauncherId;
    private String jobType;
    private String jobName;
    private String jobUser;
    private String jobStatus;
    private LocalDate jobStartDate;
    private LocalDateTime jobStartTime;
    private LocalDate jobEndDate;
    private LocalDateTime jobEndTime;
    private int jobTotalActions;
    private int jobCompletedActions;
    private String jobTrackingUrl;
    private LocalDateTime recordInsertTime;
    private LocalDateTime lastRecordUpdateTime;

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

        OozieJobRecord oozieJobRecord = new OozieJobRecord();
        oozieJobRecord.setJobLauncherId(workflowJob.getId());
        oozieJobRecord.setJobType(OozieJobType.WORKFLOW.getType());
        oozieJobRecord.setJobName(workflowJob.getAppName());
        oozieJobRecord.setJobUser(workflowJob.getUser());
        oozieJobRecord.setJobStatus(workflowJob.getStatus().toString());
        oozieJobRecord.setJobStartDate(toLocalDate.apply(workflowJob.getStartTime()));
        oozieJobRecord.setJobStartTime(toLocalDateTime.apply(workflowJob.getStartTime()));
        oozieJobRecord.setJobEndDate(toLocalDate.apply(workflowJob.getEndTime()));
        oozieJobRecord.setJobEndTime(toLocalDateTime.apply(workflowJob.getEndTime()));
        oozieJobRecord.setJobTotalActions(workflowJob.getActions().size());
        oozieJobRecord.setJobCompletedActions((int) workflowJob.getActions().stream()
                .filter(workflowAction -> completedStatuses.contains(workflowAction.getStatus()))
                .count());

        oozieJobRecord.setJobTrackingUrl(workflowJob.getConsoleUrl());
        oozieJobRecord.setRecordInsertTime(LocalDateTime.now());
        oozieJobRecord.setLastRecordUpdateTime(LocalDateTime.now());
        return oozieJobRecord;
    }
}
