package it.luca.lgd.jdbc.model;

import it.luca.lgd.oozie.OozieJobType;
import it.luca.lgd.utils.TimeUtils;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Data
@Entity
@Table(schema = "oozie", name = "oozie_job")
@NoArgsConstructor
public class OozieJobRecord implements DRLGDRecord, Serializable {

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
        oozieJobRecord.setJobStartDate(TimeUtils.fromUtilDateToLocalDate(workflowJob.getStartTime()));
        oozieJobRecord.setJobStartTime(TimeUtils.fromUtilDateToLocalDateTime(workflowJob.getStartTime()));
        oozieJobRecord.setJobEndDate(TimeUtils.fromUtilDateToLocalDate(workflowJob.getEndTime()));
        oozieJobRecord.setJobEndTime(TimeUtils.fromUtilDateToLocalDateTime(workflowJob.getEndTime()));
        oozieJobRecord.setJobTotalActions(workflowJob.getActions().size());
        oozieJobRecord.setJobCompletedActions((int) workflowJob.getActions().stream()
                .filter(workflowAction -> completedStatuses.contains(workflowAction.getStatus()))
                .count());

        oozieJobRecord.setJobTrackingUrl(workflowJob.getConsoleUrl());
        oozieJobRecord.setRecordInsertTime(LocalDateTime.now());
        oozieJobRecord.setLastRecordUpdateTime(LocalDateTime.now());
        return oozieJobRecord;
    }

    @Override
    public Object[] primaryKeyValues() {
        return new Object[]{jobLauncherId};
    }

    @Override
    public Object[] allValues() {

        return new Object[] {jobLauncherId, jobType, jobName, jobUser, jobStatus, jobStartDate, jobStartTime,
                jobEndDate, jobEndTime, jobTotalActions, jobCompletedActions, jobTrackingUrl,
                recordInsertTime, lastRecordUpdateTime
        };
    }
}
