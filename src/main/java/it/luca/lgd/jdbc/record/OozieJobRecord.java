package it.luca.lgd.jdbc.record;

import it.luca.lgd.oozie.OozieJobType;
import it.luca.lgd.utils.TimeUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@Entity
@Table(schema = "oozie", name = "oozie_job")
@NoArgsConstructor
public class OozieJobRecord extends DRLGDRecord {

    @Id private String jobLauncherId;
    private String jobType;
    private String jobName;
    private String jobFinishStatus;
    private Date jobStartDate;
    private Timestamp jobStartTime;
    private Date jobEndDate;
    private Timestamp jobEndTime;
    private int jobTotalActions;
    private int jobCompletedActions;
    private String jobTrackingUrl;

    public static OozieJobRecord from(WorkflowJob workflowJob) {

        List<WorkflowAction.Status> completedStatuses = Arrays.asList(WorkflowAction.Status.DONE,
                WorkflowAction.Status.FAILED,
                WorkflowAction.Status.KILLED,
                WorkflowAction.Status.OK);

        OozieJobRecord oozieJobRecord = new OozieJobRecord();
        oozieJobRecord.setJobLauncherId(workflowJob.getId());
        oozieJobRecord.setJobType(OozieJobType.WORKFLOW.getType());
        oozieJobRecord.setJobName(workflowJob.getAppName());
        oozieJobRecord.setJobFinishStatus(workflowJob.getStatus().toString());
        oozieJobRecord.setJobStartDate(TimeUtils.fromUtilDateToSqlDate(workflowJob.getStartTime()));
        oozieJobRecord.setJobStartTime(TimeUtils.fromUtilDateToSqlTimestamp(workflowJob.getStartTime()));
        oozieJobRecord.setJobEndDate(TimeUtils.fromUtilDateToSqlDate(workflowJob.getEndTime()));
        oozieJobRecord.setJobEndTime(TimeUtils.fromUtilDateToSqlTimestamp(workflowJob.getEndTime()));
        oozieJobRecord.setJobTotalActions(workflowJob.getActions().size());
        oozieJobRecord.setJobCompletedActions((int) workflowJob.getActions().stream()
                .filter(workflowAction -> completedStatuses.contains(workflowAction.getStatus()))
                .count());

        oozieJobRecord.setJobTrackingUrl(workflowJob.getConsoleUrl());
        oozieJobRecord.setTsInsert(new Timestamp(System.currentTimeMillis()));
        oozieJobRecord.setDtInsert(new Date(System.currentTimeMillis()));
        return oozieJobRecord;
    }

    @Override
    public Object[] primaryKeyValues() {
        return new Object[]{jobLauncherId};
    }

    @Override
    public Object[] allValues() {

        return new Object[] {jobLauncherId, jobType, jobName, jobFinishStatus, jobStartDate, jobStartTime,
                jobEndDate, jobEndTime, jobTotalActions, jobCompletedActions, jobTrackingUrl,
                tsInsert, dtInsert
        };
    }
}
