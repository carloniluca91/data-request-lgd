package it.luca.lgd.jdbc.record;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.oozie.client.WorkflowJob;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static it.luca.lgd.utils.Java8Utils.orNull;
import static it.luca.lgd.utils.TimeUtils.toLocalDate;
import static it.luca.lgd.utils.TimeUtils.toLocalDateTime;

@Getter
@Setter
@NoArgsConstructor
public class OozieJobRecord extends BaseRecord {

    private String jobLauncherId;
    private String jobName;
    private String jobAppPath;
    private Integer jobTotalActions;
    private String jobFinishStatus;
    private LocalDateTime jobStartTime;
    private LocalDate jobStartDate;
    private LocalDateTime jobEndTime;
    private LocalDate jobEndDate;
    private String jobTrackingUrl;

    public static OozieJobRecord from(WorkflowJob workflowJob) {

        OozieJobRecord oozieJobRecord = new OozieJobRecord();
        oozieJobRecord.setJobLauncherId(workflowJob.getId());
        oozieJobRecord.setJobName(workflowJob.getAppName());
        oozieJobRecord.setJobAppPath(workflowJob.getAppPath());
        oozieJobRecord.setJobTotalActions(orNull(workflowJob.getActions(), List::size));
        oozieJobRecord.setJobFinishStatus(workflowJob.getStatus().toString());
        oozieJobRecord.setJobStartTime(toLocalDateTime(workflowJob.getStartTime()));
        oozieJobRecord.setJobStartDate(toLocalDate(workflowJob.getStartTime()));
        oozieJobRecord.setJobEndTime(toLocalDateTime(workflowJob.getEndTime()));
        oozieJobRecord.setJobEndDate(toLocalDate(workflowJob.getEndTime()));
        oozieJobRecord.setJobTrackingUrl(workflowJob.getConsoleUrl());
        oozieJobRecord.setTsInsert(LocalDateTime.now());
        oozieJobRecord.setDtInsert(LocalDate.now());
        return oozieJobRecord;
    }
}
