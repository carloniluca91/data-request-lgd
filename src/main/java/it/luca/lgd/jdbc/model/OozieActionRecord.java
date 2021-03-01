package it.luca.lgd.jdbc.model;

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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Data
@Entity
@Table(schema = "oozie", name = "oozie_action")
@NoArgsConstructor
public class OozieActionRecord implements Serializable {

    @Id private String jobLauncherId;
    @Id private String actionId;
    private String actionType;
    private String actionName;
    private int actionNumber;
    private String actionStatus;
    private String actionChildId;
    private String actionChildYarnApplicationId;
    private LocalDate actionStartDate;
    private LocalDateTime actionStartTime;
    private LocalDate actionEndDate;
    private LocalDateTime actionEndTime;
    private String actionErrorCode;
    private String actionErrorMessage;
    private String actionTrackingUrl;
    private LocalDateTime recordInsertTime;
    private LocalDateTime lastRecordUpdateTime;

    public List<OozieActionRecord> fromWorkflowJob(WorkflowJob workflowJob) {

        return IntStream.range(0, workflowJob.getActions().size())
                .mapToObj(i -> {

                    WorkflowAction workflowAction = workflowJob.getActions().get(i);
                    OozieActionRecord oozieActionRecord = new OozieActionRecord();

                    oozieActionRecord.setJobLauncherId(workflowJob.getId());
                    oozieActionRecord.setActionId(workflowAction.getId());
                    oozieActionRecord.setActionType(workflowAction.getType());
                    oozieActionRecord.setActionName(workflowAction.getName());
                    oozieActionRecord.setActionNumber(i + 1);
                    oozieActionRecord.setActionStatus(workflowAction.getStatus().toString());
                    oozieActionRecord.setActionChildId(workflowAction.getExternalChildIDs());
                    oozieActionRecord.setActionChildYarnApplicationId(workflowAction.getExternalChildIDs().replace("job", "application"));
                    oozieActionRecord.setActionStartDate(TimeUtils.fromUtilDateToLocalDate(workflowAction.getStartTime()));
                    oozieActionRecord.setActionStartTime(TimeUtils.fromUtilDateToLocalDateTime(workflowAction.getStartTime()));
                    oozieActionRecord.setActionEndDate(TimeUtils.fromUtilDateToLocalDate(workflowAction.getEndTime()));
                    oozieActionRecord.setActionEndTime(TimeUtils.fromUtilDateToLocalDateTime(workflowAction.getEndTime()));
                    oozieActionRecord.setActionErrorCode(workflowAction.getErrorCode());
                    oozieActionRecord.setActionErrorMessage(workflowAction.getErrorMessage());
                    oozieActionRecord.setActionTrackingUrl(workflowAction.getTrackerUri());
                    oozieActionRecord.setRecordInsertTime(LocalDateTime.now());
                    oozieActionRecord.setLastRecordUpdateTime(LocalDateTime.now());

                    return oozieActionRecord;
                }).collect(Collectors.toList());
    }
}
