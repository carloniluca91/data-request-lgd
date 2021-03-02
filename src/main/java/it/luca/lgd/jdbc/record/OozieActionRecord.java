package it.luca.lgd.jdbc.record;

import it.luca.lgd.utils.TimeUtils;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Data
@Entity
@Table(schema = "oozie", name = "oozie_action")
@NoArgsConstructor
public class OozieActionRecord implements DRLGDRecord, Serializable {

    @Id private String jobLauncherId;
    @Id private String actionId;
    private String actionType;
    private String actionName;
    private int actionNumber;
    private String actionStatus;
    private String actionChildId;
    private String actionChildYarnApplicationId;
    private Date actionStartDate;
    private Timestamp actionStartTime;
    private Date actionEndDate;
    private Timestamp actionEndTime;
    private String actionErrorCode;
    private String actionErrorMessage;
    private String actionTrackingUrl;
    private Timestamp recordInsertTime;
    private Timestamp lastRecordUpdateTime;

    public static List<OozieActionRecord> fromWorkflowJob(WorkflowJob workflowJob) {

        // Carefully order workflow actions
        List<WorkflowAction> workflowActions = workflowJob.getActions();
        List<WorkflowAction> orderedActions = workflowActions.stream()
                .filter(a -> Optional.ofNullable(a.getStartTime()).isPresent())
                .sorted(Comparator.comparing(WorkflowAction::getStartTime))
                .collect(Collectors.toList());

        orderedActions.addAll(workflowActions.stream()
                .filter(a -> !Optional.ofNullable(a.getStartTime()).isPresent())
                .collect(Collectors.toList()));

        return IntStream.range(0, orderedActions.size())
                .mapToObj(i -> {

                    WorkflowAction workflowAction = orderedActions.get(i);
                    OozieActionRecord oozieActionRecord = new OozieActionRecord();

                    oozieActionRecord.setJobLauncherId(workflowJob.getId());
                    oozieActionRecord.setActionId(workflowAction.getId());
                    oozieActionRecord.setActionType(workflowAction.getType());
                    oozieActionRecord.setActionName(workflowAction.getName());
                    oozieActionRecord.setActionNumber(i + 1);
                    oozieActionRecord.setActionStatus(workflowAction.getStatus().toString());
                    oozieActionRecord.setActionChildId(workflowAction.getExternalChildIDs());
                    oozieActionRecord.setActionChildYarnApplicationId(Optional.ofNullable(workflowAction.getExternalChildIDs())
                            .map(s -> s.replace("job", "application"))
                            .orElse(null));

                    oozieActionRecord.setActionStartDate(TimeUtils.fromUtilDateToSqlDate(workflowAction.getStartTime()));
                    oozieActionRecord.setActionStartTime(TimeUtils.fromUtilDateToSqlTimestamp(workflowAction.getStartTime()));
                    oozieActionRecord.setActionEndDate(TimeUtils.fromUtilDateToSqlDate(workflowAction.getEndTime()));
                    oozieActionRecord.setActionEndTime(TimeUtils.fromUtilDateToSqlTimestamp(workflowAction.getEndTime()));
                    oozieActionRecord.setActionErrorCode(workflowAction.getErrorCode());
                    oozieActionRecord.setActionErrorMessage(workflowAction.getErrorMessage());
                    oozieActionRecord.setActionTrackingUrl(workflowAction.getTrackerUri());
                    oozieActionRecord.setRecordInsertTime(new Timestamp(System.currentTimeMillis()));
                    oozieActionRecord.setLastRecordUpdateTime(new Timestamp(System.currentTimeMillis()));

                    return oozieActionRecord;
                }).collect(Collectors.toList());
    }

    @Override
    public Object[] primaryKeyValues() {
        return new Object[]{jobLauncherId, actionId};
    }

    @Override
    public Object[] allValues() {

        return new Object[] {jobLauncherId, actionId, actionType, actionName, actionNumber, actionStatus, actionChildId,
                actionChildYarnApplicationId, actionStartDate, actionStartTime, actionEndDate, actionEndTime,
                actionErrorCode, actionErrorMessage, actionTrackingUrl, recordInsertTime, lastRecordUpdateTime
        };
    }
}
