package it.luca.lgd.jdbc.record;

import it.luca.lgd.utils.TimeUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import javax.persistence.Id;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class OozieActionRecord extends DRLGDRecord {

    @Id private String jobLauncherId;
    @Id private String actionId;
    private String actionType;
    private String actionName;
    private int actionNumber;
    private String actionFinishStatus;
    private String actionChildId;
    private String actionChildYarnApplicationId;
    private Date actionStartDate;
    private Timestamp actionStartTime;
    private Date actionEndDate;
    private Timestamp actionEndTime;
    private String actionErrorCode;
    private String actionErrorMessage;
    private String actionTrackingUrl;

    public static List<OozieActionRecord> batchFrom(WorkflowJob workflowJob) {

        List<WorkflowAction> orderedActions = workflowJob.getActions().stream()
                .sorted(Comparator.comparing(WorkflowAction::getStartTime))
                .collect(Collectors.toList());

        return IntStream.range(0, orderedActions.size())
                .mapToObj(i -> {

                    WorkflowAction workflowAction = orderedActions.get(i);
                    OozieActionRecord oozieActionRecord = new OozieActionRecord();

                    oozieActionRecord.setJobLauncherId(workflowJob.getId());
                    oozieActionRecord.setActionId(workflowAction.getId());
                    oozieActionRecord.setActionType(workflowAction.getType());
                    oozieActionRecord.setActionName(workflowAction.getName());
                    oozieActionRecord.setActionNumber(i + 1);
                    oozieActionRecord.setActionFinishStatus(workflowAction.getStatus().toString());
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
                    oozieActionRecord.setTsInsert(new Timestamp(System.currentTimeMillis()));
                    oozieActionRecord.setDtInsert(new Date(System.currentTimeMillis()));

                    return oozieActionRecord;
                }).collect(Collectors.toList());
    }

    @Override
    public Object[] allValues() {

        return new Object[] {jobLauncherId, actionId, actionType, actionName, actionNumber, actionFinishStatus, actionChildId,
                actionChildYarnApplicationId, actionStartDate, actionStartTime, actionEndDate, actionEndTime,
                actionErrorCode, actionErrorMessage, actionTrackingUrl, tsInsert, dtInsert
        };
    }
}
