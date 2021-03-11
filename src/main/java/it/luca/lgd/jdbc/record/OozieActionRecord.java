package it.luca.lgd.jdbc.record;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static it.luca.lgd.utils.Java8Utils.orNull;
import static it.luca.lgd.utils.TimeUtils.toLocalDate;
import static it.luca.lgd.utils.TimeUtils.toLocalDateTime;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class OozieActionRecord extends DRLGDRecord {

    private String actionId;
    private String jobLauncherId;
    private String actionType;
    private String actionName;
    private int actionNumber;
    private String actionFinishStatus;
    private String actionChildId;
    private String actionChildYarnApplicationId;
    private LocalDateTime actionStartTime;
    private LocalDate actionStartDate;
    private LocalDateTime actionEndTime;
    private LocalDate actionEndDate;
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
                    oozieActionRecord.setActionChildYarnApplicationId(orNull(workflowAction.getExternalChildIDs(),
                            s -> s.replace("job", "application")));
                    oozieActionRecord.setActionStartTime(toLocalDateTime(workflowAction.getStartTime()));
                    oozieActionRecord.setActionStartDate(toLocalDate(workflowAction.getStartTime()));
                    oozieActionRecord.setActionEndTime(toLocalDateTime(workflowAction.getEndTime()));
                    oozieActionRecord.setActionEndDate(toLocalDate(workflowAction.getEndTime()));
                    oozieActionRecord.setActionErrorCode(workflowAction.getErrorCode());
                    oozieActionRecord.setActionErrorMessage(workflowAction.getErrorMessage());
                    oozieActionRecord.setActionTrackingUrl(workflowAction.getTrackerUri());
                    oozieActionRecord.setTsInsert(LocalDateTime.now());
                    oozieActionRecord.setDtInsert(LocalDate.now());

                    return oozieActionRecord;
                }).collect(Collectors.toList());
    }
}
