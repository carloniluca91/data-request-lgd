package it.luca.lgd.jdbc.record;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
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
    private Integer actionNumber;
    private String actionFinishStatus;
    private List<String> actionChildId;
    private List<String> actionChildYarnApplicationId;
    private LocalDateTime actionStartTime;
    private LocalDate actionStartDate;
    private LocalDateTime actionEndTime;
    private LocalDate actionEndDate;
    private String actionErrorCode;
    private String actionErrorMessage;

    public static List<OozieActionRecord> batchFrom(WorkflowJob workflowJob) {

        Predicate<WorkflowAction> predicate = a -> Optional
                .ofNullable(a.getStartTime())
                .isPresent();

        // Order actions according to their startime (actions with null startTimes come last)
        List<WorkflowAction> workflowActions = workflowJob.getActions();
        List<WorkflowAction> actionsOrderedByStartTime = workflowActions.stream()
                .filter(predicate)
                .sorted(Comparator.comparing(WorkflowAction::getStartTime))
                .collect(Collectors.toList());

        actionsOrderedByStartTime.addAll(workflowActions.stream()
                .filter(predicate.negate())
                .collect(Collectors.toList()));

        return IntStream.range(0, actionsOrderedByStartTime.size())
                .mapToObj(i -> {

                    WorkflowAction workflowAction = actionsOrderedByStartTime.get(i);
                    OozieActionRecord oozieActionRecord = new OozieActionRecord();

                    oozieActionRecord.setJobLauncherId(workflowJob.getId());
                    oozieActionRecord.setActionId(workflowAction.getId());
                    oozieActionRecord.setActionType(workflowAction.getType());
                    oozieActionRecord.setActionName(workflowAction.getName());
                    oozieActionRecord.setActionNumber(i + 1);
                    oozieActionRecord.setActionFinishStatus(orNull(workflowAction.getStatus(), Enum::toString));
                    oozieActionRecord.setActionChildId(orNull(workflowAction.getExternalChildIDs(), s -> Arrays.asList(s.split(","))));
                    oozieActionRecord.setActionChildYarnApplicationId(orNull(workflowAction.getExternalChildIDs(),
                            s -> Arrays.asList(s.replace("job_", "application_")
                                    .split(","))));
                    oozieActionRecord.setActionStartTime(toLocalDateTime(workflowAction.getStartTime()));
                    oozieActionRecord.setActionStartDate(toLocalDate(workflowAction.getStartTime()));
                    oozieActionRecord.setActionEndTime(toLocalDateTime(workflowAction.getEndTime()));
                    oozieActionRecord.setActionEndDate(toLocalDate(workflowAction.getEndTime()));
                    oozieActionRecord.setActionErrorCode(workflowAction.getErrorCode());
                    oozieActionRecord.setActionErrorMessage(workflowAction.getErrorMessage());
                    oozieActionRecord.setTsInsert(LocalDateTime.now());
                    oozieActionRecord.setDtInsert(LocalDate.now());

                    return oozieActionRecord;
                }).collect(Collectors.toList());
    }
}
