package it.luca.lgd.model.jdbc;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.oozie.client.WorkflowAction;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@Entity
@Table(schema = "oozie", name = "oozie_action")
@NoArgsConstructor
public class OozieActionRecord {

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

    //TODO
    public OozieActionRecord fromWorkflowAction(WorkflowAction workflowAction) {

        return new OozieActionRecord();
    }
}
