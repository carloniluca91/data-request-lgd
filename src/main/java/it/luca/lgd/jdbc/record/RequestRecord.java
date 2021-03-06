package it.luca.lgd.jdbc.record;

import it.luca.lgd.model.JobParameters;
import it.luca.lgd.oozie.WorkflowJobLabel;
import it.luca.lgd.utils.Tuple2;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
public class RequestRecord extends BaseRecord {

    public static final String OK = "OK";
    public static final String KO = "KO";

    private Integer requestId;
    private String requestUser;
    private WorkflowJobLabel workflowJobLabel;
    private LocalDate requestDate;
    private LocalDateTime requestTime;
    private JobParameters requestParameters;
    private String jobLauncherId;
    private String jobSubmissionCode;
    private String jobSubmissionError;

    public static <T extends JobParameters> RequestRecord from(WorkflowJobLabel workflowJobLabel, T jobParameters, Tuple2<Boolean, String> tuple2) {

        RequestRecord requestRecord = new RequestRecord();
        requestRecord.setRequestUser("cloudera");
        requestRecord.setWorkflowJobLabel(workflowJobLabel);
        requestRecord.setRequestDate(LocalDate.now());
        requestRecord.setRequestTime(LocalDateTime.now());
        requestRecord.setRequestParameters(jobParameters);
        if (tuple2.getT1()) {

            requestRecord.setJobLauncherId(tuple2.getT2());
            requestRecord.setJobSubmissionCode(OK);
            requestRecord.setJobSubmissionError(null);
        } else {

            requestRecord.setJobLauncherId(null);
            requestRecord.setJobSubmissionCode(KO);
            requestRecord.setJobSubmissionError(tuple2.getT2());
        }

        requestRecord.setTsInsert(LocalDateTime.now());
        requestRecord.setDtInsert(LocalDate.now());
        return requestRecord;
    }
}
