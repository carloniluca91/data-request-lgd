package it.luca.lgd.jdbc.record;

import it.luca.lgd.model.parameters.JobParameters;
import it.luca.lgd.oozie.WorkflowJobId;
import it.luca.lgd.utils.JsonUtils;
import it.luca.lgd.utils.Tuple2;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import java.sql.Date;
import java.sql.Timestamp;

@Getter
@Setter
@NoArgsConstructor
public class RequestRecord extends DRLGDRecord {

    public static final String OK = "OK";
    public static final String KO = "KO";

    @Id
    @SequenceGenerator(name = "request_id", sequenceName = "oozie_request_id", allocationSize = 1)
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "request_id")
    private Integer requestId;

    private String requestUser;
    private WorkflowJobId workflowJobId;
    private Date requestDate;
    private Timestamp requestTime;
    private JobParameters requestParameters;
    private String jobLauncherId;
    private String jobSubmissionCode;
    private String jobSubmissionError;

    public static <T extends JobParameters> RequestRecord from(WorkflowJobId workflowJobId, T jobParameters, Tuple2<Boolean, String> tuple2) {

        RequestRecord requestRecord = new RequestRecord();
        requestRecord.setRequestUser("cloudera");
        requestRecord.setWorkflowJobId(workflowJobId);
        requestRecord.setRequestDate(new Date(System.currentTimeMillis()));
        requestRecord.setRequestTime(new Timestamp(System.currentTimeMillis()));
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

        requestRecord.setTsInsert(new Timestamp(System.currentTimeMillis()));
        requestRecord.setDtInsert(new Date(System.currentTimeMillis()));
        return requestRecord;
    }

    @Override
    public Object[] primaryKeyValues() {
        return new Object[]{requestId};
    }

    @Override
    public Object[] allValues() {
        return new Object[]{requestUser, workflowJobId.getId(), requestDate, requestTime,
                JsonUtils.objToString(requestParameters), jobLauncherId, jobSubmissionCode, jobSubmissionError};
    }
}
