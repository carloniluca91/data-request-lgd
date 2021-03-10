package it.luca.lgd.jdbc.record;

import it.luca.lgd.model.parameters.JobParameters;
import it.luca.lgd.oozie.WorkflowJobId;
import it.luca.lgd.utils.JsonUtils;
import it.luca.lgd.utils.Tuple2;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

@Getter
@Setter
@NoArgsConstructor
public class RequestRecord extends DRLGDRecord {

    public static final String OK = "OK";
    public static final String KO = "KO";

    @Id
    @GeneratedValue
    private Integer requestId;
    private String requestUser;
    private WorkflowJobId jobId;
    private Date requestDate;
    private Timestamp requestTime;
    private JobParameters requestParameters;
    private String jobLauncherId;
    private String jobSubmissionCode;
    private String jobSubmissionError;

    @Override
    public PreparedStatement getPreparedStatement(PreparedStatement ps) throws SQLException {

        ps.setString(1, requestUser);
        ps.setString(2, jobId.getId());
        ps.setDate(3, requestDate);
        ps.setTimestamp(4, requestTime);
        ps.setString(5, JsonUtils.objToString(requestParameters));
        ps.setString(6, jobLauncherId);
        ps.setString(7, jobSubmissionCode);
        ps.setString(8, jobSubmissionError);
        ps.setTimestamp(9, tsInsert);
        ps.setDate(10, dtInsert);
        return ps;
    }

    public static <T extends JobParameters> RequestRecord from(WorkflowJobId workflowJobId, T jobParameters, Tuple2<Boolean, String> tuple2) {

        RequestRecord requestRecord = new RequestRecord();
        requestRecord.setRequestUser("cloudera");
        requestRecord.setJobId(workflowJobId);
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
    public Object[] allValues() {
        return new Object[]{requestUser, jobId.getId(), requestDate, requestTime,
                JsonUtils.objToString(requestParameters), jobLauncherId, jobSubmissionCode, jobSubmissionError,
                tsInsert, dtInsert};
    }
}
