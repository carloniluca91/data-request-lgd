package it.luca.lgd.jdbc.record;

import it.luca.lgd.model.parameters.JobParameters;
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
@Entity
@Table(schema = "oozie", name = "request")
@NoArgsConstructor
public class RequestRecord extends DRLGDRecord {

    public static final String OK = "OK";
    public static final String KO = "KO";

    @Id
    @SequenceGenerator(name = "request_id", sequenceName = "oozie_request_id")
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "request_id")
    private Integer requestId;

    private String requestUser;
    private Date requestDate;
    private Timestamp requestTime;
    private String requestParameters;
    private String jobLauncherId;
    private String jobSubmissionCode;
    private String jobSubmissionError;

    public static <T extends JobParameters> RequestRecord from(T requestParameters, Tuple2<Boolean, String> tuple2) {

        RequestRecord RequestRecord = new RequestRecord();
        RequestRecord.setRequestUser("cloudera");
        RequestRecord.setRequestDate(new Date(System.currentTimeMillis()));
        RequestRecord.setRequestTime(new Timestamp(System.currentTimeMillis()));
        RequestRecord.setRequestParameters(JsonUtils.objToString(requestParameters));
        if (tuple2.getT1()) {

            RequestRecord.setJobLauncherId(tuple2.getT2());
            RequestRecord.setJobSubmissionCode(OK);
            RequestRecord.setJobSubmissionError(null);
        } else {

            RequestRecord.setJobLauncherId(null);
            RequestRecord.setJobSubmissionCode(KO);
            RequestRecord.setJobSubmissionError(tuple2.getT2());
        }

        return RequestRecord;
    }

    @Override
    public Object[] primaryKeyValues() {
        return new Object[]{requestId};
    }

    @Override
    public Object[] allValues() {
        return new Object[]{requestUser, requestDate, requestTime,
                JsonUtils.objToString(requestParameters), jobLauncherId,
                jobSubmissionCode, jobSubmissionError};
    }
}
