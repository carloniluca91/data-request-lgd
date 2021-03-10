package it.luca.lgd.jdbc.table;

import it.luca.lgd.jdbc.record.RequestRecord;
import it.luca.lgd.oozie.WorkflowJobId;
import it.luca.lgd.utils.JsonUtils;

import javax.persistence.Id;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class RequestTable extends DRLGDTable<RequestRecord> {

    @Id
    public final String REQUEST_ID = "request_id";
    public final String REQUEST_USER = "request_user";
    public final String JOB_ID = "job_id";
    public final String REQUEST_DATE = "request_date";
    public final String REQUEST_TIME = "request_time";
    public final String REQUEST_PARAMETERS = "request_parameters";
    public final String JOB_LAUNCHER_ID = "job_launcher_id";
    public final String JOB_SUBMISSION_CODE = "job_submission_code";
    public final String JOB_SUBMISSION_ERROR = "job_submission_error";

    public RequestTable() {
        super(RequestRecord.class);
    }

    @Override
    public List<String> allColumns() {
        return Arrays.asList(REQUEST_USER, JOB_ID, REQUEST_DATE, REQUEST_TIME, REQUEST_PARAMETERS,
                JOB_LAUNCHER_ID, JOB_SUBMISSION_CODE, JOB_SUBMISSION_ERROR, TS_INSERT, DT_INSERT);
    }

    @Override
    public Map<String, Object> fromRecordToMapSqlParameterSource(RequestRecord record) {

        return new LinkedHashMap<String, Object>(){{

            put(REQUEST_USER, record.getRequestUser());
            put(JOB_ID, record.getJobId().getId());
            put(REQUEST_DATE, record.getRequestDate());
            put(REQUEST_TIME, record.getRequestTime());
            put(REQUEST_PARAMETERS, JsonUtils.objToString(record.getRequestParameters()));
            put(JOB_LAUNCHER_ID, record.getJobLauncherId());
            put(JOB_SUBMISSION_CODE, record.getJobSubmissionCode());
            put(JOB_SUBMISSION_ERROR, record.getJobSubmissionError());
            put(TS_INSERT, record.getTsInsert());
            put(DT_INSERT, record.getDtInsert());
        }};
    }

    @Override
    protected RequestRecord fromResultSetToTableRecord(ResultSet rs) throws SQLException {

        RequestRecord requestRecord = new RequestRecord();

        requestRecord.setRequestId(rs.getInt(REQUEST_ID));
        requestRecord.setRequestUser(rs.getString(REQUEST_USER));
        WorkflowJobId workflowJobId = WorkflowJobId.withId(rs.getString(JOB_ID));
        requestRecord.setJobId(workflowJobId);
        requestRecord.setRequestDate(rs.getDate(REQUEST_DATE));
        requestRecord.setRequestTime(rs.getTimestamp(REQUEST_TIME));
        requestRecord.setRequestParameters(Optional.ofNullable(rs.getString(REQUEST_PARAMETERS))
                .map(s -> JsonUtils.stringToObj(s, workflowJobId.getParameterClass()))
                .orElse(null));
        requestRecord.setJobLauncherId(rs.getString(JOB_LAUNCHER_ID));
        requestRecord.setJobSubmissionCode(rs.getString(JOB_SUBMISSION_CODE));
        requestRecord.setJobSubmissionError(rs.getString(JOB_SUBMISSION_ERROR));
        requestRecord.setTsInsert(rs.getTimestamp(this.TS_INSERT));
        requestRecord.setDtInsert(rs.getDate(this.DT_INSERT));

        return requestRecord;
    }

    @Override
    public List<String> primaryKeyColumns() {
        return Collections.singletonList(REQUEST_ID);
    }

    @Override
    public String tableName() { return "request"; }
}
