package it.luca.lgd.jdbc.table;

import it.luca.lgd.jdbc.record.RequestRecord;
import it.luca.lgd.oozie.WorkflowJobId;
import it.luca.lgd.utils.JsonUtils;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class RequestTable extends DRLGDTable<RequestRecord> {

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
                JOB_LAUNCHER_ID, JOB_SUBMISSION_CODE, JOB_SUBMISSION_ERROR);
    }

    @Override
    public MapSqlParameterSource fromRecordToMapSqlParameterSource(RequestRecord record) {

        return new MapSqlParameterSource()
                .addValue(REQUEST_USER, record.getRequestUser())
                .addValue(JOB_ID, record.getWorkflowJobId().getId())
                .addValue(REQUEST_DATE, record.getRequestDate())
                .addValue(REQUEST_TIME, record.getRequestTime())
                .addValue(REQUEST_PARAMETERS, JsonUtils.objToString(record.getRequestParameters()))
                .addValue(JOB_LAUNCHER_ID, record.getJobLauncherId())
                .addValue(JOB_SUBMISSION_CODE, record.getJobSubmissionCode())
                .addValue(JOB_SUBMISSION_ERROR, record.getJobSubmissionError());
    }

    @Override
    protected RequestRecord fromResultSetToTableRecord(ResultSet rs) throws SQLException {

        RequestRecord requestRecord = new RequestRecord();

        requestRecord.setRequestId(rs.getInt(REQUEST_ID));
        requestRecord.setRequestUser(rs.getString(REQUEST_USER));
        WorkflowJobId workflowJobId = WorkflowJobId.withId(rs.getString(JOB_ID));
        requestRecord.setWorkflowJobId(workflowJobId);
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
