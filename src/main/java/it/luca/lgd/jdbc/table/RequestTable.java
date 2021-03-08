package it.luca.lgd.jdbc.table;

import it.luca.lgd.jdbc.record.RequestRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RequestTable extends DRLGDTable<RequestRecord> {

    public final String REQUEST_ID = "request_id";
    public final String REQUEST_USER = "request_user";
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
        return Arrays.asList(REQUEST_DATE, REQUEST_TIME, REQUEST_PARAMETERS, JOB_LAUNCHER_ID,
                JOB_SUBMISSION_CODE, JOB_SUBMISSION_ERROR);
    }

    @Override
    protected RequestRecord fromResultSetToTableRecord(ResultSet rs) throws SQLException {

        RequestRecord requestRecord = new RequestRecord();

        requestRecord.setRequestId(rs.getInt(REQUEST_ID));
        requestRecord.setRequestUser(rs.getString(REQUEST_USER));
        requestRecord.setRequestDate(rs.getDate(REQUEST_DATE));
        requestRecord.setRequestTime(rs.getTimestamp(REQUEST_TIME));
        requestRecord.setRequestParameters(rs.getString(REQUEST_PARAMETERS));
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
}
