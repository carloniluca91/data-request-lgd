package it.luca.lgd.jdbc.mapper;

import it.luca.lgd.jdbc.record.RequestRecord;
import it.luca.lgd.jdbc.table.RequestTable;
import it.luca.lgd.model.JobParameters;
import it.luca.lgd.oozie.WorkflowJobLabel;
import lombok.NoArgsConstructor;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

import static it.luca.lgd.utils.JsonUtils.stringToObject;
import static it.luca.lgd.utils.TimeUtils.toLocalDate;
import static it.luca.lgd.utils.TimeUtils.toLocalDateTime;

@NoArgsConstructor
public class RequestMapper implements RowMapper<RequestRecord> {

    @Override
    public RequestRecord map(ResultSet rs, StatementContext ctx) throws SQLException {

        RequestRecord requestRecord = new RequestRecord();
        requestRecord.setRequestId(rs.getInt(RequestTable.REQUEST_ID));
        requestRecord.setRequestUser(rs.getString(RequestTable.REQUEST_USER));

        WorkflowJobLabel workflowJobLabel = WorkflowJobLabel.withId(rs.getString(RequestTable.JOB_ID));
        Class<? extends JobParameters> clazz = workflowJobLabel.getParameterClass();
        requestRecord.setWorkflowJobLabel(workflowJobLabel);
        requestRecord.setRequestTime(toLocalDateTime(rs.getTimestamp(RequestTable.REQUEST_TIME)));
        requestRecord.setRequestDate(toLocalDate(rs.getDate(RequestTable.REQUEST_DATE)));
        requestRecord.setRequestParameters(stringToObject(rs.getString(RequestTable.REQUEST_PARAMETERS), clazz));
        requestRecord.setJobLauncherId(rs.getString(RequestTable.JOB_LAUNCHER_ID));
        requestRecord.setJobSubmissionCode(rs.getString(RequestTable.JOB_SUBMISSION_CODE));
        requestRecord.setJobSubmissionError(rs.getString(RequestTable.JOB_SUBMISSION_ERROR));
        requestRecord.setTsInsert(toLocalDateTime(rs.getTimestamp(RequestTable.TS_INSERT)));
        requestRecord.setDtInsert(toLocalDate(rs.getDate(RequestTable.DT_INSERT)));

        return requestRecord;
    }
}
