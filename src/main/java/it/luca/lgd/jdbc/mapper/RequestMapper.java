package it.luca.lgd.jdbc.mapper;

import it.luca.lgd.jdbc.record.RequestRecord;
import it.luca.lgd.jdbc.table.RequestTable1;
import it.luca.lgd.model.parameters.JobParameters;
import it.luca.lgd.oozie.WorkflowJobId;
import it.luca.lgd.utils.JsonUtils;
import lombok.NoArgsConstructor;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

import static it.luca.lgd.utils.TimeUtils.toLocalDate;
import static it.luca.lgd.utils.TimeUtils.toLocalDateTime;

@NoArgsConstructor
public class RequestMapper implements RowMapper<RequestRecord> {

    @Override
    public RequestRecord map(ResultSet rs, StatementContext ctx) throws SQLException {

        RequestRecord requestRecord = new RequestRecord();
        requestRecord.setRequestId(rs.getInt(RequestTable1.REQUEST_ID));
        requestRecord.setRequestUser(rs.getString(RequestTable1.REQUEST_USER));

        WorkflowJobId workflowJobId = WorkflowJobId.withId(rs.getString(RequestTable1.JOB_ID));
        Class<? extends JobParameters> clazz = workflowJobId.getParameterClass();
        requestRecord.setWorkflowJobId(workflowJobId);
        requestRecord.setRequestTime(toLocalDateTime(rs.getTimestamp(RequestTable1.REQUEST_TIME)));
        requestRecord.setRequestDate(toLocalDate(rs.getDate(RequestTable1.REQUEST_DATE)));
        requestRecord.setRequestParameters(JsonUtils.stringToObj(rs.getString(RequestTable1.REQUEST_PARAMETERS), clazz));
        requestRecord.setJobLauncherId(rs.getString(RequestTable1.JOB_LAUNCHER_ID));
        requestRecord.setJobSubmissionCode(rs.getString(RequestTable1.JOB_SUBMISSION_CODE));
        requestRecord.setJobSubmissionError(rs.getString(RequestTable1.JOB_SUBMISSION_ERROR));
        requestRecord.setTsInsert(toLocalDateTime(rs.getTimestamp(RequestTable1.TS_INSERT)));
        requestRecord.setDtInsert(toLocalDate(rs.getDate(RequestTable1.DT_INSERT)));

        return requestRecord;
    }
}
