package it.luca.lgd.jdbc.mapper;

import it.luca.lgd.jdbc.model.OozieJobRecord;
import it.luca.lgd.jdbc.table.OozieJobTableDefinition;

import java.sql.ResultSet;
import java.sql.SQLException;

public class OozieJobRecordExtractor extends DRLGDResultSetExtractor<OozieJobTableDefinition, OozieJobRecord> {

    public OozieJobRecordExtractor() {
        super(new OozieJobTableDefinition());
    }

    @Override
    protected OozieJobRecord fromResultSet(ResultSet resultSet) throws SQLException {

        OozieJobRecord oozieJobRecord = new OozieJobRecord();
        oozieJobRecord.setJobLauncherId(resultSet.getString(tableDefinition.JOB_LAUNCHER_ID));
        oozieJobRecord.setJobType(resultSet.getString(tableDefinition.JOB_TYPE));
        oozieJobRecord.setJobName(resultSet.getString(tableDefinition.JOB_NAME));
        oozieJobRecord.setJobUser(resultSet.getString(tableDefinition.JOB_USER));
        oozieJobRecord.setJobStatus(resultSet.getString(tableDefinition.JOB_STATUS));
        oozieJobRecord.setJobStartDate(resultSet.getDate(tableDefinition.JOB_START_DATE).toLocalDate());
        oozieJobRecord.setJobStartTime(resultSet.getTimestamp(tableDefinition.JOB_START_TIME).toLocalDateTime());
        oozieJobRecord.setJobEndDate(resultSet.getDate(tableDefinition.JOB_END_DATE).toLocalDate());
        oozieJobRecord.setJobEndTime(resultSet.getTimestamp(tableDefinition.JOB_END_TIME).toLocalDateTime());
        oozieJobRecord.setJobTotalActions(resultSet.getInt(tableDefinition.JOB_TOTAL_ACTIONS));
        oozieJobRecord.setJobCompletedActions(resultSet.getInt(tableDefinition.JOB_COMPLETED_ACTIONS));
        oozieJobRecord.setJobTrackingUrl(resultSet.getString(tableDefinition.JOB_TRACKING_URL));
        oozieJobRecord.setRecordInsertTime(resultSet.getTimestamp(tableDefinition.RECORD_INSERT_TIME).toLocalDateTime());
        oozieJobRecord.setLastRecordUpdateTime(resultSet.getTimestamp(tableDefinition.LAST_RECORD_UPDATE_TIME).toLocalDateTime());

        return oozieJobRecord;
    }
}
