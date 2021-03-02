package it.luca.lgd.jdbc.table;

import it.luca.lgd.jdbc.record.OozieJobRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class OozieJobTable extends DRLGDTable<OozieJobRecord> {

    public final String JOB_LAUNCHER_ID = "job_launcher_id";
    public final String JOB_TYPE = "job_type";
    public final String JOB_NAME = "job_name";
    public final String JOB_USER = "job_user";
    public final String JOB_STATUS = "job_status";
    public final String JOB_START_DATE = "job_start_date";
    public final String JOB_START_TIME = "job_start_time";
    public final String JOB_END_DATE = "job_end_date";
    public final String JOB_END_TIME = "job_end_time";
    public final String JOB_TOTAL_ACTIONS = "job_total_actions";
    public final String JOB_COMPLETED_ACTIONS = "job_completed_actions";
    public final String JOB_TRACKING_URL = "job_tracking_url";
    public final String RECORD_INSERT_TIME = "record_insert_time";
    public final String LAST_RECORD_UPDATE_TIME = "last_record_update_time";

    public OozieJobTable() {
        super(OozieJobRecord.class);
    }

    @Override
    public List<String> allColumns() {

        return Arrays.asList(JOB_LAUNCHER_ID, JOB_TYPE, JOB_NAME, JOB_USER, JOB_STATUS, JOB_START_DATE,
                JOB_START_TIME, JOB_END_DATE, JOB_END_TIME, JOB_TOTAL_ACTIONS, JOB_COMPLETED_ACTIONS,
                JOB_TRACKING_URL, RECORD_INSERT_TIME, LAST_RECORD_UPDATE_TIME);
    }

    @Override
    public List<String> primaryKeyColumns() {
        return Collections.singletonList(JOB_LAUNCHER_ID);
    }

    @Override
    protected OozieJobRecord fromResultSetToTableRecord(ResultSet rs) throws SQLException {

        OozieJobRecord oozieJobRecord = new OozieJobRecord();

        oozieJobRecord.setJobLauncherId(rs.getString(JOB_LAUNCHER_ID));
        oozieJobRecord.setJobType(rs.getString(JOB_TYPE));
        oozieJobRecord.setJobName(rs.getString(JOB_NAME));
        oozieJobRecord.setJobUser(rs.getString(JOB_USER));
        oozieJobRecord.setJobStatus(rs.getString(JOB_STATUS));
        oozieJobRecord.setJobStartDate(rs.getDate(JOB_START_DATE));
        oozieJobRecord.setJobStartTime(rs.getTimestamp(JOB_START_TIME));
        oozieJobRecord.setJobEndDate(rs.getDate(JOB_END_DATE));
        oozieJobRecord.setJobEndTime(rs.getTimestamp(JOB_END_TIME));
        oozieJobRecord.setJobTotalActions(rs.getInt(JOB_TOTAL_ACTIONS));
        oozieJobRecord.setJobCompletedActions(rs.getInt(JOB_COMPLETED_ACTIONS));
        oozieJobRecord.setJobTrackingUrl(rs.getString(JOB_TRACKING_URL));
        oozieJobRecord.setRecordInsertTime(rs.getTimestamp(RECORD_INSERT_TIME));
        oozieJobRecord.setLastRecordUpdateTime(rs.getTimestamp(LAST_RECORD_UPDATE_TIME));

        return oozieJobRecord;
    }
}
