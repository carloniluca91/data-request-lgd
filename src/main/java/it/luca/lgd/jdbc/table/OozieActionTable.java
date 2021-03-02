package it.luca.lgd.jdbc.table;

import it.luca.lgd.jdbc.record.OozieActionRecord;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class OozieActionTable extends DRLGDTable<OozieActionRecord> {

    public final String JOB_LAUNCHER_ID = "job_launcher_id";
    public final String ACTION_ID = "action_id";
    public final String ACTION_TYPE = "action_type";
    public final String ACTION_NAME = "action_name";
    public final String ACTION_NUMBER = "action_number";
    public final String ACTION_STATUS = "action_status";
    public final String ACTION_CHILD_ID = "action_child_id";
    public final String ACTION_CHILD_YARN_APPLICATION_ID = "action_child_yarn_application_id";
    public final String ACTION_START_DATE = "action_start_date";
    public final String ACTION_START_TIME = "action_start_time";
    public final String ACTION_END_DATE = "action_end_date";
    public final String ACTION_END_TIME = "action_end_time";
    public final String ACTION_ERROR_CODE = "action_error_code";
    public final String ACTION_ERROR_MESSAGE = "action_error_message";
    public final String ACTION_TRACKING_URL = "action_tracking_url";
    public final String RECORD_INSERT_TIME = "record_insert_time";
    public final String LAST_RECORD_UPDATE_TIME = "last_record_update_time";

    public OozieActionTable() {
        super(OozieActionRecord.class);
    }

    @Override
    public List<String> allColumns() {

        return Arrays.asList(JOB_LAUNCHER_ID, ACTION_ID, ACTION_TYPE, ACTION_NAME, ACTION_NUMBER, ACTION_STATUS,
                ACTION_CHILD_ID, ACTION_CHILD_YARN_APPLICATION_ID, ACTION_START_DATE, ACTION_START_TIME,
                ACTION_END_DATE, ACTION_END_TIME, ACTION_ERROR_CODE, ACTION_ERROR_MESSAGE, ACTION_TRACKING_URL,
                RECORD_INSERT_TIME, LAST_RECORD_UPDATE_TIME);
    }

    @Override
    public List<String> primaryKeyColumns() {

        return Arrays.asList(JOB_LAUNCHER_ID, ACTION_ID);
    }

    @Override
    protected OozieActionRecord fromResultSetToTableRecord(ResultSet rs) throws SQLException {

        OozieActionRecord oozieActionRecord = new OozieActionRecord();

        oozieActionRecord.setJobLauncherId(rs.getString(JOB_LAUNCHER_ID));
        oozieActionRecord.setActionId(rs.getString(ACTION_ID));
        oozieActionRecord.setActionType(rs.getString(ACTION_TYPE));
        oozieActionRecord.setActionName(ACTION_NAME);
        oozieActionRecord.setActionNumber(rs.getInt(ACTION_NUMBER));
        oozieActionRecord.setActionStatus(rs.getString(ACTION_STATUS));
        oozieActionRecord.setActionChildId(rs.getString(ACTION_CHILD_ID));
        oozieActionRecord.setActionChildYarnApplicationId(rs.getString(ACTION_CHILD_YARN_APPLICATION_ID));
        oozieActionRecord.setActionStartDate(rs.getDate(ACTION_START_DATE));
        oozieActionRecord.setActionStartTime(rs.getTimestamp(ACTION_START_TIME));
        oozieActionRecord.setActionEndDate(rs.getDate(ACTION_END_DATE));
        oozieActionRecord.setActionEndTime(rs.getTimestamp(ACTION_END_TIME));
        oozieActionRecord.setActionErrorCode(rs.getString(ACTION_ERROR_CODE));
        oozieActionRecord.setActionErrorMessage(rs.getString(ACTION_ERROR_MESSAGE));
        oozieActionRecord.setActionTrackingUrl(rs.getString(ACTION_TRACKING_URL));
        oozieActionRecord.setRecordInsertTime(rs.getTimestamp(RECORD_INSERT_TIME));
        oozieActionRecord.setLastRecordUpdateTime(rs.getTimestamp(LAST_RECORD_UPDATE_TIME));
        return oozieActionRecord;
    }
}
