package it.luca.lgd.jdbc.table;

import it.luca.lgd.jdbc.model.OozieJobRecord;

import java.util.Arrays;
import java.util.List;

public class OozieJobTableDefinition extends TableDefinition<OozieJobRecord> {

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

    public OozieJobTableDefinition() {
        super(OozieJobRecord.class);
    }

    @Override
    public List<String> allColumns() {

        return Arrays.asList(JOB_LAUNCHER_ID, JOB_TYPE, JOB_NAME, JOB_USER, JOB_STATUS, JOB_START_DATE,
                JOB_START_TIME, JOB_END_DATE, JOB_END_TIME, JOB_TOTAL_ACTIONS, JOB_COMPLETED_ACTIONS,
                JOB_TRACKING_URL, RECORD_INSERT_TIME, LAST_RECORD_UPDATE_TIME);
    }
}
