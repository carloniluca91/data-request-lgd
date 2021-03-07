SET SEARCH_PATH TO oozie;

-- oozie_action table (and comments)
CREATE TABLE IF NOT EXISTS oozie_action (

    job_launcher_id TEXT NOT NULL,
    action_id TEXT NOT NULL,
    action_type TEXT NOT NULL,
    action_name TEXT NOT NULL,
    action_number INT NOT NULL,
    action_finish_status TEXT NOT NULL,
    action_child_id TEXT,
    action_child_yarn_application_id TEXT,
    action_start_date DATE,
    action_start_time TIMESTAMP,
    action_end_date DATE,
    action_end_time TIMESTAMP,
    action_error_code TEXT,
    action_error_message TEXT,
    action_tracking_url TEXT,
    ts_insert TIMESTAMP NOT NULL DEFAULT NOW(),
    dt_insert DATE NOT NULL DEFAULT NOW()::DATE
    PRIMARY KEY (job_launcher_id, action_id),
    CONSTRAINT oozie_action_job_launcher_id_fk
    FOREIGN KEY (job_launcher_id)
        REFERENCES oozie_job (job_launcher_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

COMMENT ON COLUMN oozie_action.job_launcher_id IS 'Oozie job Id of parent Oozie job';
COMMENT ON COLUMN oozie_action.action_number IS 'Action sequence number within parent Oozie job';
COMMENT ON COLUMN oozie_action.action_child_id IS 'Job Id of action child process';
COMMENT ON COLUMN oozie_action.action_child_yarn_application_id IS 'Yarn application Id of action child process';
COMMENT ON COLUMN oozie_action.action_tracking_url IS 'Action tracking URL (for logging)';