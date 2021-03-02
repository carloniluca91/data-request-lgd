CREATE SCHEMA IF NOT EXISTS oozie AUTHORIZATION CURRENT_USER;

-- set newly created schema as default
SET SEARCH_PATH TO oozie;

-- oozie_job table (and comments)
CREATE TABLE IF NOT EXISTS oozie_job (

    job_launcher_id TEXT NOT NULL,
    job_type TEXT NOT NULL,
    job_name TEXT NOT NULL,
    job_user TEXT NOT NULL,
    job_status TEXT NOT NULL,
    job_start_date DATE,
    job_start_time TIMESTAMP,
    job_end_date DATE,
    job_end_time TIMESTAMP,
    job_total_actions INT NOT NULL,
    job_completed_actions INT NOT NULL,
    job_tracking_url TEXT,
    record_insert_time TIMESTAMP NOT NULL DEFAULT NOW(),
    last_record_update_time TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_launcher_id)
);

COMMENT ON COLUMN oozie_job.job_launcher_id IS 'Oozie job Id (generated through Oozie Client API)';
COMMENT ON COLUMN oozie_job.job_type IS 'Oozie job type (WORKFLOW|COORDINATOR|BUNDLE)';
COMMENT ON COLUMN oozie_job.job_total_actions IS 'Number of actions defined by the Oozie job';
COMMENT ON COLUMN oozie_job.job_completed_actions IS 'Number of completed actions of the Oozie job';
COMMENT ON COLUMN oozie_job.job_tracking_url IS 'Oozie job tracking URL (for logging)';
COMMENT ON COLUMN oozie_job.record_insert_time IS 'Record insert time';
COMMENT ON COLUMN oozie_job.last_record_update_time IS 'Last update time of this records';

-- oozie_action table (and comments)
CREATE TABLE IF NOT EXISTS oozie_action (

    job_launcher_id TEXT NOT NULL,
    action_id TEXT NOT NULL,
    action_type TEXT NOT NULL,
    action_name TEXT NOT NULL,
    action_number INT NOT NULL,
    action_status TEXT NOT NULL,
    action_child_id TEXT,
    action_child_yarn_application_id TEXT,
    action_start_date DATE,
    action_start_time TIMESTAMP,
    action_end_date DATE,
    action_end_time TIMESTAMP,
    action_error_code TEXT,
    action_error_message TEXT,
    action_tracking_url TEXT,
    record_insert_time TIMESTAMP NOT NULL DEFAULT NOW(),
    last_record_update_time TIMESTAMP NOT NULL DEFAULT NOW(),
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
COMMENT ON COLUMN oozie_action.record_insert_time IS 'Record insert time';
COMMENT ON COLUMN oozie_action.last_record_update_time IS 'Last update time of this records';