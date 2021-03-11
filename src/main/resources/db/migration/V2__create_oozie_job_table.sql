SET SEARCH_PATH TO oozie;

-- oozie_job table (and comments)
CREATE TABLE IF NOT EXISTS oozie_job (

    job_launcher_id TEXT NOT NULL,
    job_name TEXT NOT NULL,
    job_app_path TEXT NOT NULL,
    job_total_actions INT NOT NULL,
    job_finish_status TEXT NOT NULL,
    job_start_time TIMESTAMP,
    job_start_date DATE,
    job_end_time TIMESTAMP,
    job_end_date DATE,
    job_tracking_url TEXT,
    ts_insert TIMESTAMP NOT NULL DEFAULT NOW(),
    dt_insert DATE NOT NULL DEFAULT NOW()::DATE,
    PRIMARY KEY (job_launcher_id)
);

COMMENT ON COLUMN oozie_job.job_app_path IS 'Oozie job .xml file path on HDFS';
COMMENT ON COLUMN oozie_job.job_launcher_id IS 'Oozie job Id (generated through Oozie Client API)';
COMMENT ON COLUMN oozie_job.job_tracking_url IS 'Oozie job tracking URL (for logging)';

ALTER TABLE request ADD CONSTRAINT request_job_launcher_id_fk
FOREIGN KEY (job_launcher_id) REFERENCES oozie_job (job_launcher_id)
ON UPDATE CASCADE
ON DELETE CASCADE;