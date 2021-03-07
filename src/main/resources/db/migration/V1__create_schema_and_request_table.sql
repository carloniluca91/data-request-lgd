CREATE SCHEMA IF NOT EXISTS oozie AUTHORIZATION CURRENT_USER;

-- set newly created schema as default
SET SEARCH_PATH TO oozie;

-- create sequence for request table
CREATE SEQUENCE IF NOT EXISTS oozie_request_id;

-- request table
CREATE TABLE IF NOT EXISTS request (

    request_id INT NOT NULL DEFAULT NEXTVAL('oozie_request_id'),
    request_user TEXT NOT NULL,
    request_job_id TEXT NOT NULL,
    request_date DATE NOT NULL,
    request_time TIMESTAMP NOT NULL,
    request_parameters TEXT,
    job_launcher_id TEXT,
    job_submission_code TEXT,
    job_submission_error TEXT,
    ts_insert TIMESTAMP NOT NULL DEFAULT NOW(),
    dt_insert DATE NOT NULL DEFAULT NOW()::DATE
    PRIMARY KEY (request_id),
);

COMMENT ON COLUMN request.request_job_id IS 'Oozie job name';
COMMENT ON COLUMN request.request_parameters IS 'Oozie job provided parameters';
COMMENT ON COLUMN request.job_launcher_id IS 'Oozie job Id (generated through Oozie Client API)';
COMMENT ON COLUMN request.job_submission_code IS 'Oozie job submission code (OK|KO)';
COMMENT ON COLUMN request.job_submission_error IS 'Oozie job submission error';