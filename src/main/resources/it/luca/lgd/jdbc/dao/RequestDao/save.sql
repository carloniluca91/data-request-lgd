INSERT INTO request (
request_id,
request_user,
job_id,
request_time,
request_date,
request_parameters,
job_launcher_id,
job_submission_code,
job_submission_error,
ts_insert,
dt_insert
)
VALUES (
NEXTVAL('oozie_request_id'),
:request_user,
:job_id,
:request_time,
:request_date,
:request_parameters::jsonb,
:job_launcher_id,
:job_submission_code,
:job_submission_error,
:ts_insert,
:dt_insert)