INSERT INTO oozie_job (
job_launcher_id,
job_name,
job_app_path,
job_total_actions,
job_finish_status,
job_start_time,
job_start_date,
job_end_time,
job_end_date,
job_tracking_url
) VALUES (
:jobLauncherId,
:jobName,
:jobAppPath,
:jobTotalActions,
:jobFinishStatus,
:jobStartTime,
:jobStartDate,
:jobEndTime,
:jobEndDate,
:jobTrackingUrl
)