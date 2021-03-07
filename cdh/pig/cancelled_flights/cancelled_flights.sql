CREATE TABLE IF NOT EXISTS ${db}.${output_table} (

    origin_iata STRING COMMENT "Origin airport IATA code",
    origin_aiport STRING,
    origin_city STRING,
    destination_iata STRING COMMENT "Destination airport IATA code",
    destination_airport STRING,
    destination_city STRING,
    flight_date STRING,
    cancellation_code VARCHAR(1),
    cancellation_rationale STRING,
    ts_insert TIMESTAMP COMMENT "Data insert timestamp",
    dt_insert STRING COMMENT "Data insert date",
    requesting_user STRING COMMENT "User that issued the data request",
    workflow_job_id STRING COMMENT "Oozie workflow job id",
    job_type STRING COMMENT "Job type (PIG|HIVE|SPARK)"
)
STORED AS PARQUET;

TRUNCATE TABLE ${db}.${output_table};