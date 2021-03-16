CREATE TABLE IF NOT EXISTS ${db}.${table} (

    origin_iata STRING COMMENT "Origin airport IATA code",
    origin_aiport STRING,
    origin_city STRING,
    airline_iata STRING COMMENT "Airline IATA code",
    airline_name STRING,
    destination_iata STRING COMMENT "Destination airport IATA code",
    destination_airport STRING,
    destination_city STRING,
    flight_date STRING,
    cancellation_code VARCHAR(1),
    cancellation_rationale STRING,
    requesting_user STRING COMMENT "User that issued the data request",
    workflow_job_id STRING COMMENT "Oozie workflow job id",
    job_type STRING COMMENT "Job type (PIG|HIVE|SPARK)",
    ts_insert TIMESTAMP COMMENT "Data insert timestamp",
    dt_insert STRING COMMENT "Data insert date"
)
STORED AS PARQUET;

TRUNCATE TABLE ${db}.${table};