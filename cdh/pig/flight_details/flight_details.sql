CREATE TABLE IF NOT EXISTS ${db}.${table} (

    flight_number INT,
    airline_iata STRING COMMENT "Airline IATA code",
    airline_name STRING,
    scheduled_departure TIMESTAMP,
    origin_airport STRING COMMENT "Origin Airport IATA code",
    origin_airport_name STRING,
    origin_airport_location STRING COMMENT "Airport location. Pattern: city, state",
    scheduled_arrival TIMESTAMP,
    destination_airport STRING COMMENT "Destination Airport IATA code",
    destination_airport_name STRING,
    destination_airport_location STRING COMMENT "Airport location. Pattern: city, state",
    param_start_date STRING COMMENT "Pig job parameter: $startDate",
    param_end_date STRING COMMENT "Pig job parameter: $endDate",
    requesting_user STRING COMMENT "User that issued the data request",
    workflow_job_id STRING COMMENT "Oozie workflow job id",
    job_type STRING COMMENT "Job type (PIG|HIVE|SPARK)",
    ts_insert TIMESTAMP COMMENT "Data insert timestamp",
    dt_insert STRING COMMENT "Data insert date"
)
STORED AS PARQUET;

TRUNCATE TABLE ${db}.${table};