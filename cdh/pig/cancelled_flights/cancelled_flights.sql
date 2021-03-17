CREATE TABLE IF NOT EXISTS ${db}.${table} (

    scheduled_departure TIMESTAMP COMMENT "Flight departure time",
    origin_iata STRING COMMENT "Origin airport IATA code",
    origin_aiport STRING,
    origin_city STRING,
    airline_iata STRING COMMENT "Airline IATA code",
    airline_name STRING,
    destination_iata STRING COMMENT "Destination airport IATA code",
    destination_airport STRING,
    destination_city STRING,
    cancellation_code VARCHAR(1),
    cancellation_rationale STRING,
    param_start_date STRING COMMENT "Pig job parameter: $startDate",
    param_end_date STRING COMMENT "Pig job parameter: $endDate",
    param_iata_code STRING COMMENT "Pig job parameter: $iataCode",
    requesting_user STRING COMMENT "User that issued the data request",
    workflow_job_id STRING COMMENT "Oozie workflow job id",
    job_type STRING COMMENT "Job type (PIG|HIVE|SPARK)",
    ts_insert TIMESTAMP COMMENT "Data insert timestamp",
    dt_insert STRING COMMENT "Data insert date"
)
STORED AS PARQUET;

TRUNCATE TABLE ${db}.${table};