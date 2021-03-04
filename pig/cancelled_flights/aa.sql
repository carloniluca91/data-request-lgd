CREATE TABLE IF NOT EXISTS ${db}.${output_table} (

    c1 STRING COMMENT "Origin airport IATA code",
    c2 STRING,
    c3 STRING,
    c4 STRING COMMENT "Destination airport IATA code"
)
STORED AS PARQUET;

TRUNCATE ${db}.${output_table};