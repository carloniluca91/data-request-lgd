/*
    parameters:
        - $udfJarPath: HDFS path of jar defining PIG UDFs
        - $db : database name
        - $flights: flights table name
        - $startDate: lower date (inclusive) (yyyy-MM-dd)
        - $endDate: upper date (inclusive) (yyyy-MM-dd)
        - $airlineIatas: airline IATA code(s). Comma-separated
        - $delayThreshold: departure/arrival delay threshold
        - $outputTable: output Hive table name
        - $airlines: airlines table name
        - $userName: user that triggered the data request
        - $wfId: parent workflow job id
 */

REGISTER $udfJarPath;

-- FLIGHTS table
flights = LOAD '$db.$flights' USING org.apache.hive.hcatalog.pig.HCatLoader();

flights_filter = FILTER flights BY

   ToDate(CONCAT(year, month, day), 'yyyyMMdd') >= ToDate('$startDate', 'yyyy-MM-dd') AND
   ToDate(CONCAT(year, month, day), 'yyyyMMdd') <= ToDate('$endDate', 'yyyy-MM-dd') AND
   pig.udf.IsIn(airline, '$airlineIatas') AND
   diverted == 0 AND
   cancelled == 0 AND
   (departure_delay >= $delayThreshold OR arrival_delay >= $delayThreshold);

flights_filter_gen = FOREACH flights_filtered GENERATE

    year,
    month,
    day,
    airline,
    flight_number,
    departure_delay,
    arrival_delay,
    air_system_delay,
    security_delay,
    airline_delay,
    late_aircraft_delay,
    weather_delay;

-- group by (airline, year, month) and compute aggregated statistics
flights_group = GROUP flights_filter_gen BY (airline, CONCAT(year, month));

flights_grouped_flattened = FOREACH flights_group GENERATE

    group.airline AS airline,
    group.$1 AS year_month,
    FLATTEN(flights_group.day) AS day,
    FLATTEN(flights_group.flight_number) AS flight_number,

    -- departure stats
    FLATTEN(flights_group.scheduled_departure) AS scheduled_departure,
    FLATTEN(flights_group.departure_time) AS departure_time,
    FLATTEN(flights_group.departure_delay) AS departure_delay,
    SUM(flights_group.departure_delay) AS total_departure_delay,

    -- arrival stats
    FLATTEN(flights_group.scheduled_arrival) AS scheduled_arrival,
    FLATTEN(flights_group.arrival_time) AS arrival_time,
    FLATTEN(flights_group.arrival_delay) AS arrival_delay,
    SUM(flights_group.arrival_delay) AS total_arrival_delay,

    -- air_system stats
    FLATTEN(flights_group.air_system_delay) AS air_system_delay,
    SUM(flights_group.air_system_delay) AS total_air_system_delay,

    -- security stats
    FLATTEN(flights_group.security_delay) AS security_delay,
    SUM(flights_group.security_delay) AS total_security_delay,

    -- airline stats
    FLATTEN(flights_group.airline_delay) AS airline_delay,
    SUM(flights_group.airline_delay) AS total_airline_delay,

    -- aircraft stats
    FLATTEN(flights_group.late_aircraft_delay) AS late_aircraft_delay,
    SUM(flights_group.late_aircraft_delay) AS total_late_aircraft_delay,

    -- weather stats
    FLATTEN(flights_group.weather_delay) AS weather_delay,
    SUM(flights_group.weather_delay) AS total_weather_delay,
    SUM(flights_group) AS number_of_delayed_flights;

flights_flattened_gen = FOREACH flights_grouped_flattened GENERATE

    airline,
    year_month,
    flight_number,

    -- departure stats
    ToDate(CONCAT(year, month, day, scheduled_departure), 'yyyyMMddHHmm') AS scheduled_departure,
    ToDate(CONCAT(year, month, day, departure_time), 'yyyyMMddHHmm') as effective_departure,
    departure_delay,
    (departure_delay / total_departure_delay) AS departure_delay_weight,
    total_departure_delay,

    -- arrival stats
    ToDate(CONCAT(year, month, day, scheduled_arrival), 'yyyyMMddHHmm') AS scheduled_arrival,
    ToDate(CONCAT(year, month, day, arrival_time), 'yyyyMMddHHmm') as effective_arrival,
    arrival_delay,
    (arrival_delay / total_arrival_delay) AS arrival_delay_weight,
    total_arrival_delay,

    -- air_system stats
    air_system_delay,
    (air_system_delay / total_air_system_delay) AS air_system_delay_weight,
    total_air_system_delay,

    -- security stats
    security_delay,
    (security_delay / total_security_delay) AS security_delay_weight,
    total_security_delay,

    -- airline stats
    airline_delay,
    (airline_delay / total_airline_delay) AS airline_delay_weight,
    total_airline_delay,

    -- aircraft stats
    aircraft_delay,
    (aircraft_delay / total_aircraft_delay) AS aircraft_delay_weight,
    total_aircraft_delay,
    
    -- weather stats
    weather_delay,
    (weather_delay / total_weather_delay) AS weather_delay_weight,
    total_weather_delay,
    
    number_of_delayed_flights;

-- join with airline by iata_code
airlines = LOAD '$db.$airlines' USING org.apache.hive.hcatalog.pig.HCatLoader();
airlines_gen = FOREACH airlines GENERATE

    iata_code,
    airline;
    
flights_flattened_join = JOIN flights_flattened_gen BY airline, airlines_gen BY iata_code USING 'replicated';

flights_flattened_join_gen = FOREACH flights_flattened_join GENERATE 

    flights_flattened_gen::airline AS airline_iata_code,
    airlines_gen::airline AS airline_name,
    flights_flattened_gen::year_month AS year_month,
    flights_flattened_gen::flight_number AS flight_number,
    flights_flattened_gen::scheduled_departure AS scheduled_departure,
    flights_flattened_gen::effective_departure AS effective_departure,
    flights_flattened_gen::departure_delay AS departure_delay,
    flights_flattened_gen::departure_delay_weight AS departure_delay_weight,
    flights_flattened_gen::total_departure_delay AS total_departure_delay,
    flights_flattened_gen::scheduled_arrival AS scheduled_arrival,
    flights_flattened_gen::effective_arrival AS effective_arrival,
    flights_flattened_gen::arrival_delay AS arrival_delay,
    flights_flattened_gen::arrival_delay_weight AS arrival_delay_weight,
    flights_flattened_gen::total_arrival_delay AS total_arrival_delay,
    flights_flattened_gen::air_system_delay AS air_system_delay,
    flights_flattened_gen::air_system_delay_weight AS air_system_delay_weight,
    flights_flattened_gen::total_air_system_delay AS total_air_system_delay,
    flights_flattened_gen::security_delay AS security_delay,
    flights_flattened_gen::security_delay_weight AS security_delay_weight,
    flights_flattened_gen::total_security_delay AS total_security_delay,
    flights_flattened_gen::airline_delay AS airline_delay,
    flights_flattened_gen::airline_delay_weight AS airline_delay_weight,
    flights_flattened_gen::total_airline_delay AS total_airline_delay,
    flights_flattened_gen::aircraft_delay AS aircraft_delay,
    flights_flattened_gen::aircraft_delay_weight AS aircraft_delay_weight,
    flights_flattened_gen::total_aircraft_delay AS total_aircraft_delay,
    flights_flattened_gen::weather_delay AS weather_delay,
    flights_flattened_gen::weather_delay_weight AS weather_delay_weight,
    flights_flattened_gen::total_weather_delay AS total_weather_delay,
    flights_flattened_gen::number_of_delayed_flights AS number_of_delayed_flights,
    '$startDate' AS param_start_date,
    '$endDate' AS param_end_date,
    '$airlineIatas' AS param_airline_iatas,
    '$userName' AS requesting_user,
    '$wfId' AS workflow_job_id,
    'PIG' AS job_type,
    CurrentTime() AS ts_insert,
    ToString(CurrentTime(), 'yyyy-MM-dd') AS dt_insert;

flights_flattened_ordered = ORDER flights_flattened_join_gen BY airline_iata_code, year_month, scheduled_departure;

STORE flights_flattened_ordered INTO '$db.$outputTable' USING org.apache.hive.hcatalog.pig.HCatStorer();