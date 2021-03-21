/*
    parameters:
        - $udfJarPath: HDFS path of jar defining PIG UDFs
        - $db : database name
        - $flights: flights table name
        - $startDate: lower date (inclusive) (yyyy-MM-dd)
        - $endDate: upper date (inclusive) (yyyy-MM-dd)
        - $airlineIatas: airline IATA code(s). Comma-separated
        - $outputTable: output Hive table name
        - $airlines: airlines table name
        - $userName: user that triggered the data request
        - $wfId: parent workflow job id
 */

REGISTER $udfJarPath;

-- FLIGHTS table
flights = LOAD '$db.$flights' USING org.apache.hive.hcatalog.pig.HCatLoader();

flights_filter = FILTER flights BY

   pig.udf.ToDate(CONCAT(year, month, day, scheduled_departure), 'yyyyMMddHHmm') >= ToDate(CONCAT('$startDate', ' 00:00'), 'yyyy-MM-dd HH:mm') AND
   pig.udf.ToDate(CONCAT(year, month, day, scheduled_departure), 'yyyyMMddHHmm') <= ToDate(CONCAT('$endDate', ' 23:59'), 'yyyy-MM-dd HH:mm') AND
   pig.udf.IsIn(airline, '$airlineIatas') AND
   diverted == 0 AND
   cancelled == 0 AND
   (departure_delay >= 15 OR arrival_delay >= 15);

flights_filter_gen = FOREACH flights_filter GENERATE

    airline,
    CONCAT(year, month) AS year_month,
    flight_number,
    pig.udf.ToDate(CONCAT(year, month, day, scheduled_departure), 'yyyyMMddHHmm') AS scheduled_departure,
    pig.udf.ToDate(CONCAT(year, month, day, departure_time), 'yyyyMMddHHmm') AS effective_departure,
    departure_delay,
    pig.udf.ToDate(CONCAT(year, month, day, scheduled_arrival), 'yyyyMMddHHmm') AS scheduled_arrival,
    pig.udf.ToDate(CONCAT(year, month, day, arrival_time), 'yyyyMMddHHmm') AS effective_arrival,
    arrival_delay,
    air_system_delay,
    security_delay,
    airline_delay,
    late_aircraft_delay,
    weather_delay;

-- group by (airline, year, month) and compute aggregated statistics
flights_group = GROUP flights_filter_gen BY (airline, year_month);

flights_group_gen = FOREACH flights_group GENERATE

    group.airline AS airline,
    group.year_month AS year_month,
    SUM(flights_filter_gen.departure_delay) AS total_departure_delay,
    SUM(flights_filter_gen.arrival_delay) AS total_arrival_delay,
    SUM(flights_filter_gen.air_system_delay) AS total_air_system_delay,
    SUM(flights_filter_gen.security_delay) AS total_security_delay,
    SUM(flights_filter_gen.airline_delay) AS total_airline_delay,
    SUM(flights_filter_gen.late_aircraft_delay) AS total_late_aircraft_delay,
    SUM(flights_filter_gen.weather_delay) AS total_weather_delay,
    COUNT(flights_filter_gen) AS number_of_delayed_flights;

-- join flight_filter_gen with flights_group_gen 
flights_filter_gen_join_group_gen = JOIN flights_filter_gen BY (airline, year_month), flights_group_gen BY (airline, year_month) USING 'replicated';

flights_output = FOREACH flights_filter_gen_join_group_gen GENERATE

    flights_filter_gen::airline AS airline,
    flights_filter_gen::year_month AS year_month,
    flights_filter_gen::flight_number AS flight_number,

    -- departure stats
    flights_filter_gen::scheduled_departure AS scheduled_departure,
    flights_filter_gen::effective_departure AS effective_departure,
    flights_filter_gen::departure_delay AS departure_delay,
    (double) (flights_filter_gen::departure_delay / flights_group_gen::total_departure_delay) AS departure_delay_weight,
    flights_group_gen::total_departure_delay AS total_departure_delay,

    -- arrival stats
    flights_filter_gen::scheduled_arrival AS scheduled_arrival,
    flights_filter_gen::effective_arrival AS effective_arrival,
    flights_filter_gen::arrival_delay AS arrival_delay,
    (double) (flights_filter_gen::arrival_delay / flights_group_gen::total_arrival_delay) AS arrival_delay_weight,
    flights_group_gen::total_arrival_delay AS total_arrival_delay,

    -- air_system stats
    flights_filter_gen::air_system_delay AS air_system_delay,
    (double) (flights_filter_gen::air_system_delay / flights_group_gen::total_air_system_delay) AS air_system_delay_weight,
    flights_group_gen::total_air_system_delay AS total_air_system_delay,

    -- security stats
    flights_filter_gen::security_delay AS security_delay,
    (double) (flights_filter_gen::security_delay / flights_group_gen::total_security_delay) AS security_delay_weight,
    flights_group_gen::total_security_delay AS total_security_delay,

    -- airline stats
    flights_filter_gen::airline_delay AS airline_delay,
    (double) (flights_filter_gen::airline_delay / flights_group_gen::total_airline_delay) AS airline_delay_weight,
    flights_group_gen::total_airline_delay AS total_airline_delay,

    -- aircraft stats
    flights_filter_gen::late_aircraft_delay AS late_aircraft_delay,
    (double) (flights_filter_gen::late_aircraft_delay / flights_group_gen::total_late_aircraft_delay) AS late_aircraft_delay_weight,
    flights_group_gen::total_late_aircraft_delay AS total_late_aircraft_delay,

    -- weather stats
    flights_filter_gen::weather_delay AS weather_delay,
    (double) (flights_filter_gen::weather_delay / flights_group_gen::total_weather_delay) AS weather_delay_weight,
    flights_group_gen::total_weather_delay AS total_weather_delay,

    flights_group_gen::number_of_delayed_flights AS number_of_delayed_flights;

-- AIRLINES table
airlines = LOAD '$db.$airlines' USING org.apache.hive.hcatalog.pig.HCatLoader();

airlines_filter = FILTER airlines BY pig.udf.IsIn(iata_code, '$airlineIatas');

airlines_filter_gen = FOREACH airlines_filter GENERATE

    iata_code,
    airline;

-- join with airline by iata_code
flights_output_unordered = JOIN flights_output BY airline, airlines_filter_gen BY iata_code USING 'replicated';

flights_output_unordered_gen = FOREACH flights_output_unordered GENERATE

    flights_output::airline AS airline_iata_code,
    airlines_filter_gen::airline AS airline_name,
    flights_output::year_month AS year_month,
    flights_output::flight_number AS flight_number,
    flights_output::scheduled_departure AS scheduled_departure,
    flights_output::effective_departure AS effective_departure,
    flights_output::departure_delay AS departure_delay,
    flights_output::departure_delay_weight AS departure_delay_weight,
    flights_output::total_departure_delay AS total_departure_delay,
    flights_output::scheduled_arrival AS scheduled_arrival,
    flights_output::effective_arrival AS effective_arrival,
    flights_output::arrival_delay AS arrival_delay,
    flights_output::arrival_delay_weight AS arrival_delay_weight,
    flights_output::total_arrival_delay AS total_arrival_delay,
    flights_output::air_system_delay AS air_system_delay,
    flights_output::air_system_delay_weight AS air_system_delay_weight,
    flights_output::total_air_system_delay AS total_air_system_delay,
    flights_output::security_delay AS security_delay,
    flights_output::security_delay_weight AS security_delay_weight,
    flights_output::total_security_delay AS total_security_delay,
    flights_output::airline_delay AS airline_delay,
    flights_output::airline_delay_weight AS airline_delay_weight,
    flights_output::total_airline_delay AS total_airline_delay,
    flights_output::late_aircraft_delay AS late_aircraft_delay,
    flights_output::late_aircraft_delay_weight AS late_aircraft_delay_weight,
    flights_output::total_late_aircraft_delay AS total_late_aircraft_delay,
    flights_output::weather_delay AS weather_delay,
    flights_output::weather_delay_weight AS weather_delay_weight,
    flights_output::total_weather_delay AS total_weather_delay,
    'MINUTES' AS delay_unit_measure,
    flights_output::number_of_delayed_flights AS number_of_delayed_flights,
    '$startDate' AS param_start_date,
    '$endDate' AS param_end_date,
    '$airlineIatas' AS param_airline_iatas,
    '$userName' AS requesting_user,
    '$wfId' AS workflow_job_id,
    'PIG' AS job_type,
    CurrentTime() AS ts_insert,
    ToString(CurrentTime(), 'yyyy-MM-dd') AS dt_insert;

monthly_grouped_delays = ORDER flights_output_unordered_gen BY airline_iata_code, year_month, scheduled_departure;

STORE monthly_grouped_delays INTO '$db.$outputTable' USING org.apache.hive.hcatalog.pig.HCatStorer();