/*
    parameters:
        - $udfJarPath: HDFS path of jar defining PIG UDFs
        - $db : database name
        - $flights: flights table name
        - $startDate: lower date (inclusive) (yyyy-MM-dd)
        - $endDate: upper date (inclusive) (yyyy-MM-dd)
        - $airlines: airlines table name
        - $airports: airport table name
        - $outputTable: output Hive table name
        - $userName: user that triggered the data request
        - $wfId: parent workflow job id
 */

REGISTER $udfJarPath;

-- FLIGHTS table
flights = LOAD '$db.$flights' USING org.apache.hive.hcatalog.pig.HCatLoader();

flights_filter = FILTER flights BY

   pig.udf.ToDate(CONCAT(year, month, day, scheduled_departure), 'yyyyMMddHHmm') >= ToDate(CONCAT('$startDate', ' 00:00'), 'yyyy-MM-dd HH:mm') AND
   pig.udf.ToDate(CONCAT(year, month, day, scheduled_departure), 'yyyyMMddHHmm') <= ToDate(CONCAT('$endDate', ' 23:59'), 'yyyy-MM-dd HH:mm');

flights_filter_gen = FOREACH flights_filter GENERATE

    flight_number,
    airline AS airline_iata,
    origin_airport,
    pig.udf.ToDate(CONCAT(year, month, day, scheduled_departure), 'yyyyMMddHHmm') AS scheduled_departure,
    destination_airport,
    pig.udf.ToDate(CONCAT(year, month, day, scheduled_arrival), 'yyyyMMddHHmm') AS scheduled_arrival;

-- AIRLINES table
airlines = LOAD '$db.$airlines' USING org.apache.hive.hcatalog.pig.HCatLoader();

airlines_gen = FOREACH airlines GENERATE

    iata_code AS airline_iata,
    airline AS airline_name;

-- Join flights with airlines
flights_filter_gen_join_airlines_gen = JOIN flights_filter_gen BY airline_iata, airlines_gen BY airline_iata USING 'replicated';

flights_join_airlines = FOREACH flights_filter_gen_join_airlines_gen GENERATE

    flights_filter_gen::flight_number AS flight_number,
    flights_filter_gen::airline_iata AS airline_iata,
    airlines_gen::airline_name AS airline_name,
    flights_filter_gen::scheduled_departure AS scheduled_departure,
    flights_filter_gen::origin_airport AS origin_airport,
    flights_filter_gen::scheduled_arrival AS scheduled_arrival,
    flights_filter_gen::destination_airport AS destination_airport;

-- AIRPORTS table
airports = LOAD '$db.$airports' USING org.apache.hive.hcatalog.pig.HCatLoader();

airports_gen = FOREACH airports GENERATE

    iata_code AS airport_iata_code,
    airport AS airport_name,
    CONCAT(city, ', ', state) AS airport_location;

-- Join flights with airports for origin details
flights_join_origin = JOIN flights_join_airlines BY origin_airport, airports_gen BY airport_iata_code USING 'replicated';

flights_join_origin_gen = FOREACH flights_join_origin GENERATE

    flights_join_airlines::flight_number AS flight_number,
    flights_join_airlines::airline_iata AS airline_iata,
    flights_join_airlines::airline_name AS airline_name,
    flights_join_airlines::scheduled_departure AS scheduled_departure,
    flights_join_airlines::origin_airport AS origin_airport,
    airports_gen::airport_name AS origin_airport_name,
    airports_gen::airport_location AS origin_airport_location,
    flights_join_airlines::scheduled_arrival AS scheduled_arrival,
    flights_join_airlines::destination_airport AS destination_airport;

-- Join flights with airports for destination details
flights_join_destination = JOIN flights_join_origin_gen BY origin_airport, airports_gen BY airport_iata_code USING 'replicated';

flight_details_unordered = FOREACH flights_join_destination GENERATE

    flights_join_origin_gen::flight_number AS flight_number,
    flights_join_origin_gen::airline_iata AS airline_iata,
    flights_join_origin_gen::airline_name AS airline_name,
    flights_join_origin_gen::scheduled_departure AS scheduled_departure,
    flights_join_origin_gen::origin_airport AS origin_airport,
    flights_join_origin_gen::airport_name AS origin_airport_name,
    flights_join_origin_gen::airport_location AS origin_airport_location,
    flights_join_origin_gen::scheduled_arrival AS scheduled_arrival,
    flights_join_origin_gen::destination_airport AS destination_airport,
    airports_gen::airport_name AS destination_airport_name,
    airports_gen::airport_location AS destination_airport_location,
    '$startDate' AS param_start_date,
    '$endDate' AS param_end_date,
    '$userName' AS requesting_user,
    '$wfId' AS workflow_job_id,
    'PIG' AS job_type,
    CurrentTime() AS ts_insert,
    ToString(CurrentTime(), 'yyyy-MM-dd') AS dt_insert;

flight_details = ORDER flight_details_unordered BY scheduled_departure;

STORE flight_details INTO '$db.$outputTable' USING org.apache.hive.hcatalog.pig.HCatStorer();

